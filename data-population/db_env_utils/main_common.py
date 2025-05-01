"""
Utility code to configure ingest and extract scripts.
"""

from __future__ import annotations

import base64
import logging
import logging.config
import pathlib
import socket
import time

import app_paths
import constants
import docker_parser
import env_config
import kubernetes_wrapper
import object_store
import oradb_lib
import postgresdb_lib

LOGGER = logging.getLogger(__name__)


class Utility:
    """
    Utility class to run the extract and injest processes.
    """

    def __init__(self, env_str: str, db: constants.DBType) -> None:
        """
        Initialize the Utility class.
        """
        self.env_str = env_str
        self.env_obj = env_config.Env(env_str)
        self.app_paths = app_paths.AppPaths(self.env_obj)

        self.db_type = db
        self.kube_client = None

        self.connection_retries = 10

    def make_dirs(self) -> None:
        """
        Make necessary directories.

        Directories that need to be created are dependent on the database
        environment (TEST or PROD)

        """
        data_dir = self.app_paths.get_data_dir()
        LOGGER.debug("datadir: %s", data_dir)
        if not data_dir.exists():
            data_dir.mkdir(parents=True)

    def configure_logging(self) -> None:
        """
        Configure logging.
        """
        log_config_path = self.app_paths.get_log_config_dir()
        log_config_path = pathlib.Path(log_config_path, "logging.config")
        logging.config.fileConfig(
            log_config_path,
            disable_existing_loggers=False,
        )
        global LOGGER  # noqa: PLW0603
        LOGGER = logging.getLogger(__name__)
        LOGGER.debug("test debug message")

    def connect_ostore(self) -> object_store.OStore:
        """
        Connect to object store.

        :return: an object store wrapper object.
        :rtype: object_store.OStore
        """
        ostore_params = self.env_obj.get_ostore_constants()
        return object_store.OStore(
            conn_params=ostore_params,
            app_paths=self.app_paths,
        )

    def pull_data_classifications(self) -> None:
        """
        Pull the data classification spreadsheet from object store.

        This metadata is currently read from a ss, and is used to identify
        the security classification of the data that is being extracted from
        the database.  This is used to determine if the data needs to be
        masked or not.

        This method ensures the ss is available locally, so that subsequent
        processes that need it do not have to pull it from object store.

        :return: None
        """
        ostore = self.connect_ostore()
        LOGGER.info("pulling data classification spreadsheet from object store")
        ostore.get_data_classification_ss()

    def get_tables(self) -> list[str]:
        """
        Get table list from database.

        Redirects to the appropriate method based on the database type.

        :return: list of tables found in the database schema
        :rtype: list[str]
        """
        tables = []
        if self.db_type == constants.DBType.ORA:
            tables = self.get_tables_from_local_ora_docker()
        elif self.db_type == constants.DBType.OC_POSTGRES:
            tables = self.get_tables_from_local_postgres_docker()
        return tables

    def get_tables_from_oc_postgres(self) -> list[str]:
        """
        Get tables from local containerized postgres database.

        :return: list of tables found in the postgres database schema
        :rtype: list[str]
        """
        oc_params = self.env_obj.get_oc_constants()

        db_pod = self.get_kubnernetes_db_pod()
        db_params = self.get_dbparams_from_kubernetes()
        self.open_port_forward_sync(
            db_pod.metadata.name,
            oc_params.namespace,
            constants.DB_LOCAL_PORT,
            db_params.port,
        )

        # now get the actual tables
        oc_pg_db_params = self.get_dbparams_from_kubernetes()
        # swap the connection port to the local port configured for the
        # tunnel
        oc_pg_db_params.port = constants.DB_LOCAL_PORT
        db_connection = postgresdb_lib.PostgresDatabase(
            connection_params=oc_pg_db_params,
            app_paths=self.app_paths,
        )
        tables_to_export = db_connection.get_tables(
            schema=oc_pg_db_params.schema_to_sync,
            omit_tables=["FLYWAY_SCHEMA_HISTORY"],
        )
        LOGGER.debug("number of table to export: %s", len(tables_to_export))
        return tables_to_export

    def get_tables_from_local_postgres_docker(self) -> list[str]:
        """
        Get a list of table from local postgres docker database.

        :return: list of tables found in the local postgres database schema
        :rtype: list[str]
        """
        dcr = docker_parser.ReadDockerCompose()
        pg_local_params = dcr.get_local_postgres_conn_params()
        LOGGER.debug("schema to sync: %s", pg_local_params.schema_to_sync)
        local_docker_db = postgresdb_lib.PostgresDatabase(pg_local_params)
        tables_to_export = local_docker_db.get_tables(
            local_docker_db.schema_2_sync,
            omit_tables=["FLYWAY_SCHEMA_HISTORY"],
        )
        LOGGER.debug("tables retrieved: %s", tables_to_export)
        return tables_to_export

    def get_tables_from_local_ora_docker(self) -> list[str]:
        """
        Get list of tables from local oracle docker database.
        """
        # start by trying to get parameters from the environment
        local_ora_params = self.env_obj.get_local_ora_db_env_constants()

        LOGGER.debug("local ora params: %s", local_ora_params)

        local_docker_db = oradb_lib.OracleDatabase(
            connection_params=local_ora_params,
            app_paths=self.app_paths,
        )
        tables_to_export = local_docker_db.get_tables(
            local_docker_db.schema_2_sync,
            omit_tables=["FLYWAY_SCHEMA_HISTORY"],
        )
        LOGGER.debug("tables retrieved: %s", tables_to_export)
        return tables_to_export

    def get_kubernetes_client(self) -> None:
        """
        Populate kubernetes client.

        This method is called any time a method is called that requires the
        kube_client property
        """
        if self.kube_client is None:
            oc_params = self.env_obj.get_oc_constants()
            self.kube_client = kubernetes_wrapper.KubeClient(oc_params)

    def open_port_forward_sync(
        self,
        pod_name: str,
        namespace: str,
        local_port: str,
        remote_port: str,
    ) -> None:
        """
        Create port forward.

        opens a port-forward then waits until it has successfully been created,
        once the port-forward has completed and can be succesfully connected to
        the method will complete.

        :param pod_name: the name of the pod to establish the port-forward to
        :type pod_name: str
        :param namespace: the namespace that the pod is in
        :type namespace: str
        :param local_port: the local port for the port-forward
        :type local_port: str
        :param remote_port: the remote port for the port-forward
        :type remote_port: str
        """
        self.get_kubernetes_client()

        self.kube_client.open_port_forward(
            pod_name=pod_name,
            namespace=namespace,
            local_port=local_port,
            remote_port=remote_port,
        )
        sock_success = False
        retry = 0
        # test the connection
        while not sock_success or retry > self.connection_retries:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                sock.connect(("localhost", local_port))
                sock_success = True
            except OSError:
                LOGGER.exception("port forward not available...")
                time.sleep(1)
                retry += 1
            finally:
                # Close the socket
                sock.close()

    def get_tables_for_extract(self) -> list[str]:
        """
        Get the tables to extract from the database.

        Queries metadata from the local docker compose database to get the list
        of tables that need to be extracted.

        :return: a list of table names to be extracted, for the specified
            database type.
        :rtype: list[str]
        """
        if self.db_type == constants.DBType.ORA:
            tables = self.get_tables()
        elif self.db_type == constants.DBType.OC_POSTGRES:
            tables = self.get_tables_from_oc_postgres()
        return tables

    def get_kubnernetes_db_pod(self) -> str:
        """
        Return the database pod from kubernetes.

        :raises IndexError: raised if cannot find a database pod
        :return: string representing the database pod name
        :rtype: str
        """
        self.get_kubernetes_client()

        db_filter_string = constants.DB_FILTER_STRING.format(
            env_str=self.env_str.lower(),
        )
        pods = self.kube_client.get_pods(
            filter_str=db_filter_string,
            exclude_strs=["backup"],
        )
        if len(pods) > 1:
            pod_names = ", ".join([pod.metadata.name for pod in pods])
            msg = (
                f"searching for pods that match the pattern {db_filter_string}"
                " returned more than one pod, narrow the search pattern so only"
                f" one pod is returned.  pods currently matched: {pod_names}"
            )
            LOGGER.exception(msg)
            raise IndexError(msg)
        if len(pods) == 0:
            msg = (
                f"searching for pods that match the pattern {db_filter_string}"
                "didn't return any pods"
            )
        return pods[0]

    def get_dbparams_from_kubernetes(self) -> env_config.ConnectionParameters:
        """
        Retrieve database parameters from kubernetes.

        :raises IndexError: unable to find database pod
        :return: database connection parameters used to connect to spar database
        :rtype: env_config.ConnectionParameters
        """
        oc_params = self.env_obj.get_oc_constants()

        db_filter_string = constants.DB_FILTER_STRING.format(
            env_str=self.env_str.lower(),
        )
        self.get_kubernetes_client()
        secrets = self.kube_client.get_secrets(
            namespace=oc_params.namespace,
            filter_str=db_filter_string,
        )
        if len(secrets) > 1:
            secret_names = ", ".join(
                [secret.metadata.name for secret in secrets],
            )
            msg = (
                "searching for secrets that match the pattern "
                f"{db_filter_string} returned more than one pod, narrow the "
                "search pattern so only one pod is returned.  Secrets found "
                f"include: {secret_names}"
            )
            LOGGER.exception(msg)
            raise IndexError(msg)
        db_secret = secrets[0]
        db_conn_params = env_config.ConnectionParameters
        db_conn_params.host = "localhost"
        db_conn_params.schema_to_sync = "spar"
        db_conn_params.port = base64.b64decode(
            db_secret.data["database-port"],
        ).decode("utf-8")
        db_conn_params.service_name = base64.b64decode(
            db_secret.data["database-name"],
        ).decode("utf-8")
        db_conn_params.username = base64.b64decode(
            db_secret.data["database-user"],
        ).decode("utf-8")
        db_conn_params.password = base64.b64decode(
            db_secret.data["database-password"],
        ).decode("utf-8")
        return db_conn_params

    def run_extract(self, *, refresh: bool, single_table: str | None) -> None:
        """
        Run the extract process.
        """
        # data classification spreadsheet is used to determine what data can be
        # pulled at a column level.
        self.pull_data_classifications()
        self.make_dirs()

        # gets the table list from database
        if single_table is not None:
            tables_to_export = [single_table]
        else:
            tables_to_export = self.get_tables_for_extract()
        LOGGER.debug("tables to export: %s", tables_to_export)

        ostore = self.connect_ostore()

        # connect to database
        if self.db_type == constants.DBType.ORA:
            # if oracle then do these things...
            ora_params = self.env_obj.get_ora_db_env_constants()
            db_connection = oradb_lib.OracleDatabase(
                ora_params,
                self.app_paths,
            )  # use the environment variables for connection parameters
            db_connection.get_connection()
        elif self.db_type == constants.DBType.OC_POSTGRES:
            spar_db_params = self.get_dbparams_from_kubernetes()
            # using port forward so override the port to the local port that
            # is forwarded to the remote port
            spar_db_params.port = constants.DB_LOCAL_PORT
            db_connection = postgresdb_lib.PostgresDatabase(
                connection_params=spar_db_params,
                app_paths=self.app_paths,
            )
        # example of some of the tables that triggered the ostore upload issue
        # SEEDLOT / CLIENT_LOCATION / PARENT_TREE / SMP_MIX
        for table in tables_to_export:
            LOGGER.info("Exporting table %s", table)
            # skip the forest cover geometry table, for now
            # commonly skipped tables:
            # FOREST_COVER_GEOMETRY
            # STOCKING_STANDARD_GEOMETRY
            # TIMBER_MARK
            # HARVESTING_AUTHORITY
            tables_2_skip = []
            # leeaving
            if table.upper() in tables_2_skip:
                # FOREST_COVER_GEOMETRY - requires a tweak to address sdo
                #                         geometry
                # TIMBER_MARK - ValueError: year -1 is out of range
                # HARVESTING_AUTHORITY - ValueError: year -1 is out of range
                continue
            # the export file type is different depending on the database.
            # originally wanted to keep to parquet, but loading the json data
            # used by spar doesn't work well with postgres.  So using pg_dump.
            local_export_file = self.app_paths.get_default_export_file_path(
                table,
                self.env_obj.current_env,
                self.db_type,
            )

            # if refresh is set to true the delete the local file if it exists
            if refresh and local_export_file.exists():
                local_export_file.unlink()

            LOGGER.debug("export_file: %s", local_export_file)

            ostore_export_file = (
                self.app_paths.get_default_export_file_ostore_path(
                    table,
                    self.db_type,
                )
            )
            LOGGER.debug("ostore export file: %s", ostore_export_file)
            # if the remote export file exists and the refresh flag is not set
            # then skip the export process
            if (
                ostore.object_exists(object_name=str(ostore_export_file))
                and not refresh
            ):
                LOGGER.info(
                    "Export file %s exists in object store, skipping export",
                    ostore_export_file,
                )
                continue
            # the remote file either does not exist, or the refresh flag is set
            # to true, re-export the file and replace the local and remote data
            LOGGER.info(
                "Export file %s does not exist in object store, exporting",
                ostore_export_file,
            )

            file_created = db_connection.extract_data(
                table,
                local_export_file,
                overwrite=refresh,
            )

            if file_created:
                # push the file to object store, if a new file has been
                # created
                ostore.put_data_files(
                    [table],
                    self.env_obj.current_env,
                    self.db_type,
                )
                LOGGER.debug("pausing for 5 seconds")
                time.sleep(5)
        if self.db_type == constants.DBType.OC_POSTGRES:
            self.kube_client.close_port_forward()

    def run_injest(
        self, *, purge: bool, refreshdb: bool, table2import: str | None
    ) -> None:
        """
        Load data cached in object store to the database.

        :param purge: if true will delete all local cached data files and will
            re-pull that data from object storage.
        :type purge: bool
        :param refreshdb: If true will truncate all the tables that are to be
            loaded, otherwise will only load empty tables.
        :type refreshdb: bool
        """
        datadir = self.app_paths.get_data_dir()
        if purge and datadir.exists():
            logmsg = (
                "Purging cached local data and pulling fresh set from "
                "object store."
            )
            LOGGER.info(logmsg)
            # delete the contents of the directory.
            for file_path in datadir.iterdir():
                if file_path.is_file():
                    file_path.unlink()  # Delete the file
        self.make_dirs()
        if table2import:
            tables_to_import = [table2import]
        else:
            tables_to_import = self.get_tables()
        LOGGER.debug("tables to import: %s", tables_to_import)

        ostore = self.connect_ostore()
        dcr = docker_parser.ReadDockerCompose()

        # getting connection params from the env and then creating db connection
        # to local docker container oracle db where the data is being loaded.
        if self.db_type == constants.DBType.ORA:
            local_db_params = self.env_obj.get_local_ora_db_env_constants()

            local_db_params.schema_to_sync = self.env_obj.get_schema_to_sync()
            local_docker_db = oradb_lib.OracleDatabase(
                local_db_params,
                app_paths=self.app_paths,
            )

        elif self.db_type == constants.DBType.OC_POSTGRES:
            local_db_params = dcr.get_spar_conn_params()
            local_docker_db = postgresdb_lib.PostgresDatabase(local_db_params)

        # only gets files if they don't exist locally.  If purge is set then the
        # local cache will have been emptied before code gets here.
        ostore.get_data_files(
            tables_to_import,
            self.env_obj.current_env,
            self.db_type,
        )

        # delete the database data. Also handled downstream... could be removed
        # and only handled here.
        if refreshdb:
            local_docker_db.purge_data(
                table_list=tables_to_import,
                cascade=True,
            )
        datadir = self.app_paths.get_data_dir()
        local_docker_db.load_data_retry(
            data_dir=datadir,
            table_list=tables_to_import,
            env_str=self.env_obj.current_env,
            refreshdb=refreshdb,
        )
