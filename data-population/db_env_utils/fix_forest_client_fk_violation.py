"""
Identify and delete postgres seedlot table records with fk violations.

Identifies records in postgres seedlot table that have client references that
do not exist in test.

Once we have the client_number/location codes (from this script) that cause
the issues, we can change them to ones that exist in TEST.

Example:
update seedlot
set
    applicant_client_number = '00196805',
    applicant_locn_code = '02';
where
    applicant_client_number = '00149081' and
    applicant_locn_code = '22'

Alternatively, we can delete the records that have the fk violations but that
can result in removing a lot of tables due to fk constraints.

# -- setup to run script:

A) populate the env vars:
    POSTGRES_HOST_TEST
    POSTGRES_DB_TEST
    POSTGRES_PORT_TEST=5433
    POSTGRES_USER_TEST
    POSTGRES_PASSWORD_TEST

    ORACLE_HOST_TEST
    ORACLE_PORT_TEST
    ORACLE_SERVICE_TEST
    ORACLE_USER_TEST
    ORACLE_PASSWORD_TEST

B) create the tunnel to the oc database
    oc port-forward <database pod> 5433:5432

C) run the script
"""

import logging
import os

import constants
import db_lib
import env_config
import main_common
import oradb_lib
import postgresdb_lib

LOGGER = logging.getLogger(__name__)


class PostgresSeedlot:
    """Interfact to postgres data."""

    def __init__(self, util) -> None:
        """
        Database constructor.
        """
        self.db = None
        self.util = util
        self.db_connect()

    def db_connect(self) -> None:
        """
        Connect to the database.
        """
        # dcr = docker_parser.ReadDockerCompose()
        # LOGGER.debug("connecting to database... ")
        # local_db_params = dcr.get_spar_conn_params()
        oc_params = db_lib.ConnectionParameters(
            username=os.getenv("POSTGRES_USER_TEST"),
            password=os.getenv("POSTGRES_PASSWORD_TEST"),
            host=os.getenv("POSTGRES_HOST_TEST"),
            port=os.getenv("POSTGRES_PORT_TEST"),
            service_name=os.getenv("POSTGRES_DB_TEST"),
            schema_to_sync=os.getenv("POSTGRES_USER_TEST"),
        )
        # schema_to_sync=os.getenv("POSTGRES_USER_TEST"),
        self.db = postgresdb_lib.PostgresDatabase(
            oc_params,
            self.util.app_paths,
        )

    def get_seedlot_fc_records(self) -> set:
        """
        Get unique list of client_number and location code from seedlot table.

        :return: a list of tuples containing client_number and location code.
        :rtype: set
        """
        query = """
        select
            distinct
                applicant_client_number,
                applicant_locn_code
        from
            spar.seedlot
        """
        LOGGER.debug("query: %s", query)
        self.db.get_connection()
        cursor = self.db.connection.cursor()
        cursor.execute(query)
        cur_list = cursor.fetchall()
        return set(cur_list)


class OracleSeedlot:
    """
    Interface to oracle data.
    """

    def __init__(self, util) -> None:
        """
        Oracle constructor.
        """
        self.db = None
        self.util = util
        self.env_obj = util.env_obj
        self.db_connect()

    def db_connect(self) -> None:
        """
        Create a connection to the oracle database.
        """
        # dcr = docker_parser.ReadDockerCompose()

        # local_db_params = dcr.get_ora_conn_params()
        local_db_params = db_lib.ConnectionParameters(
            username=os.getenv("ORACLE_USER_TEST"),
            password=os.getenv("ORACLE_PASSWORD_TEST"),
            host=os.getenv("ORACLE_HOST_TEST"),
            port=os.getenv("ORACLE_PORT_TEST"),
            service_name=os.getenv("ORACLE_SERVICE_TEST"),
            schema_to_sync=os.getenv("ORACLE_SCHEMA_TO_SYNC_TEST"),
        )

        self.db = oradb_lib.OracleDatabase(local_db_params, self.util.app_paths)
        self.db.schema_to_sync = self.env_obj.get_schema_to_sync()

    def get_seedlot_fc_records(self) -> None:
        """
        Get unique list of client_number and location code from seedlot table.
        """
        query = """
        select
            distinct
                CLIENT_NUMBER,
                CLIENT_LOCN_CODE
        from
            the.CLIENT_LOCATION
        """
        self.db.get_connection()
        cursor = self.db.connection.cursor()
        cursor.execute(query)
        cur_list = cursor.fetchall()
        return set(cur_list)


if __name__ == "__main__":
    # only using to setup logging
    util = main_common.Utility("TEST", constants.DBType.OC_POSTGRES)
    util.configure_logging()
    LOGGER.setLevel(logging.DEBUG)

    pgs = PostgresSeedlot(util)
    pg_distinct = pgs.get_seedlot_fc_records()

    oras = OracleSeedlot(util)
    ora_distinct = oras.get_seedlot_fc_records()
    records_to_delete = pg_distinct - ora_distinct

    match_cnt = 0
    for record in pg_distinct:
        if record in ora_distinct:
            match_cnt += 1
        else:
            LOGGER.debug("record to delete: %s", record)
    LOGGER.debug("match count: %s", match_cnt)
    LOGGER.debug("pg distinct count: %s", len(pg_distinct))
    LOGGER.debug("records to delete: %s", records_to_delete)
