"""
Wrapper to oracle functions.

Assumptions:

When parameters are not sent to the constructor the class will look in the
following environment variables in order to create a database connection.
--------------------------------------------
ORACLE_USER - user to connect to the database with
ORACLE_PASSWORD - password for that user
ORACLE_HOST - host for the database
ORACLE_PORT - port for the database
ORACLE_SERVICE - database service
"""

from __future__ import annotations

import datetime
import json
import logging
import logging.config
import pathlib
import re
from typing import TYPE_CHECKING

import constants
import data_types
import db_lib
import duckdb
import env_config
import geopandas as gpd
import numpy
import oracledb
import pandas as pd
import pyarrow
import pyarrow.parquet
import shapely.wkt
import sqlalchemy
import sqlalchemy.types
from env_config import ConnectionParameters
from oracledb.exceptions import DatabaseError

if TYPE_CHECKING:
    import pathlib

    import app_paths


LOGGER = logging.getLogger(__name__)


class OracleDatabase(db_lib.DB):
    """
    Wrapper to access oracle databases.

    By default will attempt to get the following parameters from the
    environment:

    username: ORACLE_USER
    password: ORACLE_PASSWORD
    host: ORACLE_HOST
    port: ORACLE_PORT
    service_name: ORACLE_SERVICE
    """

    def __init__(
        self,
        connection_params: ConnectionParameters,
        app_paths: app_paths.AppPaths,
    ) -> None:
        """
        Contructs instance of OracleDatabase.

        :param connection_params: parameters that are used to connect to the db.
        :type connection_params: ConnectionParameters
        """
        super().__init__(
            connection_params,
            app_paths,
        )  # Call the parent class's __init__ method
        self.ora_cur_arraysize = 2500
        self.data_classification_struct = None

        self.data_classification = DataClassification(
            self.app_paths.get_data_classification_local_path(),
            schema=self.schema_2_sync,
        )

    def get_connection(self) -> None:
        """
        Create a connection to the oracle database.

        Creates a connection to the database using class variables that are
        populated by the object constructor.
        """
        if self.connection is None:
            LOGGER.info("connecting the oracle database: %s", self.service_name)
            dsn = oracledb.makedsn(
                self.host,
                self.port,
                service_name=self.service_name,
            )
            LOGGER.debug("dsn is: %s", dsn)
            self.dsn_conn = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=dsn,
            )

            self.connection = oracledb.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                service_name=self.service_name,
            )
            LOGGER.debug("connected to database")

    def has_raw_columns(self, table_name: str) -> bool:
        """
        Identify if table has any columns of type RAW.

        :param table_name: input table
        :type table_name: str
        :return: boolean that indicates if the table has any raw columns in it.
        :rtype: bool
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT column_name FROM all_tab_columns
        WHERE table_name = :table_name AND data_type = 'RAW'
        """
        cursor.execute(query, table_name=table_name)
        blob_field = cursor.fetchone()
        return blob_field is not None

    def populate_db_type(self) -> None:
        """
        Populate the db_type variable.

        Sets the db_type variable to SPAR.
        """
        self.db_type = constants.DBType.ORA

    def get_sqlalchemy_engine(self) -> None:
        """
        Populate the sqlalchemy engine.

        Using the sql alchemy connection string created in the constructor
        creates a sql_alchemy engine.
        """
        if self.sql_alchemy_engine is None:
            # utf8mb4 AL32UTF8
            dsn = f"oracle+oracledb://{self.username}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}"
            self.sql_alchemy_engine = sqlalchemy.create_engine(
                dsn,
                arraysize=self.ora_cur_arraysize,
            )

    def disable_trigs(self, trigger_list: list[str]) -> None:
        """
        Disable triggers.

        Disables the triggers that are in the trigger_list parameter.

        :param trigger_list: a list of triggers to disable
        :type trigger_list: list[str]
        """
        query = """
        ALTER TRIGGER {trigger_name} DISABLE
        """

        self.get_connection()
        cursor = self.connection.cursor()
        for trigger_name in trigger_list:
            LOGGER.info("disabling trigger %s", trigger_name)
            cursor.execute(query.format(trigger_name=trigger_name))
            LOGGER.debug("trigger %s disabled", trigger_name)
        cursor.close()

    def enable_trigs(self, trigger_list: list[str]) -> None:
        """
        Enable triggers.

        Enables the triggers that are in the trigger_list parameter.

        :param trigger_list: a list of triggers to enable
        :type trigger_list: list[str]
        """
        query = """
        ALTER TRIGGER {trigger_name} ENABLE
        """
        self.get_connection()
        cursor = self.connection.cursor()
        for trigger_name in trigger_list:
            LOGGER.info("enabling trigger %s", trigger_name)
            cursor.execute(query.format(trigger_name=trigger_name))
            LOGGER.debug("trigger %s enabled", trigger_name)
        cursor.close()

    def get_tables(
        self,
        schema: str,
        omit_tables: list[str] | None = None,
    ) -> list[str]:
        """
        Return a list of tables in the provided schema.

        Gets a list of tables that exist in the provided schema arguement.  Any
        tables defined in the omit_tables parameter will be exculded from the
        returned list.

        :param schema: the schema who's tables should be returned
        :type schema: str
        :param omit_tables: list of tables that should be excluded from the list
            of tables that is returned.  Default is []
        :type omit_tables: list, optional
        :return: a list of table names for the given schema
        :rtype: list[str]
        """
        if omit_tables is None:
            omit_tables = []
        if omit_tables:
            omit_tables = [table.upper() for table in omit_tables]
        self.get_connection()
        cursor = self.connection.cursor()
        LOGGER.debug("schema to sync: %s", schema)
        query = "select table_name from all_tables where owner = :schema"
        LOGGER.debug("query: %s", query)
        cursor.execute(query, schema=schema.upper())
        tables = [
            row[0].upper()
            for row in cursor
            if row[0].upper() not in omit_tables
        ]
        cursor.close()
        LOGGER.debug("tables: %s", tables)
        return tables

    def truncate_table(self, table: str, *, casacade: bool = False) -> None:
        """
        Delete all the data from the table.

        :param table: the table to delete the data from
        """
        LOGGER.debug("cascade is ignored for oracle: %s", casacade)
        self.get_connection()
        cursor = self.connection.cursor()
        LOGGER.debug("truncating table: %s", table)
        cursor.execute(f"truncate table {self.schema_2_sync}.{table}")
        self.connection.commit()
        cursor.close()

    def load_data_geoparquet(
        self,
        table: str,
        import_file: pathlib.Path,
    ) -> None:
        """
        Load data from a geoparquet file.

        :param table: the name of the table that already exists, that the
            parquet data will be loaded to.
        :type table: str
        :param import_file: the geoparquet file that will be loaded
        :type import_file: pathlib.Path
        :param purge: _description_, defaults to False
        :type purge: bool, optional
        """
        self.get_connection()  # implement as a decorator!

        cursor = self.connection.cursor()
        cursor.arraysize = self.ora_cur_arraysize

        spatial_column = self.get_geoparquet_spatial_column(import_file)
        spatial_column_wkt = spatial_column + "_wkt"
        spatial_column_wkb = spatial_column + "_wkb"
        LOGGER.debug("spatial_column is: %s", spatial_column)
        # LOGGER.debug("spatial_column wkt is: %s", spatial_column_wkt)

        gdf = gpd.read_parquet(import_file)

        gdf[spatial_column_wkt] = gdf[spatial_column].apply(
            lambda x: shapely.wkt.dumps(x) if x else None,
        )
        gdf[spatial_column_wkb] = gdf[spatial_column].apply(
            lambda x: shapely.wkb.dumps(x) if x else None,
        )

        # patch the nan to None in the df
        gdf = gdf.replace({numpy.nan: None})

        LOGGER.debug(spatial_column)
        LOGGER.debug("gdf columns: %s", gdf.columns)

        columns = self.get_column_list(table)
        LOGGER.debug("columns: %s", columns)
        spatial_columns = self.get_sdo_geometry_columns(table)
        columns_string = ", ".join(columns)
        value_param_list = []
        for column in columns:
            if column in spatial_columns:
                # value_param_list.append(f"SDO_GEOMETRY(:{column}, 3005)")
                value_param_list.append(
                    f"SDO_UTIL.FROM_WKBGEOMETRY(TO_BLOB(:{column}), 3005)"
                )
            else:
                value_param_list.append(f":{column}")
        column_value_str = ", ".join(value_param_list)
        insert_stmt = f"""
            INSERT INTO {self.schema_2_sync}.{table} ({columns_string})
            VALUES ({column_value_str})
        """  # noqa: S608
        LOGGER.debug("statement: %s", insert_stmt)
        LOGGER.debug("Loading data...")
        rowcnt = 0
        for index, row in gdf.iterrows():
            data_dict = {}
            for column in columns:
                if column.lower() == spatial_column.lower():
                    log_msg = (
                        "dealing with spatial column: %s "
                        "gdf column: %s data: %s"
                    )
                    blob_var = cursor.var(oracledb.BLOB)
                    blob_var.setvalue(0, row[spatial_column_wkt])
                    data_dict[column] = blob_var
                else:
                    data_dict[column] = row[column.lower()]
            cursor.execute(insert_stmt, data_dict)
            rowcnt += 1
            if not rowcnt % 1000:
                LOGGER.debug("   inserted rows: %s", 1000 * row)

        cursor.close()
        self.connection.commit()

    def load_data(
        self,
        table: str,
        import_file: pathlib.Path,
        *,
        refreshdb: bool = False,
    ):
        self.get_connection()
        if refreshdb:
            LOGGER.debug("refresh option enabled... truncating table %s", table)
            self.truncate_table(table=table)

        LOGGER.info("importing table %s to %s", table, import_file)
        importer = Importer(
            table_name=table,
            db_schema=self.schema_2_sync,
            oradb=self,
            import_file=import_file,
            data_cls=self.data_classification,
        )
        return importer.import_data()

    def load_data_old_delete_once_working(
        self,
        table: str,
        import_file: pathlib.Path,
        *,
        refreshdb: bool = False,
    ) -> None:
        """
        Load the data from the file into the table.

        Override the default db_lib method.  Identify if the source parquet
        file is geoparquet.  If so then use custom load logic that accomodates
        the spatial column handling.  Otherwise just pass through to the
        parent method.

        :param table: the table to load the data into
        :type table: str
        :param import_file: the file to read the data from
        :type import_file: str
        :param refreshdb: if True, delete the data from the table before loading.
        :type refreshdb: bool
        """
        self.get_connection()
        if refreshdb:
            self.truncate_table(table=table)
        dest_tab_rows = self.get_row_count(table)
        LOGGER.debug("table: %s has %s rows", table, dest_tab_rows)

        # only load data if its a zero row count.
        if not dest_tab_rows:
            if self.is_geoparquet(import_file):
                LOGGER.info("geoparquet table: %s", table)
                LOGGER.info("forking to use the SDO_GEOMETRY loader")
                self.load_data_geoparquet(
                    table=table,
                    import_file=import_file,
                )
            elif self.has_raw_columns(table) or self.has_blob(table):
                self.load_data_with_raw_blobby(
                    table=table,
                    import_file=import_file,
                )
            else:
                LOGGER.debug(
                    "regular parquet file... passing to super class method",
                )
                super().load_data(
                    table,
                    import_file,
                    refreshdb=refreshdb,
                )

    def load_data_delete(
        self,
        table: str,
        import_file: str,
        *,
        purge: bool = False,
    ) -> None:
        """
        Load the data from the file into the table.

        :param table: the table to load the data into
        :type table: str
        :param import_file: the file to read the data from
        :type import_file: str
        :param purge: if True, delete the data from the table before loading.
        :type purge: bool
        """
        # debugging to view the data before it gets loaded
        pandas_df = pd.read_parquet(import_file)
        self.get_connection()  # make sure there is an oracle connection

        LOGGER.debug("loading data for table: %s", table)

        self.get_sqlalchemy_engine()
        if purge:
            self.truncate_table(table=table.lower())
        with (
            self.sql_alchemy_engine.connect() as connection,
            connection.begin(),
        ):
            pandas_df.to_sql(
                table.lower(),
                con=connection,
                schema="THE",
                if_exists="append",
                index=False,
            )
            # now verify data
        sql = "Select count(*) from {schema}.{table}"
        cur = self.connection.cursor()
        cur.execute(sql.format(schema=self.schema_2_sync, table=table))
        result = cur.fetchall()
        rows_loaded = result[0][0]
        if not rows_loaded:
            LOGGER.error("no rows loaded to table %s", table)
        LOGGER.debug("rows loaded to table %s are:  %s", table, rows_loaded)
        cur.close()

    def load_data_retry(
        self,
        table_list: list[str],
        data_dir: pathlib.Path,
        env_str: str,
        retries: int = 1,
        *,
        refreshdb: bool = False,
    ) -> None:
        """
        Load data defined in table_list.

        Gets a list of tables in the table_list parameter, and attempts to load
        the data defined in the data directory to that table.  The load process
        will do the following steps:
            1. Disable foreign key constraints
            1. Truncate the data from all the tables
            1. Load the data from the parquet file to the table
            1. If there is an integrity error, truncate the table and retry
                after the rest of the tables have been loaded.
            1. Enable the constraints

        :param table_list: List of tables to be loaded
        :type table_list: list[str]
        :param data_dir: the directory where the parquet files are stored
        :type data_dir: pathlib.Path
        :param retries: the number of retries that have been attempted,
            defaults to 1
        :type retries: int, optional
        :param max_retries: The maximum number of times the script should
            attempt to load data, if this number is exceeded the integrity
            constraint error will be raised, defaults to 6
        :type max_retries: int, optional
        :param env_str: The environment string, used for path calculations,
            defaults to "TEST"
        :param purge: If set to true the script will truncate the table before
            it attempt a load, defaults to False
        :type purge: bool, optional
        :raises sqlalchemy.exc.IntegrityError: If unable to resolve instegrity
            constraints the method will raise this error
        """
        # get list of fk constraints and disable
        cons_list = self.get_fk_constraints()
        self.disable_fk_constraints(cons_list)

        # ditto for triggers
        trigs_list = self.get_triggers()
        LOGGER.debug("trigs_list: %s", trigs_list)
        self.disable_trigs(trigs_list)

        failed_tables = []
        LOGGER.debug("table list: %s", table_list)
        LOGGER.debug("retries: %s", retries)
        #  "FOREST_COVER_GEOMETRY", "STOCKING_STANDARD_GEOMETRY",
        tables_2_skip = []

        for table in table_list:
            if table.upper() in tables_2_skip:
                LOGGER.warning("skipping the import of the table %s", table)
                continue
            import_file = self.app_paths.get_duckdb_file_path(
                table,
                env_str,
                self.db_type,
            )
            LOGGER.info("Importing table %s %s", " " * retries * 2, table)
            try:
                self.load_data(table, import_file, refreshdb=refreshdb)
            except (
                sqlalchemy.exc.IntegrityError,
                sqlalchemy.exc.DatabaseError,
                DatabaseError,
            ) as e:
                LOGGER.exception(
                    "%s loading table %s",
                    e.__class__.__qualname__,
                    table,
                )
                LOGGER.info("Adding %s to failed tables", table)
                failed_tables.append(table)

                LOGGER.info("truncating failed load table: %s", table)
                self.truncate_table(table=table.lower())

        if failed_tables:
            # for failed tables the data will have already been truncated so
            # can run as refreshdb=False.
            if retries < self.max_retries:
                LOGGER.info("Retrying failed tables")
                retries += 1
                self.load_data_retry(
                    table_list=failed_tables,
                    data_dir=data_dir,
                    env_str=env_str,
                    retries=retries,
                    refreshdb=False,
                )
            else:
                LOGGER.error("Max retries reached for table %s", table)
                self.enable_constraints(cons_list)
                raise sqlalchemy.exc.IntegrityError
        else:
            self.fix_sequences()
            self.enable_constraints(cons_list)

            trigs_list = self.get_triggers()
            self.enable_trigs(trigs_list)

    def purge_data(
        self,
        table_list: list[str],
        retries: int = 1,
        max_retries: int = 10,
        *,
        cascade: bool = False,  # noqa: ARG002
    ) -> None:
        """
        Purge the data from the tables in the list.

        :param table_list: the list of tables to delete the data from
        :type table_list: list[str]
        """
        self.get_connection()
        failed_tables = []
        for table in table_list:
            record_count = self.get_record_count(table)
            if record_count > 0:
                try:
                    self.truncate_table(table=table)
                    LOGGER.info("purged table %s", table)
                except (
                    sqlalchemy.exc.IntegrityError,
                    DatabaseError,
                ):
                    msg = (
                        "error encountered when attempting to purge table:"
                        " %s, retrying"
                    )
                    LOGGER.warning(
                        msg,
                        table,
                    )
                    failed_tables.append(table)
        if failed_tables:
            if retries < max_retries:
                retries += 1
                self.purge_data(failed_tables, retries=retries)
            else:
                LOGGER.error("Max retries reached for table %s", table)
                raise sqlalchemy.exc.IntegrityError

    def get_fk_constraints(self) -> list[db_lib.TableConstraints]:
        """
        Return the foreign key constraints for the schema.

        Queries the schema for all the foreign key constraints and returns a
        list of TableConstraints objects.

        :return: a list of TableConstraints objects that are used to store the
            results of the foreign key constraint query
        :rtype: list[TableConstraints]
        """

        self.get_connection()

        query = """SELECT
                    ac.constraint_name,
                    ac.table_name,
                    acc.column_name,
                    ac.r_constraint_name,
                    arc.table_name AS referenced_table,
                    arcc.column_name AS referenced_column
                FROM
                    all_constraints ac
                    JOIN all_cons_columns acc ON ac.constraint_name =
                        acc.constraint_name
                    JOIN all_constraints arc ON ac.r_constraint_name =
                        arc.constraint_name
                    JOIN all_cons_columns arcc ON arc.constraint_name =
                        arcc.constraint_name
                WHERE
                    ac.constraint_type = 'R'
                    AND ac.owner = :schema
                    AND arc.owner = :schema
                ORDER BY
                    ac.table_name,
                    ac.constraint_name,
                    acc.POSITION"""
        cursor = self.connection.cursor()
        LOGGER.debug("schema_2_sync: %s", self.schema_2_sync)
        cursor.execute(query, schema=self.schema_2_sync.upper())
        constraint_list = []
        for row in cursor:
            LOGGER.debug(row)
            tab_con = db_lib.TableConstraints(*row)
            constraint_list.append(tab_con)
        return constraint_list

    def get_triggers(self) -> list[str]:
        """
        Query the database and return a list of trigger names.

        :return: list of strings containing the triggers that exist in the
            database for the schema that is described by the property
            schema_2_sync
        :rtype: list[str]
        """
        self.get_connection()
        query = """
        SELECT TRIGGER_NAME FROM ALL_TRIGGERS WHERE owner = :schema
        """
        cursor = self.connection.cursor()
        cursor.execute(query, schema=self.schema_2_sync.upper())
        trigger_list = []
        for row in cursor:
            LOGGER.debug("trigger row: %s", row)
            trigger_name = row[0]
            trigger_list.append(trigger_name)
        return trigger_list

    def disable_fk_constraints(
        self,
        constraint_list: list[db_lib.TableConstraints],
    ) -> None:
        """
        Disable all foreign key constraints.

        Iterates through the list of constraints and disables them.

        :param constraint_list: a list of constraints that are to be disabled
            by this method
        :type constraint_list: list[TableConstraints]
        """
        self.get_connection()
        cursor = self.connection.cursor()
        LOGGER.info("disabling constraints...")
        for cons in constraint_list:
            # LOGGER.debug("disabling constraint %s", cons.constraint_name)
            query = (
                f"ALTER TABLE {self.schema_2_sync}.{cons.table_name} "
                f"DISABLE CONSTRAINT {cons.constraint_name}"
            )

            cursor.execute(query)
        cursor.close()

    def enable_constraints(
        self,
        constraint_list: list[db_lib.TableConstraints],
        *,
        retries=0,
        auto_delete=False,
    ) -> None:
        """
        Enable all foreign key constraints.

        Iterates through the list of constraints and enables them.

        :param constraint_list: list of constraints that are to be enabled
        :type constraint_list: list[TableConstraints]
        """
        self.get_connection()
        cursor = self.connection.cursor()
        LOGGER.info("enabling constraints...")
        for cons in constraint_list:
            # LOGGER.info("enabling constraint %s", cons.constraint_name)
            query = (
                f"ALTER TABLE {self.schema_2_sync}.{cons.table_name} "
                f"ENABLE CONSTRAINT {cons.constraint_name}"
            )
            # try:
            cursor.execute(query)
            # except DatabaseError as e:
            #     if retries > 20:
            #         LOGGER.error(
            #             "max retries reached for constraint %s",
            #             cons.constraint_name,
            #         )
            #         raise e
            #     LOGGER.debug("fixing constraints.. failed on %s", cons)
            #     retries += 1
            #     try:
            #         self.disable_fk_constraints(constraint_list)
            #     except:
            #         LOGGER.error("error disabling constraints")
            #         raise Exception("error disabling constraints")
            #     self.delete_no_ri_data(cons)
            #     LOGGER.debug("calling back to enable constraints...")
            #     self.enable_constraints(constraint_list, retries=retries)

        cursor.close()

    def get_no_ri_data(self, constraint: db_lib.TableConstraints):
        """
        Get a records that violating RI integrity constraint.

        :param constraint: a constraint object
        :type constraint: db_lib.TableConstraints
        """
        #     constraint_name: str
        # table_name: str
        # column_names: list[str]
        # r_constraint_name: str
        # referenced_table: str
        # referenced_columns: list[str]

        # SELECT
        # cba.CB_SKEY AS cba,
        # cb.CB_SKEY AS cb,
        # cba.OPENING_ID
        # FROM
        # CUT_BLOCK_OPEN_ADMIN cba
        # FULL OUTER JOIN
        # CUT_BLOCK cb
        # ON
        # cb.CB_SKEY = cba.CB_SKEY
        # WHERE
        # ( cb.CB_SKEY IS NULL AND cba.CB_SKEY IS NOT NULL ) OR
        # ( cb.CB_SKEY IS NOT NULL AND cba.CB_SKEY IS NULL );
        column_name_str = constraint.column_names
        ref_col_name_str = constraint.referenced_columns
        if (
            isinstance(constraint.column_names, list)
            and len(constraint.column_names) > 1
        ):
            column_name_str = "|".join(constraint.column_names)
        if (
            isinstance(constraint.referenced_columns, list)
            and len(constraint.referenced_columns) > 1
        ):
            ref_col_name_str = "|".join(constraint.referenced_columns)
        query = f"""
            SELECT
                T1.{column_name_str} as C1
            FROM
                {constraint.table_name} T1
            FULL OUTER JOIN
                {constraint.referenced_table} T2
            ON
                T1.{column_name_str} = T2.{ref_col_name_str}
            WHERE
                ( T1.{column_name_str} is not null and T2.{ref_col_name_str} is null )
        """
        # OR ( T1.{column_name_str} is null and T2.{ref_col_name_str} is not null )
        LOGGER.debug("query: %s", query)
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        # flatten the struct
        records_to_delete = [rec[0] for rec in results]
        LOGGER.debug("records_to_delete: %s...", records_to_delete[0:10])
        LOGGER.debug("len of results: %s", len(results))
        return records_to_delete

    def delete_no_ri_data(self, constraint: db_lib.TableConstraints) -> None:
        """
        delete data that has referential interity constraint issues.
        """
        LOGGER.debug(
            "trying to fix data associated with constraint: %s", constraint
        )
        if (
            isinstance(constraint.column_names, list)
            and len(constraint.column_names) > 1
        ):
            LOGGER.error("unsupported: fix for this constraint: %s", constraint)
            msg = (
                f"the constraint {constraint.constraint_name} uses more "
                "than one column to define the referential integrity "
                "this is a use case that is not currently supported"
            )
            # TODO: should really define my own error here
            raise ValueError(msg)

        no_ri_data_all = self.get_no_ri_data(constraint)
        cursor = self.connection.cursor()
        for i in range(0, len(no_ri_data_all), 10000):
            no_ri_data = no_ri_data_all[i : i + 10000]

            if not isinstance(no_ri_data[0], str):
                no_ri_str = ",".join([str(val) for val in no_ri_data])
            else:
                no_ri_str = ",".join([f"'{val}'" for val in no_ri_data])
            query = f"DELETE FROM {constraint.table_name} where {constraint.column_names} in ({no_ri_str})"
            LOGGER.debug("delete not ri records query: %s ...", query[0:200])

            cursor.execute(query)
        cursor.close()

    def fix_sequences(self) -> None:
        """
        Fix the sequences.

        Identify triggers that use sequences, then identify the table and column
        that the triggers populate, and make sure the sequences nextval is
        higher than the max() value for that column.
        """
        seq_fix = FixOracleSequences(self)
        seq_fix.fix_sequences()

    def get_sdo_geometry_columns(self, table_name: str) -> list[str]:
        """
        Get the SDO_GEOMETRY columns for the table.

        :param table_name: the name of the table to get the SDO_GEOMETRY columns
            for
        :type table_name: str
        :return: a list of the SDO_GEOMETRY columns for the table
        :rtype: list[str]
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT
            column_name
        FROM
            all_tab_columns
        WHERE
            --user_generated = 'YES' AND
            data_type = 'SDO_GEOMETRY' AND
            table_name = :table_name AND
            owner = :schema_name
        """
        LOGGER.debug("query: %s", query)
        LOGGER.debug("table_name: %s", table_name)
        schema_name = self.schema_2_sync
        cursor.execute(
            query,
            table_name=table_name,
            schema_name=schema_name.upper(),
        )
        sdo_geometry = cursor.fetchall()
        LOGGER.debug("sdo_geometry cols: %s", sdo_geometry)
        return [row[0] for row in sdo_geometry]

    def get_blob_columns(self, table_name: str) -> list[str]:
        """
        Return list of columns in the table that are defined as BLOB.

        :param table_name: input table
        :type table_name: str
        :return: list of columns from the database that are defined as BLOB's.
        :rtype: list[str]
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT
            column_name
        FROM
            all_tab_columns
        WHERE
            --user_generated = 'YES' AND
            data_type = 'BLOB' AND
            table_name = :table_name AND
            owner = :schema_name
        """
        LOGGER.debug("query: %s", query)
        LOGGER.debug("table_name: %s", table_name)
        schema_name = self.schema_2_sync
        cursor.execute(
            query,
            table_name=table_name,
            schema_name=schema_name.upper(),
        )
        blob_cols = cursor.fetchall()
        LOGGER.debug("blob_cols: %s", blob_cols)
        return [row[0] for row in blob_cols]

    def get_raw_columns(self, table_name: str) -> list[str]:
        """
        Return a list of columns defined as RAW.

        :param table_name: input table name
        :type table_name: str
        :return: list of columns that are defined as RAW in the database.
        :rtype: list[str]
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT
            column_name
        FROM
            all_tab_columns
        WHERE
            data_type = 'RAW' AND
            table_name = :table_name AND
            owner = :schema_name
        """
        LOGGER.debug("query: %s", query)
        LOGGER.debug("table_name: %s", table_name)
        schema_name = self.schema_2_sync
        cursor.execute(
            query,
            table_name=table_name,
            schema_name=schema_name.upper(),
        )

        raw_cols = cursor.fetchall()

        LOGGER.debug("raw_cols: %s", raw_cols)
        return [row[0] for row in raw_cols]

    def has_sdo_geometry(self, table_name: str) -> bool:
        """
        Check if the table has a SDO_GEOMETRY column.

        :param table_name: the name of the table to check
        :type table_name: str
        :return: True if the table has a SDO_GEOMETRY column, False if it does
            not
        :rtype: bool
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT column_name FROM all_tab_columns
        WHERE table_name = :table_name AND data_type = 'SDO_GEOMETRY'
        """
        cursor.execute(query, table_name=table_name)
        sdo_geometry = cursor.fetchone()
        return sdo_geometry is not None

    def has_blob(self, table_name: str) -> bool:
        """
        Identify if the table has any columns of type BLOB.

        :param table_name: input table name
        :type table_name: str
        :return: boolean that indicates if the table has any BLOB defined
                 columns
        :rtype: bool
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT column_name FROM all_tab_columns
        WHERE table_name = :table_name AND data_type = 'BLOB'
        """
        cursor.execute(query, table_name=table_name)
        blob_field = cursor.fetchone()
        return blob_field is not None

    def get_column_list(
        self,
        table: str,
        with_type: bool = False,
        with_length_precision_scale: bool = False,
    ) -> list[str | tuple[str | int]]:
        """
        Get the list of columns for the table.

        :param table: the table to get the columns for
        :type table: str
        :param with_type: indicates if the returned object should include the
            data type. When true the return object will be a list of lists where
            the inner list is [column_name, data_type]
        :type with_type: bool
        :param with_length_precision_scale: indicates if the returned object
            should include the length, precision and scale of the column.  When
            this is true regardless of what was specified for with_type the
            returned object include the type.  example of a single row that
            gets returned when this is set to true:
            [column_name, data_type, length, precision, scale]
        :type with_length_precision_scale: bool
        :return: a list of the columns for the table
        :rtype: list[str]
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT
            column_name, data_type, data_length, data_precision, data_scale
        FROM
            all_tab_cols
        WHERE
            table_name = :table_name AND
            owner = :schema_name AND
            user_generated = 'YES'
        ORDER BY
            SEGMENT_COLUMN_ID
        """
        # LOGGER.debug("query: %s", query)
        # LOGGER.debug("table: %s", table.upper())
        # LOGGER.debug("schema: %s", self.schema_2_sync.upper())
        cursor.execute(
            query,
            table_name=table.upper(),
            schema_name=self.schema_2_sync.upper(),
        )
        results = cursor.fetchall()
        columns = [row[0].upper() for row in results]
        if with_length_precision_scale:
            columns = [
                [
                    row[0].upper(),
                    constants.ORACLE_TYPES[row[1]],
                    row[2],
                    row[3],
                    row[4],
                ]
                for row in results
            ]
        elif with_type:
            columns = [
                [row[0].upper(), constants.ORACLE_TYPES[row[1]]]
                for row in results
            ]
        return columns

    def generate_sdo_query(self, table: str) -> str:
        """
        Generate an SDO query.

        Generate a query that will extract the data from a table with
        SDO_GEOMETRY columns as well known text (WKT)

        :param table: input table name
        :type table: str
        :return: sdo query with geometries as WKT
        :rtype: str
        """
        column_list = self.get_column_list(table)
        geometry_columns = self.get_sdo_geometry_columns(table)
        column_with_wkb_func = []

        for column in column_list:
            if column in geometry_columns:
                column_with_wkb_func.append(
                    f"SDO_UTIL.TO_WKTGEOMETRY({column}) AS {column}",
                )
            else:
                column_with_wkb_func.append(column)
        # Could use the approach used in the extract_data_illegal_year method
        # to patch the query to handle sdo data.
        query = (
            f"SELECT {', '.join(column_with_wkb_func)} from "  # noqa: S608
            f"{self.schema_2_sync.upper()}.{table.upper()}"
        )
        LOGGER.debug("sdo query: %s", query)
        return query

    def generate_extract_sql_query(self, table: str) -> str:
        """
        Return a sql query to extract the table with.

        The query addresses the following conditions:
        * blob data is not copied and is replaced with EMPTY_BLOB() as <column>
        * sdo geometry is converted to wkt
        * if columns have been defined as sensitive then they are not extracted
            and are replaced with nulls if nulls and placeholder if not.
            placeholder values are dependent on data type.  For example
            varchar will get '1' nums 1 etc..

        :param table: input table to generate the table for
        :type table: str
        :return: a query that can be used to extract the data in the table.
        :rtype: str
        """
        mask_obj = db_lib.DataMasking()
        mask_columns = mask_obj.get_masked_columns(table_name=table)

        column_list = self.get_column_list(table, with_type=True)
        blob_columns = self.get_blob_columns(table)
        sdo_geometry_columns = self.get_sdo_geometry_columns(table)

        select_column_list = []

        for column in column_list:
            column_name = column[0]
            column_type = column[1]
            if column_name in blob_columns:
                select_column_list.append(
                    f"EMPTY_BLOB() AS {column}",
                )
            elif column_name in sdo_geometry_columns:
                select_column_list.append(
                    f"SDO_UTIL.TO_WKTGEOMETRY({column}) AS {column}",
                )
            elif column_name in mask_columns:
                mask_dummy_val = mask_obj.get_mask_dummy_val(column_type)
                select_column_list.append(f"{mask_dummy_val} AS {column_name}")
        query = (
            f"SELECT {', '.join(select_column_list)} from "  # noqa: S608
            f"{self.schema_2_sync}.{table}"
        )
        LOGGER.debug("query: %s", query)
        return query

    def generate_blob_query(self, table: str) -> str:
        """
        Return SQL that masks BLOBS using EMPTY_BLOB().

        :param table: Input table name.
        :type table: str
        :return: SQL that will return all the columns in the table, but for
                 any columns defined as BLOB, the data will be removed.
        :rtype: str
        """
        column_list = self.get_column_list(table)
        blob_columns = self.get_blob_columns(table)
        columns_with_blob_func = []
        for column in column_list:
            if column in blob_columns:
                columns_with_blob_func.append(
                    f"EMPTY_BLOB() AS {column}",
                )
            else:
                columns_with_blob_func.append(column)
        # Could use the approach used in the extract_data_illegal_year method
        # to patch the query to handle sdo data.
        query = (
            f"SELECT {', '.join(columns_with_blob_func)} from "  # noqa: S608
            f"{self.schema_2_sync}.{table}"
        )
        LOGGER.debug("sdo query: %s", query)
        return query

    def get_temp_parquet_file(self, *, overwrite: bool = True) -> pathlib.Path:
        """
        Get a path to a temporary parquet file.

        :param overwrite: if True checks to see if the file exists and deletes
            it.
        :type overwrite: bool
        :return: the path to the temporary parquet file
        :rtype: pathlib.Path
        """

        temp_parquet_file = self.app_paths.get_temp_parquet_file(
            self.db_type,
            overwrite=overwrite,
        )
        LOGGER.debug("temp parquet file: %s", temp_parquet_file)
        return temp_parquet_file

    def get_pyarrow_schema_from_db(
        self,
        description: oracledb.Cursor.description,
        spatial_column_name: str = None,
    ):
        # constants.PYTHON_PYARROW_TYPE_MAP
        LOGGER.debug("description: %s", description)
        schema_map = []
        for column_obj in description:
            column_type = column_obj[1]
            if column_type not in constants.DB_TYPE_PYARROW_MAP:
                msg = (
                    f"the query returned a type of {column_type}, which does "
                    "not have a mapping to the defined PYARROW mappings: "
                    f"{constants.PYTHON_PYARROW_TYPE_MAP}"
                )
                raise KeyError(msg)
            pyarrow_type = constants.DB_TYPE_PYARROW_MAP[column_type]
            if (spatial_column_name) and column_obj[
                0
            ].lower() == spatial_column_name.lower():
                # make the type binary as it will be converted from WKT to WKB
                pyarrow_type = pyarrow.binary()
            # if column_obj[0].lower() == spatial_column_name.lower():
            #     # trying to debug by taking spatial oout
            #     pass
            # else:
            schema_map.append(
                [
                    column_obj[0].lower(),
                    pyarrow_type,
                ]
            )

        schema = pyarrow.schema([(col[0], col[1]) for col in schema_map])
        return schema

    def extract_data_sdogeometry_ddb(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,  # noqa: ARG002
        chunk_size: int = 25000,
        max_records: int = 0,
    ):
        self.get_sqlalchemy_engine()
        self.get_connection()
        spatial_columns = self.get_sdo_geometry_columns(table)

        if len(spatial_columns) > 1:
            msg = "only one spatial column is supported"
            raise ValueError(msg)
        spatial_col = spatial_columns[0]
        query = self.generate_sdo_query(table)
        LOGGER.debug("spatial query: %s", query)

        temp_ddb_db = self.app_paths.get_temp_duckdb_path()
        if temp_ddb_db.exists():
            temp_ddb_db.unlink()

        ddb_con = duckdb.connect(database=temp_ddb_db)
        ddb_con.install_extension("spatial")
        ddb_con.load_extension("spatial")
        ddb_con.execute(f"SET memory_limit='{constants.DUCK_DB_MEM_LIM}'")
        itercnt = 0
        first = True
        df_cols = None
        for chunk in pd.read_sql(
            query,
            self.sql_alchemy_engine,
            chunksize=chunk_size,
        ):
            chunk[spatial_col.lower()] = chunk[spatial_col.lower()].apply(
                shapely.wkt.loads
            )
            chunk[spatial_col.lower()] = chunk[spatial_col.lower()].apply(
                shapely.wkb.dumps
            )
            df_cols = chunk.columns

            if first:
                # chunk.to_sql(table, ddb_con, if_exists="replace", index=False)
                LOGGER.debug(
                    "creating the table, writing first chunk: %s", chunk_size
                )
                ddb_con.sql("CREATE TABLE my_table AS SELECT * FROM chunk")
                first = False
            else:
                # chunk.to_sql(table, ddb_con, if_exists="append", index=False)
                # insert into the table "my_table" from the DataFrame "my_df"
                LOGGER.debug(
                    "writing chunk to the table (chunk/chunk_size), (%s/%s)",
                    itercnt + 1,
                    (itercnt + 1) * chunk_size,
                )
                ddb_con.sql("INSERT INTO my_table SELECT * FROM chunk")
            if (max_records) and itercnt * chunk_size > max_records:
                break
            itercnt += 1

        # now write to geoparquet
        LOGGER.debug("write to parquet...")
        fix_cols = []
        for col in df_cols:
            if col.lower() == spatial_col.lower():
                fix_cols.append(
                    f"ST_GeomFromWKB({spatial_col}) AS {spatial_col}"
                )
            else:
                fix_cols.append(col)
        ddb_2_parquet_query_str = f"""
            COPY (select {", ".join(fix_cols)} FROM my_table)
            TO '{export_file}' (FORMAT PARQUET);
        """
        LOGGER.debug(ddb_2_parquet_query_str)
        LOGGER.info("writing to parquet file: %s", export_file)
        ddb_con.sql(ddb_2_parquet_query_str)
        LOGGER.debug("file has been extracted!")
        ddb_con.close()
        if export_file.exists() and temp_ddb_db.exists():
            LOGGER.debug("remove the temporary duck db file: %s", temp_ddb_db)
        return True

    def extract_data_sdogeometry(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,  # noqa: ARG002
        chunk_size: int = 25000,
        max_records: int = 0,
    ) -> bool:
        """
        Extract data from a table that contains SDO_GEOMETRY columns.

        :param table: table who's data should be extracted
        :type table: str
        :param export_file: the file that the data will be written to
        :type export_file: pathlib.Path
        :param overwrite: if the file already exists what to do
        :type overwrite: bool, optional
        :param chunk_size: number of records to read from database at a time.
        :type chunk_size: int, optional
        :param max_records: if set defines the maximum number of records to read.
            will be the chunk_size * x > max_records
        :type max_records: int, optional
        :return: was the file successfully created
        :rtype: bool
        """
        self.get_sqlalchemy_engine()
        self.get_connection()

        column_list = self.get_column_list(table)
        LOGGER.debug("column list: %s", column_list)

        spatial_columns = self.get_sdo_geometry_columns(table)
        if len(spatial_columns) > 1:
            msg = "only one spatial column is supported"
            raise ValueError(msg)
        spatial_col = spatial_columns[0]

        query = self.generate_sdo_query(table)
        LOGGER.debug("spatial query: %s", query)

        # need to get the types
        cur = self.connection.cursor()
        cur.execute(query)
        pyarrow_schema = self.get_pyarrow_schema_from_db(
            description=cur.description, spatial_column_name=spatial_col
        )
        cur.close()
        LOGGER.debug("pyarrow schema: %s", pyarrow_schema)

        writer = None

        # chunk_size = 25000  # 25000  # number of rows to include in a chunk
        itercnt = 1
        for chunk in pd.read_sql(
            query,
            self.sql_alchemy_engine,
            chunksize=chunk_size,
        ):
            if chunk.empty:
                itercnt += 1
                LOGGER.debug("empty dataframe... itercnt: %s", itercnt)
                continue
            if itercnt == 1:
                # after first chunk is read, convert it to a pyarrow table and
                # use that as the schema for the stream writer.
                LOGGER.debug(
                    "first chunk has been read, rows: %s",
                    len(chunk),
                )
                LOGGER.debug(
                    "column name and types %s / %s", chunk.columns, chunk.dtypes
                )
                table = self.df_to_gdf(chunk, spatial_col, pyarrow_schema)
                # define the geoparquet metadata:
                geo_metadata = {
                    "version": "1.0.0",
                    "primary_column": spatial_col.lower(),
                    "columns": {
                        spatial_col.lower(): {
                            "encoding": "WKB",
                            "geometry_type": "Unknown",  # You can specify "Point", "Polygon", etc.
                            "crs": "EPSG:3005",  # Set CRS if applicable
                        }
                    },
                }

                # 🔹 Attach metadata to schema (only once, at creation)
                metadata = (
                    pyarrow_schema.metadata or {}
                )  # Get existing metadata if any
                metadata[b"geo"] = json.dumps(geo_metadata).encode(
                    "utf-8"
                )  # Store as bytes
                pyarrow_schema = pyarrow_schema.with_metadata(
                    metadata
                )  # Update schema with metadata
                LOGGER.debug("pyarrow schema: %s", pyarrow_schema)

                writer = pyarrow.parquet.ParquetWriter(
                    str(export_file),
                    pyarrow_schema,
                    compression="snappy",
                )
                itercnt += 1
                writer.write_table(table)
                LOGGER.debug(
                    "records read (max recs): %s (%s)",
                    itercnt * chunk_size,
                    max_records,
                )

            LOGGER.debug(
                "read chunk:%s chunks read: %s",
                chunk_size,
                chunk_size * itercnt,
            )
            table = self.df_to_gdf(chunk, spatial_col, pyarrow_schema)
            LOGGER.debug("    writing chunk to parquet file...")
            writer.write_table(table)

            if (max_records) and itercnt * chunk_size > max_records:
                break
            itercnt += 1
            continue
        LOGGER.debug("closing the file")
        writer.close()
        return True

    def extract_data_blob(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,  # noqa: ARG002
        chunk_size=25000,
        max_records=0,
    ) -> bool:
        """
        Write table with BLOB data to parquet file.

        The query that is used by this method will mask out any of the BLOB data
        with the function EMPTY_BLOB as we do not require any of the BLOB data
        in the downstream env used by this tool, atm.

        :param table: Input table name
        :type table: str
        :param export_file: The path to the parquet file
        :type export_file: pathlib.Path
        :param overwrite: identify if we want to overwrite existing files
        :type overwrite: bool, optional
        :raises ValueError: error is raised if no BLOB columns are found in the
            table, as a different method should be used in these circumstances.
        :return: a boolean that indicates that the dump succeeded.
        :rtype: bool
        """
        column_list = self.get_column_list(table)
        blob_columns = self.get_blob_columns(table)
        if len(blob_columns) > 1:
            msg = "only one blob column is supported"
            raise ValueError(msg)

        LOGGER.debug("column list: %s", column_list)
        query = self.generate_blob_query(table)
        LOGGER.debug("spatial query: %s", query)
        self.get_sqlalchemy_engine()

        writer = None

        itercnt = 1
        for chunk in pd.read_sql(
            query,
            self.sql_alchemy_engine,
            chunksize=chunk_size,
        ):
            table = pyarrow.Table.from_pandas(chunk)
            if itercnt == 1:
                # after first chunk is read, convert it to a pyarrow table and
                # use that as the schema for the stream writer.
                LOGGER.debug(
                    "first chunk has been read, rows: %s",
                    len(chunk),
                )
                writer = pyarrow.parquet.ParquetWriter(
                    str(export_file),
                    table.schema,
                    compression="snappy",
                )
                itercnt += 1
                writer.write_table(table)
                continue
            LOGGER.debug(
                "read chunk:%s chunks read: %s",
                chunk_size,
                chunk_size + itercnt,
            )
            LOGGER.debug("    writing chunk to parquet file...")
            writer.write_table(table)
            if (max_records) and chunk_size * itercnt > max_records:
                break
            itercnt += 1
        writer.close()
        return True

    def get_table_columns(self, table: str) -> list[str]:
        """
        Get the columns for the table.

        :param table: the table to get the columns for
        :type table: str
        :return: a list of the columns for the table
        :rtype: list[str]
        """
        query = (
            "SELECT column_name FROM  ALL_TAB_COLUMNS WHERE "
            f"owner = '{self.schema_2_sync}' AND TABLE_NAME = '{table}'"
        )
        cur = self.connection.cursor()
        cur.execute(cur)
        results = cur.fetchall()
        return [row[0] for row in results]

    # def has_masked_data(self, table: str) -> bool:
    #     """
    #     Identify if the table has any columns of type RAW.

    #     :param table: input table name
    #     :type table: str
    #     :return: boolean that indicates if the table has any RAW defined
    #              columns
    #     :rtype: bool
    #     """
    #     mask_table = [
    #         mask_descriptor.table_name.upper()
    #         for mask_descriptor in constants.DATA_TO_MASK
    #         if mask_descriptor.table_name.upper() == table.upper()
    #     ]
    #     if mask_table:
    #         return True
    #     return False

    def df_to_gdf(
        self, df: pd.DataFrame, spatial_col: str, pyarrow_schema: pyarrow.Schema
    ) -> gpd.GeoDataFrame:
        """
        Convert a pandas DataFrame to a geopandas GeoDataFrame.

        Also store information as wkb to allow for writing to parquet through
        the stream writer.

        :param df: the pandas DataFrame to convert
        :type df: pd.DataFrame
        :param spatial_col: the name of the spatial column
        :type spatial_col: str
        """
        LOGGER.debug("    converting df to gdf...")
        # gdf = gpd.GeoDataFrame(
        #     df,
        #     geometry=gpd.GeoSeries.from_wkt(
        #         df[spatial_col.lower()],
        #     ),
        #     crs="EPSG:3005",
        # )

        # # Convert to Shapely geometries
        # gdf[spatial_col.lower()] = gdf[spatial_col.lower()].apply(
        #     shapely.wkb.dumps
        # )

        # trying to hack the geopandas dataframe without using geopandas
        # rely on the metadata to inform that geopandas

        # convert from WKT to WKB
        LOGGER.debug("df types: %s", df.dtypes)
        LOGGER.debug("sample spatial data: %s", df[spatial_col.lower()].head(5))
        # df[spatial_col.lower()] = df[spatial_col.lower()].apply(
        #     shapely.wkt.loads
        # )
        # df[spatial_col.lower()] = df[spatial_col.lower()].apply(
        #     shapely.wkb.dumps
        # )
        if not isinstance(df[spatial_col.lower()].head(1)[0], bytes):
            LOGGER.debug("convert to WKB")
            df[spatial_col.lower()] = df[spatial_col.lower()].apply(
                lambda x: shapely.wkb.dumps(shapely.wkt.loads(x))
            )

        tabletmp = pyarrow.Table.from_pandas(df)
        LOGGER.debug("pyarrow schema: %s", tabletmp.schema)
        table = pyarrow.Table.from_pandas(df, pyarrow_schema)
        # Convert the GeoDataFrame to a PyArrow Table
        return table

    def extract_data(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,
        chunk_size: int = 25000,
        max_records: int = 0,
    ) -> bool:
        LOGGER.info("exporting table %s to %s", table, export_file)
        extract = Extractor(
            table_name=table,
            db_schema=self.schema_2_sync,
            oradb=self,
            export_file=export_file,
            data_cls=self.data_classification,
        )
        return extract.extract()

    # def load_data_classification(self) -> None:
    #     """
    #     Load the data classification from the database.

    #     :return: None
    #     """
    #     if not self.data_classification_struct:
    #         self.data_classification_struct = db_lib.DataClassification(
    #             self,
    #             schema=self.schema_2_sync,
    #         )
    #         self.data_classification_struct.load_data_classification()

    def extract_data_delete_me(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,
        chunk_size: int = 25000,
        max_records: int = 0,
    ) -> bool:
        """
        Extract a table from the database to a parquet file.

        This method overrides the default method id db_lib to enable forking
        logic when a table with a SDO_GEOMETRY column is encountered.

        :param table: the name of the table who's data will be copied to the
            parquet file
        :type table: str
        :param export_file: the full path to the file that will be created, and
            populated with the data from the table.
        :type export_file: str
        :param overwrite: if True the file will be overwritten if it exists,
        :return: True if the file was created, False if it was not
        :rtype: bool
        """
        LOGGER.debug("exporting table %s to %s", table, export_file)
        file_created = False

        if self.has_sdo_geometry(table):
            LOGGER.info("table %s has SDO_GEOMETRY column", table)
            LOGGER.info("forking to use the SDO_GEOMETRY extractor")
            # extract_data_sdogeometry_ddb
            # file_created = self.extract_data_sdogeometry(
            file_created = self.extract_data_sdogeometry_ddb(
                table=table,
                export_file=export_file,
                overwrite=overwrite,
                chunk_size=chunk_size,
                max_records=max_records,
            )
        elif self.has_blob(table):
            LOGGER.info("table %s has a field with BLOB type", table)
            LOGGER.info("using the oracle specific BLOB extractor")
            file_created = self.extract_data_blob(
                table=table,
                export_file=export_file,
                overwrite=overwrite,
                chunk_size=chunk_size,
                max_records=max_records,
            )

        else:
            try:
                file_created = super().extract_data(
                    table,
                    export_file,
                    overwrite=overwrite,
                    chunk_size=chunk_size,
                    max_records=max_records,
                )
            except ValueError as e:
                if str(e) == "year -1 is out of range":
                    LOGGER.debug(
                        "Caught the ValueError: %s, trying workaround...",
                        e,
                    )
                    self.extract_data_illegal_year(
                        table=table,
                        export_file=export_file,
                    )
                else:
                    msg = "error that is raised is not an illegal date"
                    raise ValueError(msg) from e
        return file_created

    def extract_data_illegal_year(
        self,
        table: str,
        export_file: pathlib.Path,
    ) -> bool:
        """
        Make illegal dates legal and then dump the data to a parquet file.

        Some dates that are being extracted from oracle include dates with
        negative years.  This causes issues at the python db api level.  The
        workaround implemented here moodifies the query that is used to retrieve
        dates from the database, so that any dates with years of < 0 are changed
        to having a year or 1.  I have no idea WHY there would be dates in the
        database that have years of -1???

        :param table: the input table that is to be exported
        :type table: str
        :param export_file: the name of the export file that is to be created.
        :type export_file: pathlib.Path
        :return: a boolean indicating whether the method succeded or failed.
        :rtype: bool
        """

        self.get_connection()
        table_obj = self.get_table_object(table)
        select_obj = sqlalchemy.select(table_obj)
        # convert the select to a string
        select_str = str(
            select_obj.compile(compile_kwargs={"literal_binds": True}),
        )

        # iterate over the columns, finding the date columns and replace
        # with a function that should render the dates with -1 to be 1, then
        # replace that column in the sql statement
        for column in select_obj.columns:
            LOGGER.debug("column: %s type: %s", column, column.type)
            if isinstance(column.type, sqlalchemy.DateTime):
                column_name = (
                    f"{self.schema_2_sync}.{table}.{column.name}".lower()
                )
                column_new = (
                    f"to_date(to_char({column_name}, "
                    "'YYYY-MM-DD HH:MM:SS'), 'YYYY-MM-DD HH24:MI:SS') as "
                    f"{column.name}"
                )
                select_str = select_str.replace(column_name, column_new)

        # execute query and load results to the dataframe
        cur = self.connection.cursor()
        cur.execute(select_str)
        df_orders = pd.DataFrame(cur.fetchall())

        # finally write the dataframe to the parquet file
        df_orders.to_parquet(
            export_file,
            engine="pyarrow",
        )
        return True

    def is_geoparquet(self, parquet_file_path: pathlib.Path) -> bool:
        """
        Identify if the path refers to a geoparquet file.

        In order to support spatial data some of the parquet files that are used
        to cache the data are geoparquet files and require different
        functionality in order to load.

        :param parquet_file_path: path to input parquet file
        :type parquet_file_path: pathlib.Path
        :return: bool that indicates if the file is a generic parquet file
            or a geoparquet file.
        :rtype: bool
        """
        metadata = pyarrow.parquet.read_metadata(parquet_file_path)
        is_geo_parquet = False

        # Check for GeoParquet-specific metadata
        if b"geo" in metadata.metadata:
            LOGGER.debug("input parquet is geo! %s", parquet_file_path)
            is_geo_parquet = True
        # if not is_geo_parquet:
        #     gdf = gpd.read_parquet(str(parquet_file_path))
        #     # Check if the DataFrame has a geometry column
        #     if isinstance(gdf, gpd.GeoDataFrame) and gdf.geometry is not None:
        #         is_geo_parquet = True

        return is_geo_parquet

    def get_geoparquet_spatial_column(
        self,
        parquet_file_path: pathlib.Path,
    ) -> str | None:
        """
        Get the spatial column from the parquet file if one exists.

        :param parquet_file_path: The path to the parquet file
        :type parquet_file_path: pathlib.Path
        :return: the name of the spatial column if one exists.
        :rtype: str|None
        """
        metadata = pyarrow.parquet.read_metadata(parquet_file_path)
        spatial_column = None
        if b"geo" in metadata.metadata:
            geo_metadata = json.loads(metadata.metadata[b"geo"].decode("utf-8"))
            spatial_column = geo_metadata.get("primary_column")

            # all spatial columns? geo_metadata.get('columns', {}).keys()
        return spatial_column

    def load_data_with_raw_blobby(
        self,
        table: str,
        import_file: pathlib.Path,
    ) -> None:
        """
        Load data from parquet file to oracle table with BLOB columns.

        :param table: table name
        :type table: str
        :param import_file: import file path
        :type import_file: pathlib.Path
        :param purge: Should the contents of the file be purged before writing,
                      defaults to False
        :type purge: bool, optional
        """
        # make sure there is a connection
        self.get_connection()
        self.get_sqlalchemy_engine()

        total_rows_read = 0

        # delete records if necessary
        LOGGER.debug("loading data to table: %s", table)
        with (
            self.sql_alchemy_engine.connect() as connection,
            connection.begin(),
        ):
            # set up a chunk reader
            LOGGER.debug("reading parquet file, %s", import_file)
            parquet_reader = pyarrow.parquet.ParquetFile(import_file)
            iter_cnt = 1

            # get blob columns
            blob_columns = self.get_blob_columns(table)
            LOGGER.debug("blob_columns: %s", blob_columns)

            # Reading the data from parquet, in chunks
            for batch in parquet_reader.iter_batches(
                batch_size=self.chunk_size,
            ):
                # messaging
                end_row_cnt = iter_cnt * self.chunk_size
                start_row_cnt = end_row_cnt - self.chunk_size
                LOGGER.debug(
                    "writing rows from %s to %s",
                    start_row_cnt,
                    end_row_cnt,
                )

                # pyarrow record batch to pandas dataframe
                to_ora_df = batch.to_pandas()
                total_rows_read = total_rows_read + len(to_ora_df)
                # override the current class implementation with my own to_sql
                # method
                to_ora_df.__class__ = DataFrameExtended

                LOGGER.debug("columns in dataframe: %s", to_ora_df.columns)
                to_ora_df.to_sql(
                    name=table,
                    con=self.connection,
                    if_exists="append",
                    index=False,
                    blob_cols=blob_columns,
                )
                iter_cnt += 1

        # now verify data load
        self.connection.commit()
        rows_loaded = self.get_row_count(table_name=table)

        if not rows_loaded:
            LOGGER.error("no rows loaded to table %s", table)
        LOGGER.info("rows loaded to table %s are:  %s", table, rows_loaded)
        LOGGER.info("rows in parquet file: %s", total_rows_read)
        if rows_loaded != total_rows_read:
            LOGGER.warning(
                "discrepancy between source and dest data for table: %s",
                table,
            )


class FixOracleSequences:
    """
    Fix the sequences in the oracle database.
    """

    def __init__(self, dbcls: OracleDatabase) -> None:
        """
        Construct instance of the FixOracleSequences class.

        :param dbcls: an OracleDatabase class
        :type dbcls: OracleDatabase
        """
        self.dbcls = dbcls

    def get_triggers_with_sequences(self) -> list[env_config.TriggerSequence]:
        """
        Get the triggers that use sequences.

        :return: A list of Trigger Sequence Objects
        :rtype: list[env_config.TriggerSequence]
        """
        query = """
            SELECT
                owner,
                name AS trigger_name,
                referenced_name AS sequence_name
            FROM
                all_dependencies
            WHERE
                owner = 'THE' AND
                TYPE = 'TRIGGER'
                AND REFERENCED_TYPE = 'SEQUENCE'
        """
        self.dbcls.get_connection()
        cursor = self.dbcls.connection.cursor()
        cursor.execute(query)
        trig_seq_list = []
        for row in cursor:
            trig_seq = env_config.TriggerSequence(
                owner=row[0],
                trigger_name=row[1],
                sequence_name=row[2],
            )
            trig_seq_list.append(trig_seq)
        return trig_seq_list

    def get_trigger_body(
        self,
        trigger_name: str,
        trigger_owner: str,
    ) -> env_config.TriggerBodyTable:
        """
        Extract the trigger body from the database.

        :param trigger_name: name of the trigger to extract
        :type trigger_name: str
        :param trigger_owner: owner of the trigger / schema
        :type trigger_owner: str
        :return: the trigger body in a TriggerBodyTable object
        :rtype: env_config.TriggerBodyTable
        """
        LOGGER.debug("getting trigger body")
        query = """
            SELECT
                table_owner,
                table_name,
                trigger_body
            FROM
                all_triggers
            WHERE
                trigger_name = :trigger_name
                AND owner = :trigger_owner
        """
        self.dbcls.get_connection()
        cursor = self.dbcls.connection.cursor()
        cursor.execute(
            query,
            trigger_name=trigger_name,
            trigger_owner=trigger_owner,
        )
        trigger_struct = cursor.fetchone()
        cursor.close()

        return env_config.TriggerBodyTable(
            trigger_body=trigger_struct[2],
            table_name=trigger_struct[1],
            table_owner=trigger_struct[0],
        )

    def extract_inserts(self, trigger_body: str, table_name: str) -> str:
        """
        Extract the insert statements from the trigger body.

        :param trigger_body: a string with a trigger definition
        :type trigger_body: str
        :param table_name: name of the table who's insert statements are to be
            extracted
        :type table_name: str
        :return: insert statement from the trigger body for given table
        :rtype: str
        """
        # find the position of the insert statements
        # then from there find the first ';' character
        LOGGER.debug("trigger body: %s ...", trigger_body[0:400])
        LOGGER.debug("table name: %s", table_name)
        regex_exp = (
            r"INSERT\s+INTO\s+\w*\.?\w+\s*\([^)]*\)\s*VALUES\s*\([^;]*\);"
        )
        LOGGER.debug("extracting insert statement")
        pattern = re.compile(regex_exp, re.IGNORECASE | re.DOTALL)
        insert_statements = pattern.findall(trigger_body)
        LOGGER.debug("insert statements: %s", len(insert_statements))
        return insert_statements

    def fix_sequences(self) -> None:
        """
        Fix the sequences in the database.

        Queries for sequences that are used in triggers, then finds the max
        value of the sequence column in the table and sets the sequence nextval
        to be higher than the max value.
        """
        LOGGER.debug("fixing sequences")
        trig_seq_list = self.get_triggers_with_sequences()
        LOGGER.debug("sequences found: %s", len(trig_seq_list))
        for trig_seq in trig_seq_list:
            trigger_struct = self.get_trigger_body(
                trig_seq.trigger_name,
                trig_seq.owner,
            )

            inserts = self.extract_inserts(
                trigger_struct.trigger_body,
                trigger_struct.table_name,
            )
            for insert in inserts:
                sequence_column = self.extract_sequence_column(
                    insert,
                    trigger_struct.table_name,
                )
                LOGGER.debug("sequence column %s", sequence_column)
                insert_statement_table = self.extract_table_name(insert)

                current_max_value = self.get_max_value(
                    table=insert_statement_table,
                    column=sequence_column,
                    schema=trig_seq.owner,
                )
                sequence_next_val = self.get_sequence_nextval(
                    sequence_name=trig_seq.sequence_name,
                    sequence_owner=trig_seq.owner,
                )
                LOGGER.debug("max value: %s", current_max_value)
                LOGGER.debug("sequence nextval: %s", sequence_next_val)
                if current_max_value > sequence_next_val:
                    LOGGER.debug("fixing sequence: %s", trig_seq.sequence_name)
                    self.set_sequence_nextval(
                        sequence_name=trig_seq.sequence_name,
                        sequence_owner=trig_seq.owner,
                        new_value=current_max_value + 1,
                    )

    def set_sequence_nextval(
        self,
        sequence_name: str,
        sequence_owner: str,
        new_value: str,
    ) -> None:
        """
        Set the nextval for a sequenc to the supplied 'new_value' value.

        :param sequence_name: name of the sequence
        :type sequence_name: str
        :param sequence_owner: schema owner of the sequence
        :type sequence_owner: str
        :param new_value: the value to set the nextval to
        :type new_value: str
        """
        query = (
            f"ALTER SEQUENCE {sequence_owner}.{sequence_name} restart start "
            f"with {new_value}"
        )
        LOGGER.debug("alter statement: %s", query)
        LOGGER.info(
            "updating the sequence %s to have a nextval of %s",
            sequence_name,
            new_value,
        )
        LOGGER.debug("query: %s", query)
        self.dbcls.get_connection()
        cur = self.dbcls.connection.cursor()
        cur.execute(query)
        self.dbcls.connection.commit()

    def get_sequence_nextval(
        self,
        sequence_name: str,
        sequence_owner: str,
    ) -> int:
        """
        Get the nextval for a sequence.

        :param sequence_name: name of the sequence
        :type sequence_name: str
        :param sequence_owner: owner of the sequence
        :type sequence_owner: str
        :return: the next val for the sequence
        :rtype: int
        """
        query = (
            "SELECT last_number + increment_by FROM all_sequences "
            "WHERE sequence_name = :sequence_name AND sequence_owner "
            " = :sequence_owner"
        )
        LOGGER.debug("query: %s ", query)
        self.dbcls.get_connection()
        cur = self.dbcls.connection.cursor()
        cur.execute(
            query,
            sequence_name=sequence_name,
            sequence_owner=sequence_owner,
        )
        row = cur.fetchone()
        return row[0]

    def get_max_value(self, schema: str, table: str, column: str) -> int:
        """
        Get the largest value in the table colum provided.

        :param schema: schema of the table
        :type schema: str
        :param table: the table name
        :type table: str
        :param column: the name of the column
        :type column: str
        :return: the max value
        :rtype: int
        """
        query = f"SELECT MAX({column}) FROM {schema}.{table}"  # noqa: S608
        self.dbcls.get_connection()
        cur = self.dbcls.connection.cursor()
        cur.execute(query)
        row = cur.fetchone()
        return row[0]

    def extract_table_name(self, insert_statement: str) -> str | None:
        """
        Extract the table name from the insert statement.

        :param insert_statement: a string with an insert statement
        :type insert_statement: str
        :return: the name of the table in the insert statement if one could
            be extracted, otherwise returns None
        :rtype: str | None
        """
        insert_pattern_str = (
            r"INSERT\s+INTO\s+(\w*\.?\w+)\s*\(.*?\)\s*VALUES\s*\(.*?\);"
        )
        insert_pattern = re.compile(
            insert_pattern_str,
            re.IGNORECASE | re.DOTALL,
        )
        match = insert_pattern.search(insert_statement)

        if match:
            table_name = match.group(1)
            LOGGER.debug("table name from insert statement: %s", table_name)
            return table_name
        return None

    def extract_sequence_column(
        self,
        insert_statement: str,
        table_name: str,
    ) -> str | None:
        """
        Extract the sequence column from the insert statement.

        :param insert_statement: the insert statement
        :type insert_statement: str
        :param table_name: table name to extract the sequence column from
        :type table_name: str
        :return: the name of the sequence column if one could be extracted.
        :rtype: str | None
        """
        LOGGER.debug("extract the column for the table %s", table_name)
        insert_pattern_str = (
            r"INSERT\s+INTO\s+\w*\.?\w+\s*\((.*?)\)\s*VALUES\s*\((.*?)\);" ""
        )
        insert_pattern = re.compile(
            insert_pattern_str,
            re.IGNORECASE | re.DOTALL,
        )
        insert_match = insert_pattern.search(insert_statement)
        sequence_column = None
        if insert_match:
            columns_str = insert_match.group(1)
            values_str = insert_match.group(2)

            # Split the columns and values by comma and strip whitespace
            columns = [col.strip() for col in columns_str.split(",")]
            values = [val.strip() for val in values_str.split(",")]
            LOGGER.debug(columns)
            LOGGER.debug(values)
            val_cnt = 0
            for val in values:
                if val.upper().endswith(".NEXTVAL"):
                    LOGGER.debug(val)
                    break
                val_cnt += 1
            sequence_column = columns[val_cnt]
            LOGGER.debug(sequence_column)
        return sequence_column


class DataFrameExtended(pd.DataFrame):
    """
    Functionality to copy BLOB, SDO, and masked data from dataframe.

    :param pd: pandas dataframe
    :type pd: pandas.DataFrame
    """

    def to_sql(
        self,
        table_name: str,
        oradb: OracleDatabase,
        if_exists: str = "append",
        index: bool = False,  # noqa: FBT001, FBT002
        *args,
        **kwargs,
    ) -> None:
        """
        Dump all the data to oracle.

        :param name: The name of the table to write the data to.
        :type name: _type_
        :param con: A link to a python database connection object.
        :type con: oracledb.Connection
        :param if_exists: can be append|replace, determines what to do if the
                          database object already exists, defaults to "append"
        :type if_exists: str, optional
        :param index: Just leave this as is, defaults to False
        :type index: bool, optional
        :param blob_cols: List of columns in the destination table that are
                          defined as BLOB.  The blob columns will be populated
                          using the oracle EMPTY_BLOB() method. defaults to []
        :type blob_cols: list, optional
        :raises AssertionError: _description_
        """
        # make sure there is a connection
        oradb.get_connection()

        if if_exists not in ["replace", "append"]:
            msg = "not if_exists in ['replace', 'append'] is not yet impemented"
            raise AssertionError(
                msg,
            )

        # Truncate database table
        # NOTE: Users may have to perform to_sql in the correct
        # sequence to avoid causing foreign key errors with this step
        if if_exists == "replace":
            with oradb.connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_name}")

        # If index, then we also want the index inserted
        # LOGGER.debug("blobcols: %s", blob_cols)
        # LOGGER.debug("index name: %s", self.index.name)
        # LOGGER.debug("columns: %s", list(self.columns))

        # cols = [self.index.name] * index + list(self.columns)
        # blob_cols = [col.upper() for col in blob_cols]
        # sdo_cols = [col.upper() for col in sdo_cols]
        column_list = oradb.get_column_list(
            table_name,
            with_length_precision_scale=True,
        )
        # LOGGER.debug("columns and types: %s", column_list)
        cols = [column_data_list[0].upper() for column_data_list in column_list]
        insert_placeholders = self.get_value_placeholders(
            column_list=column_list,
            table_name=table_name,
            oradb=oradb,
        )
        LOGGER.debug("insert_placeholders: %s", insert_placeholders)
        cmd = (
            f"INSERT /*+ append */ INTO {table_name} ({', '.join(cols)}) VALUES "  # noqa: S608
            f"({', '.join(insert_placeholders)})"
        )
        LOGGER.debug("insert statement: %s", cmd)

        # add faker values to the data if necesssary

        # table_data = list(self.itertuples(index=index))
        # LOGGER.debug("type(table_data) %s", type(table_data))
        # Replace nan with None for SQL to accept it.

        # table_data = self.to_list()
        # LOGGER.debug("dataframe to tuple...")
        data = [
            tuple(self.convert_types(val) for val in row)
            for row in self.itertuples(index=False, name=None)
        ]

        if len(data) == 0:
            pass
        else:
            # input sizes need to be adjusted to get WKT data which typically
            # exceeds the default size of 4000 for VARCHAR2.
            input_sizes = self.get_input_sizes(column_list=column_list)
            LOGGER.debug("input_sizes: %s", input_sizes)
            with oradb.connection.cursor() as cursor:
                cursor.execute(
                    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                )
                cursor.execute(
                    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                )
                LOGGER.debug("writing to oracle...")
                # LOGGER.debug("first row: %s", data[0])
                # try:

                cursor.setinputsizes(*input_sizes)
                cursor.executemany(cmd, data)
                # except:
                #     oradb.truncate_table(table=table_name)

                #     for row in data:
                #         try:
                #             cursor.execute(cmd, row)
                #         except Exception as e:
                #             LOGGER.debug("bad row: %s", row)
                #             raise e

            LOGGER.debug("data has been entered!")

    def convert_types(self, value: any) -> any:
        """
        Cast data to types that can be sent to Oracle.

        The dataframe gets converted to a list of tuples and that structure is
        then sent to executemany oracle function.  This function is called on
        each value when the dataframe is converted to a list of tuples. It
        ensures that the datatypes that oracle does not accept are cast to ones
        that it will.

        :param value: input value to be converted
        :type value: any
        :return: oracle save cast of the value.
        :rtype: any
        """
        if isinstance(value, numpy.datetime64):
            # Convert np.datetime64 to Python datetime.datetime
            return pd.Timestamp(value).to_pydatetime()
        elif isinstance(value, numpy.integer):
            return int(value)  # Convert int64 to native int
        elif isinstance(value, numpy.floating):
            return float(value)  # Convert float64 to native float
        elif isinstance(value, pd.Timestamp):
            return datetime.datetime.fromtimestamp(value.timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        elif pd.isna(value):
            return None  # Convert NaN to None for Oracle NULL
        return value  # Return as-is for other types (e.g., str)        )

    def get_input_sizes(
        self,
        column_list: list[list[str, str]],
    ) -> list[str]:
        """
        Get cursor input size / type list
        """
        input_sizes = []
        for column_data in column_list:
            column_name, column_type = column_data[0:2]
            if column_type == constants.ORACLE_TYPES.SDO_GEOMETRY:
                LOGGER.debug("found sdo col %s", column_name)
                input_sizes.append(oracledb.CLOB)
            else:
                input_sizes.append(None)
        return input_sizes

    def get_value_placeholders(
        self,
        table_name: str,
        column_list: list[list[str, str]],
        oradb: OracleDatabase,
    ) -> list[str]:
        """
        Get the value placeholders for the insert statement.

        :param oradb: an OracleDatabase object
        :type oradb: OracleDatabase
        :return: a list of the value placeholders for the insert statement
        :rtype: list[str]
        """
        # column_list = oradb.get_column_list(
        #     self.table_name,
        #     with_type=True,
        # )
        col_cnt = 1
        insert_placeholders = []
        for column_data_list in column_list:
            column_name = column_data_list[0]
            column_type = column_data_list[1]
            # column_type = constants.ORACLE_TYPES[column_type]
            LOGGER.debug("column name: %s", column_name)
            LOGGER.debug("column type: %s", column_type)
            col_placeholder = f":{col_cnt}"
            # mask_obj = oradb.data_classification.get_mask_info(
            #     table_name=table_name,
            #     column_name=column_name,
            # )
            if column_type in [
                constants.ORACLE_TYPES.BLOB,
                constants.ORACLE_TYPES.CLOB,
            ]:
                insert_placeholders.append(
                    f"NVL(TO_BLOB({col_placeholder}), EMPTY_BLOB())",
                )
            elif column_type in [constants.ORACLE_TYPES.SDO_GEOMETRY]:
                insert_placeholders.append(
                    f"SDO_UTIL.FROM_WKTGEOMETRY({col_placeholder}, 3005)",
                )
            # elif mask_obj:
            #     faker_method = mask_obj.faker_method
            #     fake_data = self.get_default_fake_data(
            #         column_data_list,
            #         faker_method,
            #     )
            #     # may need a case, only want to insert if not null
            #     insert_placeholders.append(
            #         f"NVL2({col_placeholder}, {fake_data}, NULL)",
            #     )
            # TODO: handle the mask data here once I get this working
            else:
                insert_placeholders.append(col_placeholder)
            col_cnt += 1
        return insert_placeholders

    def to_list(self) -> list[list]:
        """
        Convert and clean the dataframe to a list.

        Iterates through the data, converts bytestrings to hex, converts
        pandas.NaN to None and returns a list for entry to the database.

        :return: a list of lists from the dataframe that should be safe to write
                 to oracle.
        :rtype: list[list]
        """
        table_data = list(self.itertuples(index=False))
        # Replace nan with None for SQL to accept it.
        table_data = [
            [None if pd.isna(value) else value for value in sublist]
            for sublist in table_data
        ]
        is_null = pd.isnull  # Store function reference for efficiency

        for i in range(len(table_data)):  # Iterate over rows
            for j in range(len(table_data[i])):  # Iterate over columns
                if is_null(table_data[i][j]):  # Check for NaN
                    table_data[i][j] = None  # Replace NaN with None
                if isinstance(table_data[i][j], bytes):
                    table_data[i][j] = table_data[i][j].hex()
        return table_data


class Extractor:
    """
    Extraction specific logic / code.
    """

    def __init__(
        self,
        table_name: str,
        db_schema: str,
        oradb: OracleDatabase,
        export_file: pathlib.Path,
        data_cls: DataClassification,
    ):
        self.table_name = table_name
        self.db_schema = db_schema
        self.oradb = oradb
        self.export_file = export_file
        self.data_cls = data_cls

        # make sure there is a connection to the database
        self.oradb.get_sqlalchemy_engine()
        self.oradb.get_connection()

        # example of how to get dbapi connection from the engine
        # connection = engine.connect()
        # dbapi_conn = connection.connection

    def log_mask_config(self):
        LOGGER.debug("const data mask: %s", constants.DATA_TO_MASK)

    def extract(
        self,
        *,
        overwrite: bool = False,  # noqa: ARG002
        chunk_size: int = 25000,
        max_records: int = 0,
    ) -> bool:
        # identify if spatial data
        spatial_columns = self.oradb.get_sdo_geometry_columns(self.table_name)
        if len(spatial_columns) > 1:
            msg = "only one spatial column is supported"
            raise ValueError(msg)
        spatial_col = None
        if spatial_columns:
            spatial_col = spatial_columns[0]
            # spatial will use a smaller chunk size
            chunk_size = 1000
        query = self.generate_extract_sql_query()

        ddb_util = DuckDbUtil(self.export_file, self.table_name)
        # handle the duck db init
        # ddb_con = duckdb.connect(database=export_file)
        chunk_cnt = 0
        first = True
        df_cols = None

        ora_cols = self.oradb.get_column_list(self.table_name, with_type=True)
        ddb_util.create_table(ora_cols)

        for chunk in pd.read_sql(
            query,
            self.oradb.sql_alchemy_engine,
            chunksize=chunk_size,
        ):
            # handle spatial
            if spatial_col:
                # convert spatial from wkt to wkb
                chunk[spatial_col.lower()] = chunk[spatial_col.lower()].apply(
                    shapely.wkt.loads,
                )
                chunk[spatial_col.lower()] = chunk[spatial_col.lower()].apply(
                    shapely.wkb.dumps,
                )
                # df_cols = chunk.columns

            if first:
                # chunk.to_sql(table, ddb_con, if_exists="replace", index=False)
                LOGGER.debug(
                    "creating the table, writing first chunk: %s", chunk_size
                )
                # ddb_util.create_and_write_chunk(chunk)
                ddb_util.insert_chunk(chunk)

                first = False
            else:
                LOGGER.info(
                    "writing chunk to the table (chunk/chunk_size), (%s/%s)",
                    chunk_cnt + 1,
                    (chunk_cnt + 1) * chunk_size,
                )
                ddb_util.insert_chunk(chunk)
            if (max_records) and chunk_cnt * chunk_size > max_records:
                break
            chunk_cnt += 1
        return True

    def generate_extract_sql_query(self) -> str:
        """
        Return a sql query to extract the table with.

        The query addresses the following conditions:
        * blob data is not copied and is replaced with EMPTY_BLOB() as <column>
        * sdo geometry is converted to wkt
        * if columns have been defined as sensitive then they are not extracted
            and are replaced with nulls if nulls and placeholder if not.
            placeholder values are dependent on data type.  For example
            varchar will get '1' nums 1 etc..

        :return: a query that can be used to extract the data in the table.
        :rtype: str
        """
        # mask_obj = db_lib.DataMasking()
        # mask_columns = mask_obj.get_masked_columns(table_name=self.table_name)

        column_list = self.oradb.get_column_list(
            self.table_name, with_type=True
        )
        blob_columns = self.oradb.get_blob_columns(self.table_name)
        sdo_geometry_columns = self.oradb.get_sdo_geometry_columns(
            self.table_name
        )

        select_column_list = []

        # Define a mapping
        for column_name, column_type in column_list:
            mask_obj = self.data_cls.get_mask_info(self.table_name, column_name)
            if column_type in [
                constants.ORACLE_TYPES.BLOB,
                constants.ORACLE_TYPES.CLOB,
            ]:
                select_column_list.append(
                    f"EMPTY_BLOB() AS {column_name}",
                )
            elif column_type in [constants.ORACLE_TYPES.SDO_GEOMETRY]:
                select_column_list.append(
                    f"SDO_UTIL.TO_WKTGEOMETRY({column_name}) AS {column_name}",
                )
            # elif column_name in mask_columns:
            elif mask_obj:
                mask_dummy_val = self.data_cls.get_mask_dummy_val(column_type)
                column_value = f"nvl2({column_name}, {mask_dummy_val}, NULL) as {column_name.upper()}"
                select_column_list.append(column_value)
            else:
                select_column_list.append(column_name)
        query = (
            f"SELECT {', '.join(select_column_list)} from "  # noqa: S608
            f"{self.db_schema.upper()}.{self.table_name.upper()}"
        )
        LOGGER.debug("query: %s", query)
        return query


class Importer:
    def __init__(
        self,
        table_name: str,
        db_schema: str,
        oradb: OracleDatabase,
        import_file: pathlib.Path,
        data_cls: DataClassification,
    ):
        self.table_name = table_name
        self.db_schema = db_schema
        self.oradb = oradb
        self.import_file = import_file
        self.data_cls = data_cls

        self.chunk_size = 10000

        # make sure there is a connection to the database
        self.oradb.get_sqlalchemy_engine()
        self.oradb.get_connection()

        # populated when needed
        self.sdo_cols = []
        self.blob_cols = []
        self.mask_cols = []

        self.__populate_col_types()

    def __populate_col_types(self):
        """
        populates the following parameters:
        * blob_cols
        * sdo_cols
        * mask_cols
        """
        # ensure a connection has been built
        self.oradb.get_connection()
        column_list = self.oradb.get_column_list(
            self.table_name,
            with_type=True,
        )
        for column_name, column_type in column_list:
            mask_obj = self.data_cls.get_mask_info(self.table_name, column_name)
            column_name_for_placeholder = column_name
            if column_type in [
                constants.ORACLE_TYPES.BLOB,
                constants.ORACLE_TYPES.CLOB,
            ]:
                self.blob_cols.append(column_name)
            elif column_type in [constants.ORACLE_TYPES.SDO_GEOMETRY]:
                self.sdo_cols.append(column_name)
            elif mask_obj:
                self.mask_cols.append(column_name)

    def get_import_column_list(self, use_numeric: bool = False) -> list[str]:
        """
        Get the list of columns that are to be imported.

        Addresses type conversions for some data types as well as handling
        the generation of fake data for classified columns.

        :return: a list of columns that can be used in an insert statement.
        :rtype: list[str]
        """
        column_list = self.oradb.get_column_list(
            self.table_name,
            with_type=True,
        )
        # if self.blob_cols is None:
        #     self.blob_cols = self.oradb.get_blob_columns(self.table_name)

        # if self.sdo_cols is None:
        #     self.sdo_cols = self.oradb.get_sdo_geometry_columns(self.table_name)

        select_column_list = []
        blob_cols = []
        # Define a mapping
        col_cnt = 1
        for column_name, column_type in column_list:
            mask_obj = self.data_cls.get_mask_info(self.table_name, column_name)
            column_name_for_placeholder = column_name
            if use_numeric:
                column_name_for_placeholder = str(col_cnt)
            if column_type in [
                constants.ORACLE_TYPES.BLOB,
                constants.ORACLE_TYPES.CLOB,
            ]:
                blob_cols.append(column_name)
                # select_column_list.append(f"TO_BLOB(:{column_name})")
                # NVL(TO_BLOB(:2), EMPTY_BLOB()))
                select_column_list.append(
                    f"NVL(TO_BLOB(:{column_name_for_placeholder}), EMPTY_BLOB())"
                )
            # # SDO_UTIL.FROM_WKBGEOMETRY(:3)
            elif column_type in [constants.ORACLE_TYPES.SDO_GEOMETRY]:
                select_column_list.append(
                    f"SDO_GEOM.SDO_MBR(SDO_UTIL.FROM_WKTGEOMETRY(:{column_name_for_placeholder}, 3005))",
                )
                # f"SDO_UTIL.FROM_WKBGEOMETRY(TO_BLOB(:{column_name}), 3005)",

            # TODO: figure out the masking after get the data sorted out
            # # elif column_name in mask_columns:
            # elif mask_obj:
            #     mask_dummy_val = self.data_cls.get_mask_dummy_val(column_type)
            #     column_value = f"nvl2({column_name}, {mask_dummy_val}, NULL) as {column_name.upper()}"
            #     select_column_list.append(column_value)
            else:
                select_column_list.append(f":{column_name_for_placeholder}")

            col_cnt += 1
        return select_column_list

    def get_default_fake_data(
        self, column_data: list[str | int], faker_method=None
    ) -> str | int:
        """
        Returns fake data based on the data type.

        column_data:
            0 - column name
            1 = column type
            2 = column length
            3 = column precision
            4 = column scale
        """
        column_type = column_data[1]
        # LOGGER.debug("column_type: %s", column_type)
        fake_data = None
        if not faker_method:
            faker_method = constants.ORACLE_TYPES_DEFAULT_FAKER[column_type]
        if column_type in [
            constants.ORACLE_TYPES.VARCHAR2,
        ]:
            try:
                fake_data = faker_method()
                fake_data = fake_data[0 : column_data[2]]
            except:
                LOGGER.debug("faker_method: %s", faker_method)
                LOGGER.debug("column_data: %s", column_data)
                LOGGER.debug("fakedata: %s", fake_data)
                raise

        elif column_type in [
            constants.ORACLE_TYPES.CHAR,
            constants.ORACLE_TYPES.LONG,
        ]:
            fake_data = faker_method()
            # fake_data = f"'{fake_data}'"
        elif column_type in [
            constants.ORACLE_TYPES.NUMBER,
        ]:
            fake_data = faker_method()
        elif column_type in [constants.ORACLE_TYPES.DATE]:
            fake_data = faker_method()
            # fake_data = f"'{fake_data}'"
        else:
            msg = "no faker defined for the data type: %s"
            LOGGER.error(msg, column_data)
            raise ValueError(msg)
        # LOGGER.debug("fake data: %s", fake_data)
        return fake_data

    def import_data(self):
        # make sure there is a connection
        self.oradb.get_sqlalchemy_engine()
        if self.blob_cols is None:
            self.blob_cols = self.oradb.get_blob_columns(self.table_name)

        if self.sdo_cols is None:
            self.sdo_cols = self.oradb.get_sdo_geometry_columns(self.table_name)
        import_column_list = self.get_import_column_list(use_numeric=True)
        column_list = self.oradb.get_column_list(
            table=self.table_name, with_length_precision_scale=True
        )
        dest_tab_rows = self.oradb.get_row_count(self.table_name)
        LOGGER.debug("table: %s has %s rows", self.table_name, dest_tab_rows)
        if not dest_tab_rows:
            duckdb_util = DuckDbUtil(
                duckdb_path=self.import_file,
                table_name=self.table_name,
            )
            LOGGER.debug("getting new chunk from ddb")

            # cursor = self.oradb.connection.cursor()
            # cursor.arraysize = self.oradb.ora_cur_arraysize

            # blob_cols = [col.lower() for col in self.blob_cols]
            ddb_chunk_generator = duckdb_util.get_chunk_generator(
                self.chunk_size,
            )
            chunk_count = 0
            for ddb_chunk in ddb_chunk_generator:
                #
                # ensure all cols in the dataframe are upper case, to match
                # column names in oracle
                ddb_chunk.columns = [col.upper() for col in ddb_chunk.columns]

                # inject in fake data for the columns that are classified
                if self.oradb.data_classification.has_masking(
                    table_name=self.table_name,
                ):
                    LOGGER.debug("adding fake data to %s", self.table_name)
                    for column_data_list in column_list:
                        column_name = column_data_list[0]
                        mask_obj = self.oradb.data_classification.get_mask_info(
                            table_name=self.table_name,
                            column_name=column_name,
                        )
                        if mask_obj:
                            LOGGER.debug("mask_obj: %s", mask_obj)
                            faker_method = mask_obj.faker_method
                            # fake_data = self.get_default_fake_data(
                            #     column_data_list,
                            #     faker_method,
                            # )
                            mask = ddb_chunk[
                                column_name
                            ].notnull()  # or .notna()
                            # LOGGER.debug("mask: %s", mask)
                            ddb_chunk.loc[mask, column_name] = [
                                self.get_default_fake_data(
                                    column_data=column_data_list,
                                    faker_method=faker_method,
                                )
                                for _ in range(mask.sum())
                            ]
                            # self[column_name] = fake_data

                # insert_statement = f"""
                #     INSERT INTO {self.db_schema}.{self.table_name} ({f",".join(column_list)})
                #     VALUES ({", ".join(import_column_list)})
                # """  # noqa: S608
                # LOGGER.debug("insert_statement: %s", insert_statement)
                # blob_cols = self.oradb.get_blob_columns(self.table_name)

                # if self.sdo_cols:
                #     for sdo_col in self.sdo_cols:
                #         LOGGER.debug("trying to address sdo col %s", sdo_col)
                #         LOGGER.debug(
                #             "data frame columns: %s", ddb_chunk.columns
                #         )
                #     input_sizes = []
                #     for col in column_list:
                #         if col in self.sdo_cols:
                #             LOGGER.debug("found sdo col %s", col)
                #             input_sizes.append(oracledb.CLOB)
                #         else:
                #             input_sizes.append(None)

                #     LOGGER.debug("rearrange dataframe for loading...")
                #     data = [
                #         tuple(self.convert_types(val) for val in row)
                #         for row in ddb_chunk.itertuples(index=False, name=None)
                #     ]
                #     LOGGER.debug("input sizes: %s", input_sizes)
                #     cursor.setinputsizes(*input_sizes)

                #     LOGGER.debug("data: %s", data[0])
                #     LOGGER.debug("loading %s rows", len(data))
                #     cursor.executemany(insert_statement, data)
                # else:
                # ddb_chunk.to_sql(
                #     self.table_name,
                #     self.oradb.connection,
                #     schema=self.db_schema,
                #     if_exists="append",
                #     index=False,
                # )
                ddb_chunk.__class__ = DataFrameExtended
                ddb_chunk.to_sql(
                    self.table_name,
                    self.oradb,
                    schema=self.db_schema,
                    if_exists="append",
                    index=False,
                )
                chunk_count += 1
                # if chunk_count > 3:
                #     break

                LOGGER.info("loaded %s rows", chunk_count * self.chunk_size)
                LOGGER.debug("getting new chunk to load...")
            self.oradb.connection.commit()

    def generate_import_sql_query(self) -> str:
        """
        Return a sql query to import the dataframe with.

        The query addresses the following conditions:
        * blob data is not copied and is replaced with EMPTY_BLOB() as <column>
        * sdo geometry is converted to wkt
        * if columns have been defined as sensitive then they are not extracted
            and are replaced with nulls if nulls and placeholder if not.
            placeholder values are dependent on data type.  For example
            varchar will get '1' nums 1 etc..

        :return: a query that can be used to extract the data in the table.
        :rtype: str
        """

        column_list = self.oradb.get_column_list(
            self.table_name, with_type=True
        )
        blob_columns = self.oradb.get_blob_columns(self.table_name)
        sdo_geometry_columns = self.oradb.get_sdo_geometry_columns(
            self.table_name
        )

        select_column_list = []

        # Define a mapping
        for column_name, column_type in column_list:
            mask_obj = self.data_cls.get_mask_info(self.table_name, column_name)
            if column_type in [
                constants.ORACLE_TYPES.BLOB,
                constants.ORACLE_TYPES.CLOB,
            ]:
                select_column_list.append(
                    f"EMPTY_BLOB() AS {column_name}",
                )
            elif column_type in [constants.ORACLE_TYPES.SDO_GEOMETRY]:
                select_column_list.append(
                    f"SDO_UTIL.TO_WKTGEOMETRY({column_name}) AS {column_name}",
                )
            # elif column_name in mask_columns:
            elif mask_obj:
                mask_dummy_val = self.data_cls.get_mask_dummy_val(column_type)
                column_value = f"nvl2({column_name}, {mask_dummy_val}, NULL) as {column_name.upper()}"
                select_column_list.append(column_value)
            else:
                select_column_list.append(column_name)
        query = (
            f"SELECT {', '.join(select_column_list)} from "  # noqa: S608
            f"{self.db_schema.upper()}.{self.table_name.upper()}"
        )
        LOGGER.debug("query: %s", query)
        return query


class DuckDbUtil:
    def __init__(self, duckdb_path: pathlib.Path, table_name: str):
        self.duckdb_path = duckdb_path
        self.table_name = table_name
        self.ddb_con = duckdb.connect(database=self.duckdb_path)
        self.ddb_con.install_extension("spatial")
        self.ddb_con.load_extension("spatial")
        self.ddb_con.execute(f"SET memory_limit='{constants.DUCK_DB_MEM_LIM}'")

        # this gets populated when the table is created
        self.spatial_cols = None

        # once the table is created, this is the column list that gets used to
        # insert data from the dataframe
        self.insert_cols = None

        # if extracting this is where the column configuration gets cached so
        # that it doesn't need to be recreated each time.
        self.query_cols = None

    def close(self):
        self.ddb_con.close()

    def get_spatial_columns(self):
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{self.table_name}'
            AND (lower(data_type) LIKE '%geometry%' OR lower(data_type) LIKE '%geography%');
        """
        self.ddb_con.execute(query)

        # Fetch the results
        results = self.ddb_con.fetchall()

        # Print the results
        columns = []
        for row in results:
            columns.append(row[0])
        return columns

    def get_columns(self):
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{self.table_name}';
        """
        self.ddb_con.execute(query)
        results = self.ddb_con.fetchall()
        cols = [col[0] for col in results]
        LOGGER.debug("ddb table: %s columns: %s", self.table_name, cols)
        return cols

    def export_to_parquet(self, export_file: pathlib.Path):
        # need to test this.
        LOGGER.debug("write to parquet...")

        spatial_cols = self.get_spatial_columns()
        spatial_cols_lower = [col.lower() for col in spatial_cols]
        LOGGER.debug("spatial column for DDB: %s", spatial_cols_lower)
        fix_cols = []
        columns = self.get_columns()
        for col in columns:
            if col.lower() in spatial_cols_lower:
                fix_cols.append(f"ST_GeomFromWKB({col}) AS {col}")
            else:
                fix_cols.append(col)
        ddb_2_parquet_query_str = f"""
            COPY (select {", ".join(fix_cols)} FROM {self.table_name})
            TO '{export_file}' (FORMAT PARQUET);
        """
        LOGGER.debug(ddb_2_parquet_query_str)
        LOGGER.info("writing to parquet file: %s", export_file)
        self.ddb_con.sql(ddb_2_parquet_query_str)
        LOGGER.debug("file has been extracted!")

    def get_ddb_type_from_ora_type(self, ora_type: str) -> str:
        """
        Map the oracle type to the duckdb type.

        :param ora_type: The oracle type"
        """
        LOGGER.debug("ora_type: %s", ora_type)
        # ora_type = constants.ORACLE_TYPES[ora_type_str]
        duck_type = constants.ORACLE_TYPES_TO_DDB_TYPES[ora_type]
        # TODO: for varchar (num chars) and decimals (precison and scale)
        # TODO: hold and see if we can use sqlalchemy to generate the table.
        return duck_type

    def create_table(self, ora_cols):
        create_tab_cols = []
        for ora_col in ora_cols:
            ora_name = ora_col[0]
            ora_type = ora_col[1]

            ddb_type = self.get_ddb_type_from_ora_type(ora_type)
            create_tab_cols.append(f"{ora_name} {ddb_type.name}")

        create_table_ddl = (
            f"CREATE TABLE {self.table_name} ( {', '.join(create_tab_cols)})"
        )
        LOGGER.debug("create_table_ddl: %s", create_table_ddl)
        self.ddb_con.sql(
            create_table_ddl,
        )
        self.spatial_cols = self.get_spatial_columns()
        spatial_cols_lower = [col.lower() for col in self.spatial_cols]

        # configure the column mapping for the insert statement, this is required
        # for spatial so that the wkb data is converted to the correct GEOMETRY
        # type when the data gets inserted.
        if self.insert_cols == None:
            self.insert_cols = []
            columns = self.get_columns()
            for col in columns:
                if col.lower() in spatial_cols_lower:
                    self.insert_cols.append(f"ST_GeomFromWKB({col}) AS {col}")
                else:
                    self.insert_cols.append(col)

    def create_and_write_chunk(self, chunk):
        """
        Create a table using the chunk data.

        Uses the chunk (dataframe) to create the table in duckdb and then writes
        the data in the chunk to duckdb.  This methodology only works for non
        spatial tables.  Even then sometimes the calculated data profile is
        incorrect.  Recommend defining the schema for the table, and then
        create the table, and then load the data.  Seems to be way more reliable
        an approach.

        :param chunk: input dataframe object
        :type chunk: pandas.Dataframe
        """
        self.ddb_con.sql(
            f"CREATE TABLE {self.table_name} AS SELECT * FROM chunk"
        )

    def get_chunk_generator(self, chunk_size: int):
        """
        Gets a chunk of data from the duckdb table.

        Addresses any specific type conversions, example the spatial data type
        gets converted to wkb.

        :param chunk_size: The number of rows to be returned in a
                           dataframe/chunk
        :type chunk_size: int
        """
        extract_query = self.get_ddb_extract_query()
        LOGGER.debug("ddb extractor sql : %s", extract_query)
        chunk = pd.read_sql_query(
            extract_query,
            self.ddb_con,
            chunksize=chunk_size,
        )
        # chunk = self.convert_duckdb_to_oracle(chunk)
        return chunk

    def convert_duckdb_to_oracle(self, df):
        # Convert datetime64[ns] to Python datetime for Oracle compatibility
        for column in df.select_dtypes(include=["datetime64[ns]"]).columns:
            df[column] = pd.to_datetime(df[column])

        # Convert boolean columns to NUMBER(1) for Oracle compatibility
        for column in df.select_dtypes(include=["bool"]).columns:
            df[column] = df[column].apply(lambda x: 1 if x else 0)

        # Ensure all numeric columns are float64 for Oracle compatibility
        for column in df.select_dtypes(include=["float64", "int64"]).columns:
            df[column] = df[column].astype(
                "float64"
            )  # You can adjust precision as needed

        # # Convert object (string) to varchar for Oracle compatibility
        # for column in df.select_dtypes(include=["object"]).columns:
        #     df[column] = df[column].astype("str")

        return df

    def get_ddb_extract_query(self) -> str:
        """
        Generate the query to extract data from the duckdb table.

        :return: the query to extract data from the duckdb table
        :rtype: str
        """
        if self.spatial_cols is None:
            self.spatial_cols = self.get_spatial_columns()
        spatial_cols = [col.lower() for col in self.spatial_cols]
        LOGGER.debug("spatial cols: %s", spatial_cols)
        cols = self.get_columns()
        query_cols = []
        for col in cols:
            LOGGER.debug("col: %s", col)
            if col.lower() in spatial_cols:
                # ST_AsText ST_AsWKB
                query_cols.append(f"ST_AsText({col}) AS {col}")
            else:
                query_cols.append(col)
        self.query_cols = query_cols

        query = f"SELECT {', '.join(self.query_cols)} FROM {self.table_name}"
        LOGGER.debug("query: %s", query)
        return query

    def insert_chunk(
        self,
        chunk,
    ):
        """
        Write the incomming chunk to the duckdb table.

        This method is used to write the data to the duckdb table.  It looks
        at the duckdb schema to determine if any of the columns have a spatial
        data type.  If they do then the insert statement for the spatial data
        gets wrapped in a method that will convert WKB to duckdb st_geometry
        data type.

        :param chunk: incomming dataframe object to write to the duckdb database
        :type chunk: pandas.Dataframe
        :raises e: error that gets raised if the data does not conform to the
            duckdb data profile.
        """
        try:
            spatial_cols_lower = [col.lower() for col in self.spatial_cols]
            LOGGER.debug("spatial column for DDB: %s", spatial_cols_lower)
            # duckdb.execute("INSERT INTO spatial_data VALUES (?, ST_GeomFromWKB(?))", (1, wkb_data))

            chunk_insert = f"""
                INSERT INTO {self.table_name} SELECT {", ".join(self.insert_cols)} from chunk;
            """
            # chunk_insert = f"INSERT INTO {self.table_name} SELECT * FROM chunk"

            self.ddb_con.sql(chunk_insert)
        except duckdb.duckdb.ConversionException as e:
            LOGGER.debug("chunk causing issues: %s ", chunk)
            raise e


class DataClassification:
    """
    Data classification and data masking class.

    Retrieves and merges the data classification information in the data
    classification spreadsheet, and the classifications that are already defined
    in the constants.DATA_TO_MASK list.  Where data is defined in both locations
    the constants.DATA_TO_MASK will take precidence.
    """

    def __init__(self, data_class_ss_path: pathlib.Path, schema: str):
        """
        Construct instance of the DataClassification class.

        :param data_class_ss_path: path to the data classificiaton spreadsheet.
        :type data_class_ss_path: pathlib.Path
        :param schema: the schema that all the objects in the spreadsheet belong
            to.
        :type schema: str
        """
        self.valid_sheets = ["ECAS", "GAS2", "CLIENT", "ISP", "ILCR", "GAS"]
        self.dc_struct: None | dict[str, dict[str, data_types.DataToMask]] = (
            None
        )
        self.schema = schema
        self.ss_path = data_class_ss_path
        self.load()

    def get_mask_info(self, table_name: str, column_name: str) -> str:
        """
        Get the data classification for a given table and column.

        :param table_name: name of the table
        :type table_name: str
        :param column_name: name of the column
        :type column_name: str
        :return: the data classification for the table and column
        :rtype: str
        """
        # double check that the struct has been populated
        if self.dc_struct is None:
            self.load()
        # create short vars
        tab = table_name.upper()
        col = column_name.upper()

        # get_mask_info
        if (tab in self.dc_struct) and col in self.dc_struct[tab]:
            return self.dc_struct[tab][col]
        return None

    # def get_ma

    def get_mask_dummy_val(self, data_type: constants.ORACLE_TYPES) -> str:
        """
        Return the dummy value for the data type.

        Recieves an oracle type like VARCHAR2, NUMBER, etc. and returns a dummy
        value that will go into the cells for this column that will be used
        to indicate that a particular column has been masked.

        :param data_type: the data type of the column to be masked
        :type data_type: constants.ORACLE_TYPES
        :return: the dummy value for the data type
        :rtype: str
        """
        if data_type not in constants.OracleMaskValuesMap:
            raise ValueError(  # noqa: TRY003
                f"data type %s not in OracleMaskValuesMap",
                str(data_type),
            )
        return constants.OracleMaskValuesMap[data_type]

    def has_masking(self, table_name: str) -> bool:
        """
        Check if the table has any masking.

        :param table_name: name of the table
        :type table_name: str
        :return: True if the table has any masking, False otherwise
        :rtype: bool
        """
        if table_name.upper() in self.dc_struct:
            return True
        return False

    def load(self) -> None:
        """
        Merge classifications in constants with classes defined in the ss.
        """
        self.dc_struct = {}
        for class_rec in constants.DATA_TO_MASK:
            tab = class_rec.table_name
            col = class_rec.column_name
            if tab not in self.dc_struct:
                self.dc_struct[tab] = {}
            if col not in self.dc_struct[tab]:
                self.dc_struct[tab][col] = class_rec

        for sheet_name in self.valid_sheets:
            dfs = pd.read_excel(self.ss_path, sheet_name=sheet_name)
            #'TABLE_NAME' / 'COLUMN NAME' / 'INFO SECURITY CLASS'
            subset_df = dfs.loc[
                dfs["INFO SECURITY CLASS"].str.lower() != "public",
                ["TABLE NAME", "COLUMN NAME", "INFO SECURITY CLASS"],
            ]
            for index, row in subset_df.iterrows():
                #
                tab = row["TABLE NAME"].upper()
                col = row["COLUMN NAME"].upper()

                class_rec = data_types.DataToMask(
                    table_name=tab,
                    schema=self.schema,
                    column_name=col,
                    faker_method=None,
                    percent_null=0,
                )
                if tab not in self.dc_struct:
                    self.dc_struct[tab] = {}
                if col not in self.dc_struct[tab]:
                    self.dc_struct[tab][col] = class_rec
