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

import json
import logging
import logging.config
import re
from typing import TYPE_CHECKING

import app_paths
import constants
import db_lib
import env_config
import geopandas as gpd
import numpy
import oracledb
import pandas as pd
import pyarrow
import pyarrow.parquet
import shapely.wkt
import sqlalchemy
from env_config import ConnectionParameters
from oracledb.exceptions import DatabaseError

if TYPE_CHECKING:
    import pathlib

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
        *,
        purge: bool = False,
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
        LOGGER.debug("spatial_column is: %s", spatial_column)
        LOGGER.debug("spatial_column wkt is: %s", spatial_column_wkt)

        gdf = gpd.read_parquet(import_file)

        gdf[spatial_column_wkt] = gdf[spatial_column].apply(
            lambda x: shapely.wkt.dumps(x) if x else None,
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
                value_param_list.append(f"SDO_GEOMETRY(:{column}, 3005)")
            else:
                value_param_list.append(f":{column}")
        column_value_str = ", ".join(value_param_list)
        insert_stmt = f"""
            INSERT INTO {self.schema_2_sync}.{table} ({columns_string})
            VALUES ({column_value_str})
        """
        LOGGER.debug("statement: %s", insert_stmt)
        for index, row in gdf.iterrows():
            data_dict = {}
            LOGGER.debug("row %s", row)
            for column in columns:
                if column.lower() == spatial_column.lower():
                    log_msg = (
                        "dealing with spatial column: %s "
                        "gdf column: %s data: %s"
                    )
                    LOGGER.debug(
                        log_msg,
                        column,
                        spatial_column_wkt,
                        row[spatial_column_wkt],
                    )
                    data_dict[column] = row[spatial_column_wkt]
                else:
                    LOGGER.debug(
                        "row value:%s - %s",
                        column,
                        row[column.lower()],
                    )
                    data_dict[column] = row[column.lower()]
            LOGGER.debug("write record: %s", index)
            LOGGER.debug("data_dict: %s", data_dict)
            cursor.execute(insert_stmt, data_dict)
        cursor.close()

    def load_data(
        self,
        table: str,
        import_file: pathlib.Path,
        *,
        purge: bool = False,
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
        :param purge: if True, delete the data from the table before loading.
        :type purge: bool
        """
        self.get_connection()

        if self.is_geoparquet(import_file):
            LOGGER.info("geoparquet table: %s", table)
            LOGGER.info("forking to use the SDO_GEOMETRY loader")
            self.load_data_geoparquet(
                table=table,
                import_file=import_file,
                purge=purge,
            )
        else:
            super().load_data(
                table,
                import_file,
                purge=purge,
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
        purge: bool = False,
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
        cons_list = self.get_fk_constraints()
        self.disable_fk_constraints(cons_list)

        trigs_list = self.get_triggers()
        LOGGER.debug("trigs_list: %s", trigs_list)
        self.disable_trigs(trigs_list)

        failed_tables = []
        LOGGER.debug("table list: %s", table_list)
        LOGGER.debug("retries: %s", retries)
        for table in table_list:
            spaces = " " * retries * 2
            import_file = self.app_paths.get_parquet_file_path(
                table,
                env_str,
                self.db_type,
            )
            LOGGER.info("Importing table %s %s", spaces, table)
            try:
                self.load_data(table, import_file, purge=purge)
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
            if retries < self.max_retries:
                LOGGER.info("Retrying failed tables")
                retries += 1
                self.load_data_retry(
                    table_list=failed_tables,
                    data_dir=data_dir,
                    env_str=env_str,
                    retries=retries,
                    purge=purge,
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
        self.get_connection()
        cursor = self.connection.cursor()
        cursor.execute(query, schema=self.schema_2_sync)
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

        for cons in constraint_list:
            LOGGER.info("disabling constraint %s", cons.constraint_name)
            query = (
                f"ALTER TABLE {self.schema_2_sync}.{cons.table_name} "
                f"DISABLE CONSTRAINT {cons.constraint_name}"
            )

            cursor.execute(query)
        cursor.close()

    def enable_constraints(
        self,
        constraint_list: list[db_lib.TableConstraints],
    ) -> None:
        """
        Enable all foreign key constraints.

        Iterates through the list of constraints and enables them.

        :param constraint_list: list of constraints that are to be enabled
        :type constraint_list: list[TableConstraints]
        """
        self.get_connection()
        cursor = self.connection.cursor()

        for cons in constraint_list:
            LOGGER.info("enabling constraint %s", cons.constraint_name)
            query = (
                f"ALTER TABLE {self.schema_2_sync}.{cons.table_name} "
                f"ENABLE CONSTRAINT {cons.constraint_name}"
            )
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

    def get_column_list(self, table: str) -> list[str]:
        """
        Get the list of columns for the table.

        :param table: the table to get the columns for
        :type table: str
        :return: a list of the columns for the table
        :rtype: list[str]
        """
        self.get_connection()
        cursor = self.connection.cursor()
        query = """
        SELECT
            column_name
        FROM
            all_tab_cols
        WHERE
            table_name = :table_name AND
            owner = :schema_name AND
            user_generated = 'YES'
        ORDER BY
            SEGMENT_COLUMN_ID
        """
        LOGGER.debug("query: %s", query)
        LOGGER.debug("table: %s", table.upper())
        LOGGER.debug("schema: %s", self.schema_2_sync.upper())
        cursor.execute(
            query,
            table_name=table.upper(),
            schema_name=self.schema_2_sync.upper(),
        )
        columns = [row[0] for row in cursor]
        LOGGER.debug("columns: %s", columns)
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
            f"SELECT {', '.join(column_with_wkb_func)} from "
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
        return temp_parquet_file

    def extract_data_sdogeometry(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,
    ) -> bool:
        """
        Extract data from a table that contains SDO_GEOMETRY columns.

        :param table: table who's data should be extracted
        :type table: str
        :param export_file: the file that the data will be written to
        :type export_file: pathlib.Path
        :param overwrite: if the file already exists what to do
        :type overwrite: bool, optional
        :return: was the file successfully created
        :rtype: bool
        """
        column_list = self.get_column_list(table)
        spatial_columns = self.get_sdo_geometry_columns(table)
        if len(spatial_columns) > 1:
            msg = "only one spatial column is supported"
            raise ValueError(msg)
        spatial_col = spatial_columns[0]

        LOGGER.debug("column list: %s", column_list)
        query = self.generate_sdo_query(table)
        LOGGER.debug("spatial query: %s", query)
        self.get_sqlalchemy_engine()

        writer = None

        chunk_size = 25000  # 25000  # number of rows to include in a chunk
        itercnt = 1
        for chunk in pd.read_sql(
            query,
            self.sql_alchemy_engine,
            chunksize=chunk_size,
        ):
            if itercnt == 1:
                # after first chunk is read, convert it to a pyarrow table and
                # use that as the schema for the stream writer.
                LOGGER.debug(
                    "first chunk has been read, rows: %s", len(chunk_size)
                )
                table = self.df_to_gdf(chunk, spatial_col)
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
            table = self.df_to_gdf(chunk, spatial_col)
            LOGGER.debug("    writing chunk to parquet file...")
            writer.write_table(table)
            itercnt += 1
        writer.close()
        return True

    def df_to_gdf(self, df: pd.DataFrame, spatial_col: str) -> gpd.GeoDataFrame:
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
        df[spatial_col.lower()] = df[spatial_col.lower()].apply(
            shapely.wkt.loads,
        )

        gdf = gpd.GeoDataFrame(
            df,
            geometry=spatial_col.lower(),
            crs="EPSG:3005",
        )

        gdf["geometry_wkb"] = gdf[spatial_col.lower()].apply(
            lambda geom: geom.wkb,
        )

        # Drop the original geometry column
        gdf = gdf.drop(spatial_col.lower(), axis=1)

        # Rename the WKB column to 'geometry'
        gdf = gdf.rename(columns={"geometry_wkb": spatial_col.lower()})

        # Convert the GeoDataFrame to a PyArrow Table
        table = pyarrow.Table.from_pandas(gdf)
        return table

    def extract_data(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,
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
            file_created = self.extract_data_sdogeometry(
                table=table,
                export_file=export_file,
                overwrite=overwrite,
            )
        else:
            try:
                file_created = super().extract_data(
                    table,
                    export_file,
                    overwrite=overwrite,
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
                    raise e
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
        is_parquet = False

        # Check for GeoParquet-specific metadata
        if b"geo" in metadata.metadata:
            LOGGER.debug("input parquet is geo! %s", parquet_file_path)
            is_parquet = True
        return is_parquet

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
        query = f"SELECT MAX({column}) FROM {schema}.{table}"
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
