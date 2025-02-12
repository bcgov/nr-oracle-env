"""
Abstract base class for database operations.
"""

from __future__ import annotations

import logging
import os
import pathlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import constants
import pandas as pd
import psycopg2
import psycopg2.sql
import sqlalchemy
from env_config import ConnectionParameters
from oracledb.exceptions import DatabaseError as OracleDatabaseError

if TYPE_CHECKING:
    import oradb_lib

LOGGER = logging.getLogger(__name__)


@dataclass
class TableConstraints:
    """
    Data class for storing constraints.

    Model / types for storing database constraints when queried from the
    database.
    """

    constraint_name: str
    table_name: str
    column_names: list[str]
    r_constraint_name: str
    referenced_table: str
    referenced_columns: list[str]


@dataclass
class SequenceTableColumns:
    """
    A class to represent the sequence / table relationship.

    Attributes:
        sequence_name (str): The name of the sequence.
        table_name (str): The name of the table.
        table_schema (str): The schema of the table.
        column_name (str): The name of the column.
    """  # noqa: D413

    sequence_name: str
    table_name: str
    table_schema: str
    column_name: str


class DB(ABC):
    """
    Database abstract base class.

    Partially implemented abstract class for database operations to support
    data import and export operations.

    :param ABC: inheriting the ABC (abstract base class module)
    :type ABC: abc.ABC
    :raises NotImplementedError: abstract methods raise this error if they have
        not been implemented
    :raises sqlalchemy.exc.IntegrityError: raised when the script is unable to
        load the data into the database
    """

    def __init__(self, connection_params: ConnectionParameters) -> None:
        """
        Create a DB Class object.

        Constructor method for objects of this class.

        :param connection_params: A connection parameter object that contains
            the credentials used to connect to the database
        :type connection_params: ConnectionParameters
        :raises NotImplementedError: raised if the inheriting class has not
            defined this method.
        """
        if connection_params is None:
            connection_params = ConnectionParameters
        self.username = connection_params.username
        self.password = connection_params.password
        self.host = connection_params.host
        self.port = connection_params.port
        self.service_name = connection_params.service_name
        self.schema_2_sync = connection_params.schema_to_sync

        # if the parameters are not supplied attempt to get them from the
        # environment
        if self.username is None:
            self.username = os.getenv("ORACLE_USER")
        if self.password is None:
            self.password = os.getenv("ORACLE_PASSWORD")
        if self.host is None:
            self.host = os.getenv("ORACLE_HOST")
        if self.port is None:
            self.port = os.getenv("ORACLE_PORT")
        if self.service_name is None:
            self.service_name = os.getenv("ORACLE_SERVICE")

        self.connection = None
        self.sql_alchemy_engine = None
        self.db_type = None
        self.populate_db_type()

        # methods that implement retries how many times to allow the errors to
        # be caught and retried
        self.max_retries = 10

    @abstractmethod
    def get_connection(self) -> None:
        """
        Connect to the database.

        Implement this method to populate the self.connection object property

        :raises NotImplementedError: _description_
        """
        raise NotImplementedError

    @abstractmethod
    def populate_db_type(self) -> None:
        """
        Populate the database type.

        Implement this method to populate the self.db_type object property

        :raises NotImplementedError: _description_
        """
        raise NotImplementedError

    @abstractmethod
    def get_sqlalchemy_engine(self) -> None:
        """
        Create a SQLAlchemy engine object.

        :raises NotImplementedError: _description_
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def truncate_table(self, table: str, *, cascade: bool = False) -> None:
        """
        Delete all the data from the table.

        Recieves a table name as an arguement, method implements logic to
        delete all the data from said table.

        :param table: the table to delete the data from
        """
        raise NotImplementedError

    @abstractmethod
    def get_fk_constraints(self) -> list[oradb_lib.TableConstraints]:
        """
        Return the foreign key constraints for the schema.

        Queries the schema for all the foreign key constraints and returns a
        list of TableConstraints objects.

        :return: a list of TableConstraints objects that are used to store the
            results of the foreign key constraint query
        :rtype: list[TableConstraints]


        """
        raise NotImplementedError

    @abstractmethod
    def get_triggers(
        self,
    ) -> list[
        str
    ]:  # TODO: return here and confirm the type that is returned is str
        """
        Return the triggers for the schema.

        Queries the schema for all the triggers and returns a list of trigger
        names.

        :return: a list of trigger names
        :rtype: list[str]
        """
        raise NotImplementedError

    @abstractmethod
    def disable_fk_constraints(
        self,
        constraint_list: list[TableConstraints],
    ) -> None:
        """
        Disable foreign key constraints.

        Recieves a list of foreign key constraints that are to be disabled.

        :param constraint_list: list of strings describing foreign key
            constraints
        :type constraint_list: list[TableConstraints]
        """
        raise NotImplementedError

    @abstractmethod
    def disable_trigs(
        self,
        trigger_list: list[str],
    ) -> None:
        """
        Disable triggers.

        Recieves a list of triggers that are to be disabled.

        :param trigger_list: list of strings describing triggers
        :type trigger_list: list[str]
        """
        raise NotImplementedError

    @abstractmethod
    def fix_sequences(
        self,
    ) -> None:
        """
        Resolve database sequences to that they are valid.

        Gets a list of sequences in the database, iterates over them to
        determine the tables/columns that they are associated with, and then
        queries the next value for those sequences and compares with the current
        maximum values in the tables, then updates the sequence so that the
        sequences next value is greater than the current maximum for the table
        it populates.

        :param trigger_list: list of strings describing triggers
        :type trigger_list: list[str]
        """
        raise NotImplementedError

    def enable_trigs(
        self,
        trigger_list: list[str],
    ) -> None:
        """
        Enable triggers.

        Recieves a list of triggers that are to be enabled.

        :param trigger_list: list of strings describing triggers
        :type trigger_list: list[str]
        """
        raise NotImplementedError

    @abstractmethod
    def enable_constraints(
        self,
        constraint_list: list[TableConstraints],
    ) -> None:
        """
        Enable all foreign key constraints.

        Iterates through the list of constraints and enables them.

        :param constraint_list: list of constraints that are to be enabled
        :type constraint_list: list[TableConstraints]
        """
        raise NotImplementedError

    def extract_data(
        self,
        table: str,
        export_file: pathlib.Path,
        *,
        overwrite: bool = False,
    ) -> bool:
        """
        Extract a table from the database to a parquet file.

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

        # check that the directory for export file exists
        export_file.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.debug("export file: %s", export_file)
        if not export_file.exists() or overwrite:
            table_obj = self.get_table_object(table)
            select_obj = sqlalchemy.select(table_obj)

            if export_file.exists():
                # delete the local file if it exists
                export_file.unlink()
                # delete the local file if it exists
            LOGGER.debug("data_query_sql: %s", select_obj)
            LOGGER.debug("reading the %s", table)
            self.get_sqlalchemy_engine()
            df_orders = pd.read_sql(select_obj, self.sql_alchemy_engine)

            LOGGER.debug("writing to parquet file: %s ", export_file)
            df_orders.to_parquet(
                export_file,
                engine="pyarrow",
            )
            file_created = True
        else:
            LOGGER.info("file exists: %s, not re-exporting", export_file)
        return file_created

    def get_table_object(self, table_name: str) -> sqlalchemy.Table:
        """
        Get a SQLAlchemy Table object for an existing database table.

        :param table_name: the name of the table to get a SQLAlchemy Table
            object for
        :type table_name: str
        :return: returns a SQLAlchemy Table object for the table
        :rtype: sqlalchemy.Table
        """
        self.get_sqlalchemy_engine()
        metadata = sqlalchemy.MetaData()
        LOGGER.debug("schema2Sync is: %s", self.schema_2_sync)
        return sqlalchemy.Table(
            table_name.lower(),
            metadata,
            autoload_with=self.sql_alchemy_engine,
            schema=self.schema_2_sync.lower(),
        )

    def get_tmp_file(self) -> pathlib.Path:
        """
        Return a temporary file name.

        :return: the temporary file name
        :rtype: str
        """
        tmp_file_name = pathlib.Path("tmp_file")
        if pathlib.Path.exists(tmp_file_name):
            pathlib.Path(tmp_file_name).unlink()
        return tmp_file_name

    def load_data(
        self,
        table: str,
        import_file: pathlib.Path,
        *,
        purge: bool = False,
    ) -> None:
        """
        Load the data from the file into the table.

        This is the default method that expects to load the data from a parquet
        file.

        :param table: the table to load the data into
        :type table: str
        :param import_file: the file to read the data from
        :type import_file: str
        :param purge: if True, delete the data from the table before loading.
        :type purge: bool
        """
        # debugging to view the data before it gets loaded
        LOGGER.debug("input parquet file to load: %s", import_file)
        LOGGER.debug("reading parquet file, %s", import_file)
        pandas_df = pd.read_parquet(import_file)
        LOGGER.debug("done loading dataframe.")

        LOGGER.debug("table: %s", table)

        self.get_sqlalchemy_engine()
        if purge:
            self.truncate_table(table.lower())
        LOGGER.debug("loading data to table: %s", table)
        if self.db_type == constants.DBType.SPAR:
            method = "multi"
        elif self.db_type == constants.DBType.ORA:
            method = None
        pandas_df.to_sql(
            table.lower(),
            self.sql_alchemy_engine,
            schema=self.schema_2_sync,
            if_exists="append",
            index=False,
            method=method,
        )

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
            import_file = constants.get_default_export_file_path(
                table,
                env_str,
                self.db_type,
            )

            LOGGER.info("Importing table %s %s", spaces, table)
            try:
                self.load_data(table, import_file, purge=purge)
            except (
                sqlalchemy.exc.IntegrityError,
                OracleDatabaseError,
            ) as e:
                LOGGER.exception(
                    "%s loading table %s",
                    e.__class__.__qualname__,
                    table,
                )
                LOGGER.info("Adding %s to failed tables", table)
                failed_tables.append(table)
                LOGGER.info("truncating failed load table: %s", table)
                self.truncate_table(table.lower())

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

    def get_record_count(self, table: str) -> int:
        """
        Return the record count for the table.

        :param table: name of the table to get the record count for
        :type table: str
        :return: integer representing the number of records/rows in the table
        :rtype: int
        """
        query = "SELECT COUNT(*) FROM {schema}.{table}"

        LOGGER.debug("record count query: %s", query)
        self.get_connection()
        cursor = self.connection.cursor()
        if self.db_type == constants.DBType.SPAR:
            query = psycopg2.sql.SQL(
                "SELECT COUNT(*) FROM {schema}.{table}",
            ).format(
                table=psycopg2.sql.Identifier(table.lower()),
                schema=psycopg2.sql.Identifier(self.schema_2_sync),
            )
        elif self.db_type == constants.DBType.ORA:
            query = query.format(
                schema=self.schema_2_sync,
                table=table.lower(),
            )
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        LOGGER.debug("record count for %s is %s", table, count)
        return count

    def purge_data(
        self,
        table_list: list[str],
        retries: int = 1,
        max_retries: int = 25,
        *,
        cascade: bool = False,
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
                    self.truncate_table(table=table, cascade=True)
                    LOGGER.info("purged table %s", table)
                except (
                    sqlalchemy.exc.IntegrityError,
                    OracleDatabaseError,
                    psycopg2.errors.FeatureNotSupported,
                    psycopg2.errors.InFailedSqlTransaction,
                ) as e:
                    # might need DatabaseError,
                    LOGGER.warning(
                        "error on table %s raised when purging",
                        table,
                    )
                    # if retries > 3:
                    #     raise
                    LOGGER.exception(
                        "%s purging table %s",
                        e.__class__.__qualname__,
                        table,
                    )
                    failed_tables.append(table)
        if failed_tables:
            if retries < max_retries:
                retries += 1
                LOGGER.debug("retrying failed tables: %s", failed_tables)
                LOGGER.debug("retries: %s", retries)
                self.purge_data(
                    table_list=failed_tables,
                    retries=retries,
                    cascade=True,
                )
            else:
                msg = "Max retries reached for table %s"
                LOGGER.error(msg, table)
                msg = msg % table
                LOGGER.debug("error message: %s", msg)
                LOGGER.debug("failed tables %s", failed_tables)
                # statement=None, params=None, orig=e)
                raise sqlalchemy.exc.DBAPIError(
                    statement=msg,
                    params=None,
                    orig=None,
                )
