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

import functools
import logging
import logging.config
import re
from typing import TYPE_CHECKING

import constants
import db_lib
import oracledb
import pandas as pd
import sqlalchemy
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

    def get_connection(self) -> None:
        """
        Create a connection to the oracle database.

        Creates a connection to the database using class variables that are
        populated by the object constructor.
        """
        if self.connection is None:
            LOGGER.info("connecting the oracle database: %s", self.service_name)
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
            dsn = f"oracle+oracledb://{self.username}:{self.password}@{self.host}:{self.port}/?service_name={self.service_name}"
            self.sql_alchemy_engine = sqlalchemy.create_engine(
                dsn,
                arraysize=1000,
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

    def load_data(
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
            import_file = constants.get_parquet_file_path(
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


class FixOracleSequences:

    def __init__(self, dbcls: OracleDatabase):
        self.dbcls = dbcls

    def get_triggers_with_sequences(self):
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
            owner = row[0]
            trigger_name = row[1]
            sequence_name = row[2]
            trig_seq_dict = {}
            trig_seq_dict["owner"] = row[0]
            trig_seq_dict["trigger_name"] = row[1]
            trig_seq_dict["sequence_name"] = row[2]
            trig_seq_list.append(trig_seq_dict)
        return trig_seq_list

    def get_trigger_body(
        self,
        trigger_name,
        trigger_owner,
    ):
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
            query, trigger_name=trigger_name, trigger_owner=trigger_owner
        )
        trigger_struct = cursor.fetchone()
        cursor.close()

        trigger_body = trigger_struct[2]
        table_name = trigger_struct[1]
        ret_dict = {}
        ret_dict["trigger_body"] = trigger_body
        ret_dict["table_name"] = table_name
        return ret_dict

    def extract_inserts(self, trigger_body, table_name):
        # find the position of the insert statements
        # then from there find the first ';' character
        # LOGGER.debug("trigger body: %s", trigger_body)
        # regex_exp = (
        #     rf"INSERT\s+INTO\s+\w*\.*{table_name}\s*\(.*?\)\s*VALUES\s*\(.*?\);"
        # )
        LOGGER.debug("trigger body: %s ...", trigger_body[0:400])
        LOGGER.debug("table name: %s", table_name)
        regex_exp = (
            rf"INSERT\s+INTO\s+\w*\.?\w+\s*\([^)]*\)\s*VALUES\s*\([^;]*\);"
        )
        LOGGER.debug("extracting insert statement")
        pattern = re.compile(regex_exp, re.IGNORECASE | re.DOTALL)
        insert_statements = pattern.findall(trigger_body)
        LOGGER.debug("insert statements: %s", len(insert_statements))
        # for match in insert_statements:
        #     LOGGER.debug("match is %s", match)

        return insert_statements

    def fix_sequences(self):
        LOGGER.debug("fixing sequences")
        trig_seq_list = self.get_triggers_with_sequences()
        LOGGER.debug("sequences found: %s", len(trig_seq_list))
        for trig_seq in trig_seq_list:
            trigger_struct = self.get_trigger_body(
                trig_seq["trigger_name"], trig_seq["owner"]
            )

            inserts = self.extract_inserts(
                trigger_struct["trigger_body"], trigger_struct["table_name"]
            )
            for insert in inserts:
                sequence_column = self.extract_sequence_column(
                    insert, trigger_struct["table_name"]
                )
                LOGGER.debug("sequence column %s", sequence_column)
                insert_statement_table = self.extract_table_name(insert)

                current_max_value = self.get_max_value(
                    table=insert_statement_table,
                    column=sequence_column,
                    schema=trig_seq["owner"],
                )
                sequence_next_val = self.get_sequence_nextval(
                    sequence_name=trig_seq["sequence_name"],
                    sequence_owner=trig_seq["owner"],
                )
                LOGGER.debug("max value: %s", current_max_value)
                LOGGER.debug("sequence nextval: %s", sequence_next_val)
                if current_max_value > sequence_next_val:
                    LOGGER.debug(
                        "fixing sequence: %s", trig_seq["sequence_name"]
                    )
                    self.set_sequence_nextval(
                        sequence_name=trig_seq["sequence_name"],
                        sequence_owner=trig_seq["owner"],
                        new_value=current_max_value + 1,
                    )

    def set_sequence_nextval(self, sequence_name, sequence_owner, new_value):
        query = f"ALTER SEQUENCE {sequence_owner}.{sequence_name} restart start with {new_value}"
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

    def get_sequence_nextval(self, sequence_name, sequence_owner):
        query = "SELECT last_number + increment_by FROM all_sequences WHERE sequence_name = :sequence_name AND sequence_owner = :sequence_owner"
        LOGGER.debug("query: %s ", query)
        self.dbcls.get_connection()
        cur = self.dbcls.connection.cursor()
        cur.execute(
            query, sequence_name=sequence_name, sequence_owner=sequence_owner
        )
        row = cur.fetchone()
        return row[0]

    def get_max_value(self, schema, table, column):
        query = f"SELECT MAX({column}) FROM {schema}.{table}"
        self.dbcls.get_connection()
        cur = self.dbcls.connection.cursor()
        cur.execute(query)
        row = cur.fetchone()
        return row[0]

    def extract_table_name(self, insert_statement):
        insert_pattern_str = (
            r"INSERT\s+INTO\s+(\w*\.?\w+)\s*\(.*?\)\s*VALUES\s*\(.*?\);"
        )
        insert_pattern = re.compile(
            insert_pattern_str, re.IGNORECASE | re.DOTALL
        )
        match = insert_pattern.search(insert_statement)

        if match:
            table_name = match.group(1)
            return table_name
        else:
            return None

    def extract_sequence_column(self, insert_statement, table_name):
        insert_pattern_str = (
            rf"INSERT\s+INTO\s+\w*\.?\w+\s*\((.*?)\)\s*VALUES\s*\((.*?)\);"
        )
        insert_pattern = re.compile(
            insert_pattern_str, re.IGNORECASE | re.DOTALL
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
