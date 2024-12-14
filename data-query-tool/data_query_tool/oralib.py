import logging
import pathlib

import oracledb
import packaging.version
import sqlalchemy

from . import constants, types

print(f"name: {__name__}")
LOGGER = logging.getLogger(__name__)


class Oracle:
    """
    Communication with oracle database.
    """

    def __init__(self, connection_params: types.ConnectionParameters):
        """
        Create instance of database class.

        :param connection_params: _description_
        :type connection_params: types.ConnectionParameters
        """
        self.connection_params = connection_params
        self.connection = None
        self.connect()

        self.sa_engine = None

    def connect_sa(self):
        if not self.sa_engine:
            connstr = f"oracle+oracledb://{self.connection_params.username}:{self.connection_params.password}@{self.connection_params.host}:{self.connection_params.port}/?service_name={self.connection_params.service_name}"
            self.sa_engine = sqlalchemy.create_engine(
                connstr,
            )

    def connect(self):
        """
        Connect to the database.
        """

        LOGGER.debug(
            "Connecting to Oracle db/host %s / %s ",
            self.connection_params.service_name,
            self.connection_params.host,
        )
        self.connection = oracledb.connect(
            user=self.connection_params.username,
            password=self.connection_params.password,
            host=self.connection_params.host,
            port=self.connection_params.port,
            service_name=self.connection_params.service_name,
        )

    def disconnect(self):
        """
        Close connection to the database.
        """
        self.connection.close()

    def get_related_tables_sa(
        self, table_name: str, schema: str, existing=[]
    ) -> types.DBDependencyMapping:
        self.connect_sa()
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(
            table_name.lower(),
            metadata,
            schema=schema.lower(),
            autoload_with=self.sa_engine,
        )
        related_struct = []
        related_tables = []
        for fk in table.foreign_keys:
            LOGGER.debug(
                f"Table {table_name} depends on {fk.column.table.name} {fk.column.table.primary_key.name}"
            )
            LOGGER.debug(fk)
            related_tables.append((fk.column.table.schema, fk.column.table.name))
        # get rid of dups
        related_tables = set(related_tables)
        for table_schema in related_tables:
            related_table = table_schema[1]
            related_schema = table_schema[0]
            LOGGER.debug("related table: %s", related_table)
            if related_table not in existing:
                related_struct.append(
                    self.get_related_tables_sa(
                        table_name=related_table,
                        schema=related_schema,
                        existing=existing,
                    )
                )
                existing.append(related_table)
        relationship_struct = types.DBDependencyMapping(
            object_name=table_name,
            object_schema=schema,
            object_type=types.ObjectType.TABLE,
            dependency_list=related_struct,
        )
        return relationship_struct

    def get_related_tables(
        self,
        table_name: str,
        schema: str,
    ) -> types.DBDependencyMapping:
        """
        Create recursive data structure that defines relationships to table.

        :param table_name: the name of the table to get related tables for
        :return: the related tables for the table
        """
        query = """
        select table_name, owner
        from all_constraints
        where r_constraint_name in (
            select constraint_name
            from all_constraints
            where TABLE_NAME=:table_name and OWNER=:schema
        ) AND CONSTRAINT_type = 'R'
        """
        cursor = self.connection.cursor()
        LOGGER.debug("table name / schema: %s / %s", table_name, schema)
        # cursor.execute(query, {"table_name": table_name, "schema": schema})
        cursor.execute(query, table_name=table_name, schema=schema)
        cur_results = cursor.fetchall()
        LOGGER.debug("cursor results: %s", cur_results)
        cursor.close()

        related_struct = []
        for curser_record in cur_results:
            LOGGER.debug("curser_record: %s", curser_record)
            related_table = curser_record[0]
            schema = curser_record[1]
            LOGGER.debug("related table: %s", related_table)
            if related_table:
                related_struct.append(
                    self.get_related_tables(table_name=related_table, schema=schema)
                )

        # relationship_struct = {table_name: related_struct}
        relationship_struct = types.DBDependencyMapping(
            object_name=table_name,
            object_schema=schema,
            object_type=types.ObjectType.TABLE,
            dependency_list=related_struct,
        )
        """
            the return structure will end up looking something like this:
            input_table: [{somerelatedtable_1: [{anotherrelation_1: []}
                                                {anotherrelation_2: []}]},
                          {somerelatedtable_2: [{anotherrelation: []},
                                                {anotherrelation: []}]}]

        """
        return relationship_struct

    def create_migrations(
        self,
        relationships: types.DBDependencyMapping,
    ) -> list[str]:
        """
        Create database migrations for the input relationships.

        Iterates over the relationships starting with the outer edges of the
        recursive structure, creating migrations in the order which they
        need to be created in order to maintain referential integrity.

        :param relationships: _description_
        :type relationships: types.ObjectStoreParameters
        """
        migration_code = []
        for relation in relationships.dependency_list:
            if not relation.dependency_list:
                # create the migration here now
                object_ddl = self.get_ddl(
                    object_name=relation.object_name,
                    object_type=relation.object_type,
                    object_schema=relation.object_schema,
                )
                migration_code.append(object_ddl + "\n")
                LOGGER.debug("DDL: TABLE: %s", relation.object_name)
            else:
                # first call itself to ensure the migrations have been
                # created that need to take place first.
                migration_code = migration_code + self.create_migrations(
                    relationships=relation,
                )
                LOGGER.debug("DDL: TABLE: %s", relation.object_name)
                object_ddl = self.get_ddl(
                    object_name=relation.object_name,
                    object_type=relation.object_type,
                    object_schema=relation.object_schema,
                )
                migration_code.append(object_ddl + "\n")

        LOGGER.debug("DDL: TABLE: %s", relationships.object_name)
        object_ddl = self.get_ddl(
            object_name=relationships.object_name,
            object_type=relationships.object_type,
            object_schema=relationships.object_schema,
        )
        migration_code.append(object_ddl + "\n")
        return migration_code

    def get_ddl(
        self, object_name: str, object_schema: str, object_type: types.ObjectType
    ):
        LOGGER.debug("object name: %s", object_name)
        plsql_env_config = (
            "begin "
            "    dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'SQLTERMINATOR', true); "
            "    dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'PRETTY', true); "
            "    dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', false); "
            "    dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'STORAGE', false); "
            "    dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'TABLESPACE', false); "
            "end; "
        )
        query = (
            "SELECT "
            "dbms_metadata.get_ddl(:object_type, :object_name, :object_schema) "
            "FROM dual"
        )
        cursor = self.connection.cursor()
        LOGGER.debug("running plsql env config...")
        cursor.execute(plsql_env_config)
        LOGGER.debug("runnig query to get ddl.")
        cursor.execute(
            query,
            object_name=object_name.upper(),
            object_type=object_type.name.upper(),
            object_schema=object_schema.upper(),
        )
        ddl_result_cell = cursor.fetchone()
        ddl_str = ddl_result_cell[0].read()
        LOGGER.debug("ddl for %s is %s", object_name, ddl_str)
        cursor.close()
        return ddl_str
