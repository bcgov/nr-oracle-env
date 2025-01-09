"""
Utility functions for interacting with Oracle databases.
"""

import logging

import oracledb
import sqlalchemy

from . import types

LOGGER = logging.getLogger(__name__)


class Oracle:
    """
    Communication with oracle database.
    """

    def __init__(self, connection_params: types.ConnectionParameters) -> None:
        """
        Create instance of database class.

        :param connection_params: _description_
        :type connection_params: types.ConnectionParameters
        """
        self.connection_params = connection_params
        self.connection = None
        self.connect()

        self.sa_engine = None

        # used to keep track of what tables have already been added to a
        # migration file.
        self.exported_tables = ExportTables()

    def connect_sa(self) -> None:
        """
        Populate the sqlalchemy connection engine.

        Populates the property used for the sqlalchemy engine if it has not
        already been populated.
        """
        if not self.sa_engine:
            connstr = f"oracle+oracledb://{self.connection_params.username}:{self.connection_params.password}@{self.connection_params.host}:{self.connection_params.port}/?service_name={self.connection_params.service_name}"
            self.sa_engine = sqlalchemy.create_engine(
                connstr,
            )

    def connect(self) -> None:
        """
        Connect to the database.

        Using the parameters provided to the constructor, creates a connection
        object to the database, and stored it in the object property
        `connection`
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

    def disconnect(self) -> None:
        """
        Close connection to the database.
        """
        self.connection.close()
        self.connection = None

    def get_related_tables_sa(
        self,
        table_name: str,
        schema: str,
        existing: list[tuple[str, str]] | None = None,
    ) -> types.DBDependencyMapping:
        """
        Return a hierarchical data structure with table dependencies.

        Queries the database metadata to determine what all the table foreign
        key relationships are, and then queries related tables for their
        foreign key relations.

        Example structure:
        .. code-block:: python

        DBDependencyMapping(
            object_type=<ObjectType.TABLE: 1>,
            object_name='SEEDLOT',
            object_schema='THE',
            dependency_list=[
                DBDependencyMapping(
                    object_type=<ObjectType.TABLE: 1>,
                    object_name='superior_provenance',
                    object_schema='the',
                    dependency_list=[
                        DBDependencyMapping(object_type=<ObjectType.TABLE: 1>,
                            object_name='genetic_worth_code',
                            object_schema='the',
                            dependency_list=[   ...


        :param table_name: _description_
        :type table_name: str
        :param schema: _description_
        :type schema: str
        :param existing: _description_, defaults to []
        :type existing: list, optional
        :return: _description_
        :rtype: types.DBDependencyMapping
        """
        if not existing:
            existing = []
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
                "Table %s depends on %s %s",
                table_name,
                fk.column.table.name,
                fk.column.table.primary_key.name,
            )
            related_tables.append((fk.column.table.schema, fk.column.table.name))
        # get rid of dups
        related_tables = set(related_tables)
        for table_schema in related_tables:
            related_table = table_schema[1]
            related_schema = table_schema[0]
            LOGGER.debug("related table: %s", related_table)
            if (related_schema, related_table) not in existing:
                related_struct.append(
                    self.get_related_tables_sa(
                        table_name=related_table,
                        schema=related_schema,
                        existing=existing,
                    ),
                )
                existing.append((related_schema, related_table))
        return types.DBDependencyMapping(
            object_name=table_name,
            object_schema=schema,
            object_type=types.ObjectType.TABLE,
            dependency_list=related_struct,
        )

    def get_related_tables(
        self,
        table_name: str,
        schema: str,
    ) -> types.DBDependencyMapping:
        """
        Create recursive data structure that defines relationships to table.

        deprecated: use get_related_tables_sa instead.  Leaving this code in
                    here for now, as eventually may want to pivot back to this
                    approach where the sql to retrieve the relationships is
                    explicitly defined.  Will provide more control.

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
                    self.get_related_tables(table_name=related_table, schema=schema),
                )

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
            # no dependencies and not already exported
            if not relation.dependency_list and not self.exported_tables.exists(
                table_name=relation.object_name, schema=relation.object_schema
            ):
                self.exported_tables.add_table(
                    table_name=relation.object_name, schema=relation.object_schema
                )
                # create the migration here now
                object_ddls = self.get_ddl(
                    object_name=relation.object_name,
                    object_type=relation.object_type,
                    object_schema=relation.object_schema,
                )
                for object_ddl in object_ddls:
                    migration_code.append(object_ddl + "\n")
                LOGGER.debug("DDL: TABLE: %s", relation.object_name)
            # dependencies and not already exported
            elif not self.exported_tables.exists(
                table_name=relation.object_name, schema=relation.object_schema
            ):
                # first call itself to ensure the migrations have been
                # created that need to take place first.
                self.exported_tables.add_table(
                    table_name=relation.object_name, schema=relation.object_schema
                )
                migration_code = migration_code + self.create_migrations(
                    relationships=relation,
                )
                LOGGER.debug("DDL: TABLE: %s", relation.object_name)
                object_ddls = self.get_ddl(
                    object_name=relation.object_name,
                    object_type=relation.object_type,
                    object_schema=relation.object_schema,
                )
                for object_ddl in object_ddls:
                    migration_code.append(object_ddl + "\n")

        if not self.exported_tables.exists(
            table_name=relation.object_name, schema=relation.object_schema
        ):
            self.exported_tables.add_table(
                table_name=relation.object_name, schema=relation.object_schema
            )

            LOGGER.debug("DDL: TABLE: %s", relationships.object_name)
            object_ddls = self.get_ddl(
                object_name=relationships.object_name,
                object_type=relationships.object_type,
                object_schema=relationships.object_schema,
            )
            for object_ddl in object_ddls:
                migration_code.append(object_ddl + "\n")
        return migration_code

    def get_ddl(
        self,
        object_name: str,
        object_schema: str,
        object_type: types.ObjectType,
    ) -> list[str]:
        """
        Return ddl for given object.

        Queries the databases metdata / getddl function to retrieve the ddl
        associated with the object.  Configures the metadata query so that it
        doesn't return parameters like Tablespace, max extents, etc...

        :param object_name: The name of the object to generate the DDL for.
        :type object_name: str
        :param object_schema: The schema for the given object.
        :type object_schema: str
        :param object_type: The type of database object.. (currently only
            supports Table)
        :type object_type: types.ObjectType
        :return: A list of strings that represent DDL statements required to
            create the object.
        :rtype: list[str]
        """
        LOGGER.debug("object name: %s", object_name)
        plsql_env_config = """
            begin
                dbms_metadata.set_transform_param
                    (dbms_metadata.session_transform, 'SQLTERMINATOR', true);
                dbms_metadata.set_transform_param
                    (dbms_metadata.session_transform, 'PRETTY', true);
                dbms_metadata.set_transform_param
                    (dbms_metadata.session_transform,
                    'SEGMENT_ATTRIBUTES', false);
                dbms_metadata.set_transform_param
                    (dbms_metadata.session_transform, 'STORAGE', false);
                dbms_metadata.set_transform_param
                    (dbms_metadata.session_transform, 'TABLESPACE', false);
            end;
            """
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

    def get_triggers(
        self, table_name: str, schema: str, include_disabled: bool = False
    ):
        """
        Identify the triggers that are defined for a given table.

        Queries the metadata to identify triggers that have been defined on the
        provided table

        :param table_name: The table name to get triggers for.
        :type table_name: str
        :param schema: The schema that contains the input table
        :type schema: str
        :param include_disabled: by default will not include disabled triggers,
            if you want disabled triggers then set this to be true
        :type include_disabled: bool
        """
        query = """
            SELECT
                TRIGGER_NAME
            FROM
                ALL_TRIGGERS
            WHERE
                TABLE_NAME=:table_name and OWNER=:schema AND STATUS = 'ENABLED'
        """
        cursor = self.connection.cursor()
        LOGGER.debug("table name / schema: %s / %s", table_name, schema)

        cursor.execute(query, table_name=table_name, schema=schema)
        cur_results = cursor.fetchall()
        LOGGER.debug("cursor results: %s", cur_results)
        return cur_results

    def get_trigger_deps(self, table_name: str, schema: str) -> list[types.Dependency]:
        """
        Identify the triggers dependencies for the given table.

        Queries the metadata to identify any triggers for the given table, then
        iterates over each trigger retrieving any dependencies for each trigger
        and then returns as a list of dependencies.

        :param table_name: name of the trigger who's dependencies are to be
            retrieved.
        :type table_name: str
        :param schema: The schema where the table has setup is resort like
            casita.
        :type schema: schema
        :return: a list of trigger dependencies for the given table.
        :rtype: list[types.Dependency]
        """
        triggers = self.get_triggers(table_name=table_name, schema=schema)
        dependencies = []
        for trigger in triggers:
            trigger_name = trigger[0]
            query = """
                SELECT REFERENCED_NAME, REFERENCED_TYPE, REFERENCED_OWNER
                FROM DBA_DEPENDENCIES
                WHERE
                    name=:trigger_name AND
                    owner=:schema AND
                    (TYPE != 'TRIGGER' OR REFERENCED_OWNER != 'SYS') AND
                    (REFERENCED_NAME!=:table_name OR REFERENCED_TYPE != 'TABLE')
            """
            cursor = self.connection.cursor()
            cursor.execute(
                query, trigger_name=trigger_name, schema=schema, table_name=table_name
            )
            cur_results = cursor.fetchall()
            for cur_result in cur_results:
                object_type = types.ObjectType[cur_result[1]]
                dependency = types.Dependency(
                    object_name=cur_result[0],
                    object_type=object_type,
                    object_schema=cur_result[2],
                )
                dependencies.append(dependency)
            LOGGER.debug("cursor results: %s", cur_results)
        return dependencies

    def add_trigger_deps(self)

class ExportTables:
    """
    Used to keep track of what tables have been added to a migration file.

    This is used to prevent duplicate migrations from being created, which would
    create an error when the migrations are run.
    """

    def __init__(self) -> None:
        """
        Class constructor.
        """
        self.exported_tables = []

    def add_table(self, table_name: str, schema: str) -> None:
        """
        Add table to the exported tables list.

        :param table_name: input table name
        :type table_name:
        :param schema: Input schema name
        :type schema: str
        """
        self.exported_tables.append(f"{schema.upper()}.{table_name.upper()}")

    def add_tables(self, tables: list[types.Table]) -> None:
        """
        Add list of tables to the exported tables list.

        :param tables: list of table to add to the exported tables list, that
            is used to keep track of table that have already been exported.
        :type tables: list[types.Table]
        """
        for table_struct in tables:
            self.add_table(
                table_name=table_struct.table_name, schema=table_struct.schema
            )

    def exists(self, table_name: str, schema: str) -> bool:
        """
        Check if table exists in the exported tables list.

        :param table_name: _description_
        :type table_name: str
        :param schema: _description_
        :type schema: str
        :return: _description_
        :rtype: bool
        """
        return f"{schema.upper()}.{table_name.upper()}" in self.exported_tables
        return f"{schema.upper()}.{table_name.upper()}" in self.exported_tables
