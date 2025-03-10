"""

Rewrite of oralib.

Copied a lot of the logic over from the original oralib in order to better
accomodate all the various database objects that might need to be reported out
on.
"""

import logging
import typing
from typing import Optional

import oracledb
import sqlalchemy

from . import types

LOGGER = logging.getLogger(__name__)


class DDLCache:
    """
    Cache DDL statements.

    Used to cache DDL statements that are generated for a given object.  By
    caching here before writing, allows for re-organization of the output by
    database object type.  For example a dependency list is generated, but the
    actual creation of any TRIGGERS is always done last.\
    """

    # Caches the DDL, and organizes by types to ensure that the
    # order of object creation is correct
    def __init__(self, ora_obj: "Oracle") -> None:
        """
        Create instance of DDLCache.

        :param ora_obj: an instance of the Oracle class.
        :type ora_obj: Oracle
        """
        self.ora_obj = ora_obj
        self.ddl_cache = []
        # triggers are created last
        self.triggers = []
        self.packages = []
        self.func_proc = []  # functions and procedures
        self.db_types = []

    def add_ddl(
        self,
        db_object_type: types.ObjectType,
        ddl: list[str],
    ) -> None:
        """
        Add DDL to the cache.

        :param db_object_type: a database object type descriptor.
        :type db_object_type: types.ObjectType
        :param ddl: List of DDL statements associated with this object.
        :type ddl: list[str]
        """
        if db_object_type == types.ObjectType.TRIGGER:
            self.triggers.extend(ddl)
        elif db_object_type == types.ObjectType.PACKAGE:
            self.packages.extend(ddl)
        elif db_object_type in (
            types.ObjectType.FUNCTION,
            types.ObjectType.PROCEDURE,
        ):
            self.func_proc.extend(ddl)
        elif db_object_type == types.ObjectType.TYPE:
            self.db_types.extend(ddl)
        else:
            self.ddl_cache.extend(ddl)

    def get_ddl(self) -> list[types.DDLCachedObject]:
        """
        Compile and return the DDL code.

        Returns a list of strings where each string contains a DDL statement.

        :return: Returns a list of strings where each string contains a DDL
            statement.  Returns the statements in the order in which they should
            be executed.
        :rtype: list[str]
        """
        all_DDL = []  # noqa: N806
        ddl_obj = None
        # the order in which the various types of DDL should be dumped.
        ddl_types = [
            (types.DDLType.DB_OBJ_DDL, self.ddl_cache),
            (types.DDLType.DB_TYPES, self.db_types),
            (types.DDLType.PACKAGE, self.packages),
            (types.DDLType.FUNC_PROC, self.func_proc),
            (types.DDLType.TRIGGER, self.triggers),
        ]
        for ddl_type in ddl_types:
            ddl_type_obj = ddl_type[0]
            ddl_list = ddl_type[1]
            if ddl_list:
                ddl_obj = types.DDLCachedObject(
                    ddl_type=ddl_type_obj,
                    ddl_definition=ddl_list,
                )
                all_DDL.append(ddl_obj)
        return all_DDL

    def merge_caches(self, ddl_cache: "DDLCache") -> None:
        """
        Join two DDL cache objects.

        :param ddl_cache: Input DDL cache that should be merged with this one.
        :type ddl_cache: DDLCache
        """
        self.ddl_cache.extend(ddl_cache.ddl_cache)
        self.triggers.extend(ddl_cache.triggers)
        self.packages.extend(ddl_cache.packages)
        self.func_proc.extend(ddl_cache.func_proc)
        self.db_types.extend(ddl_cache.db_types)


class Oracle:
    """
    Interface to communication with the oracle database.
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
        self.exported_objects = ExportedObjects()

        # a list of tables who's deps have been defined.  If a table is
        # encountered that has been added to the list, then the tool will
        # not get the parent dependencies for this table, or it would create
        # a infinite loop.
        self.processed_objects = ProcessedObjects()

        # mapping database object types to the methods that will handle
        # the retrieval of their dependencies
        self.data_type_handlers = {
            types.ObjectType.TABLE: self.get_table_deps,
            types.ObjectType.PACKAGE: self.get_deps,
            types.ObjectType.TRIGGER: self.get_trigger_deps,
            types.ObjectType.PROCEDURE: self.get_deps,
            types.ObjectType.FUNCTION: self.get_deps,
            types.ObjectType.VIEW: self.get_deps,
            types.ObjectType.SEQUENCE: self.get_deps,
            types.ObjectType.TYPE: self.get_deps,
            types.ObjectType.SYNONYM: self.get_deps,
        }

        self.dba_deps_query = """
            SELECT
            REFERENCED_NAME, REFERENCED_TYPE, REFERENCED_OWNER
            FROM
                DBA_DEPENDENCIES
            WHERE
                name = :object_name AND
                owner = :schema AND
                referenced_name <> :object_name -- don't want SELF reference
                AND NOT
                REFERENCED_OWNER = 'SYS' AND NOT
                ( REFERENCED_OWNER = 'PUBLIC' AND
                    REFERENCED_TYPE = 'SYNONYM' ) AND NOT
                ( REFERENCED_OWNER = 'MDSYS' AND
                    REFERENCED_NAME = 'SDO_GEOMETRY' AND
                    REFERENCED_TYPE = 'TYPE' ) AND NOT
                ( REFERENCED_NAME in ('STANDARD', 'DBMS_STANDARD',
                    'SYS_STUB_FOR_PURITY_ANALYSIS') AND
                  REFERENCED_TYPE='PACKAGE' AND REFERENCED_OWNER = 'SYS')
        """

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
            self.sa_engine.connect()

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

    def get_table_deps(
        self,
        object_name: str,
        schema: str,
        *,
        parent_obj: str | None = None,  # noqa: ARG002
    ) -> types.DBDependencyMapping:
        """
        Retrieve the dependencies for the given table.

        Determines dependencies by chasing down foreign keys.  Does not search
        for other tables who's foreign keys reference the given table.

        After chasing  the foreign keys, queries for triggers that exist on the
        table and returns those as dependencies of the table.

        :param object_name: the name of the table to get dependencies for.
        :type object_name: str
        :param schema: the schema that the table exists in.
        :type schema: str
        :param parent_obj: if there is a parent object in the dependency
            analysis add here, defaults to None
        :type parent_obj: str, optional
        :return: a dependency object that describes the table and its immediate
            dependencies.
        :rtype: types.DBDependencyMapping
        """
        # Handle the foreign key relations
        self.connect_sa()
        metadata = sqlalchemy.MetaData()
        LOGGER.debug("incomming table: %s", object_name)
        table = sqlalchemy.Table(
            object_name.lower(),
            metadata,
            schema=schema.lower(),
            autoload_with=self.sa_engine,
        )
        related_tables = []
        for fk in table.foreign_keys:
            LOGGER.debug(
                "Table %s depends on %s %s",
                object_name,
                fk.column.table.name,
                fk.column.table.primary_key.name,
            )
            related_tables.append(
                (fk.column.table.schema, fk.column.table.name),
            )
        current_table_dep = types.DBDependencyMapping(
            object_type=types.ObjectType.TABLE,
            object_name=object_name,
            object_schema=schema,
            dependency_list=[],
        )
        # get rid of dups
        related_tables = set(related_tables)
        for related_schema, related_table in related_tables:
            dep = types.DBDependencyMapping(
                object_type=types.ObjectType.TABLE,
                object_name=related_table.upper(),
                object_schema=related_schema.upper(),
                dependency_list=[],
            )
            if dep:
                current_table_dep.dependency_list.append(dep)
        # get trigger deps for the table
        trigger_deps = self.get_table_trigger_deps(
            table_name=object_name,
            schema=schema,
        )
        # if the table has not already been processed, then add the trigger deps
        # otherwise remove any deps for the table as they could lead to an
        # infinite loop
        if trigger_deps:
            current_table_dep.dependency_list.extend(trigger_deps)

        if self.processed_objects.exists(
            object_name=object_name,
            object_type=types.ObjectType.TABLE,
        ):
            current_table_dep.dependency_list = []
        else:
            self.processed_objects.add(
                object_name=object_name,
                object_type=types.ObjectType.TABLE,
            )
        return current_table_dep

    def get_table_trigger_deps(
        self,
        table_name: str,
        schema: str,
        *,
        include_disabled: bool = False,
    ) -> list[types.DBDependencyMapping]:
        # get the triggers that are defined for a table
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
        LOGGER.debug("triggers for table: %s", table_name)
        query = """
            SELECT
                TRIGGER_NAME,
                OWNER
            FROM
                ALL_TRIGGERS
            WHERE
                TABLE_NAME=:table_name and OWNER=:schema
        """
        if not include_disabled:
            query = query + " AND STATUS = 'ENABLED'"
        cursor = self.connection.cursor()
        LOGGER.debug(
            "table name / schema: %s / %s",
            table_name.upper(),
            schema.upper(),
        )

        cursor.execute(query, table_name=table_name, schema=schema)
        cur_results = cursor.fetchall()
        # convert into a simple list of trigger names
        LOGGER.debug("trigger deps cursor results: %s", cur_results)
        trigger_list = []
        for cur_result in cur_results:
            LOGGER.debug(
                "trigger name: %s depends on table %s",
                cur_result[0],
                table_name,
            )
            dep_obj = types.DBDependencyMapping(
                object_type=types.ObjectType.TRIGGER,
                object_name=cur_result[0],
                object_schema=cur_result[1],
                dependency_list=[],
            )
            trigger_list.append(dep_obj)
        return trigger_list

    def get_db_object_deps(
        self,
        object_name: str,
        object_type: types.ObjectType,
        schema: str,
        *,
        parent_obj: str | None = None,  # noqa: ARG002
        depth: int = 0,
    ) -> types.DBDependencyMapping:
        """
        Retrieve the dependency tree for the given object.

        A generic dependency generator.  Should be able to take any database
        object, and generate a dependency tree for it.

        :param object_name: _description_
        :type object_name: str
        :param object_type: _description_
        :type object_type: types.ObjectType
        :param schema: _description_
        :type schema: str
        :param parent_obj: _description_, defaults to None
        :type parent_obj: str, optional
        :param depth: _description_, defaults to 0
        :type depth: int, optional
        :return: _description_
        :rtype: types.DBDependencyMapping
        """
        existing = []
        if not existing:
            existing = []
        LOGGER.debug("%s", "-" * 50)
        LOGGER.debug(
            "object type: %s  object_name: %s",
            object_type,
            object_name,
        )
        LOGGER.debug("depth: %s", depth)

        # need to map the object type to a method
        deps_obj = None
        try:
            # calling the method that corresponds with the object type
            deps_obj = self.data_type_handlers[object_type](
                object_name=object_name,
                schema=schema,
            )
            # Now iterate over the dependencies_list
            updated_deps = []
            if deps_obj and deps_obj.dependency_list:
                LOGGER.debug(
                    "getting dependencies for: %s %s",
                    object_name,
                    object_type,
                )
                # recurse!
                for dep in deps_obj.dependency_list:
                    new_dep = self.get_db_object_deps(
                        object_name=dep.object_name,
                        object_type=dep.object_type,
                        schema=dep.object_schema,
                        parent_obj=object_name,
                        depth=depth + 1,
                    )
                    if new_dep:
                        updated_deps.append(new_dep)
                    elif not new_dep and dep:
                        updated_deps.append(dep)
                    # remove duplicates - long term shouldn't need this
                    updated_deps = self.remove_duplicates(updated_deps)

            deps_obj.dependency_list = updated_deps
        except KeyError:
            LOGGER.exception("unknown key: %s", object_type)
            raise
        if deps_obj:
            LOGGER.debug("number of deps: %s", len(deps_obj.dependency_list))
        else:
            LOGGER.debug("no data for %s %s", object_name, object_type)
        return deps_obj

    def remove_duplicates(
        self,
        dependency_list: list[types.DBDependencyMapping],
    ) -> list[types.DBDependencyMapping]:
        """
        Remove duplicates and return a list of dependencies.

        :param dependency_list: list of dependencies
        :type dependency_list: list[types.DBDependencyMapping]
        :return: the same list as recieved, but without any duplicates
        :rtype: list[types.DBDependencyMapping]
        """
        if not dependency_list:
            return dependency_list
        no_dups = []
        ids = []
        for dep in dependency_list:
            unique_id = f"{dep.object_name}-{dep.object_type.name}"
            if unique_id not in ids:
                no_dups.append(dep)
                ids.append(unique_id)
        return no_dups

    def get_object_type(
        self,
        object_name: str,
        schema: str,
    ) -> types.ObjectType:
        """
        Return the object type.

        Queries the database for the specified object and returns its type.

        :param object_name: input object name
        :type object_name: _type_
        :param schema: schema that the object exists within.
        :type schema: _type_
        :raises ValueError: If more than one object is found in the database
            with the same name, raise this error.
        :return: the object type.
        :rtype: types.ObjectType
        """
        # assuming that all packages will also have a package body
        query = """
        SELECT object_type
        FROM all_objects
        WHERE
            OBJECT_NAME = :object_name AND
            OWNER = :schema AND
            OBJECT_TYPE != 'PACKAGE BODY'
        """
        cursor = self.connection.cursor()
        cursor.execute(
            query,
            object_name=object_name,
            schema=schema,
        )
        cur_results = cursor.fetchall()
        cursor.close()
        if len(cur_results) > 1:
            msg = (
                f"found {len(cur_results)} number of objects with the name "
                f"{object_name}"
            )
            LOGGER.error(msg)
            raise ValueError(msg)
        LOGGER.debug("cur_results: %s", cur_results)
        object_type = types.ObjectType[cur_results[0][0]]
        LOGGER.debug("object type: %s", object_type)
        return object_type

    def get_deps(
        self,
        object_name: str,
        schema: str,
        *,
        parent_obj: str | None = None,  # noqa: ARG002
    ) -> types.DBDependencyMapping:
        """
        Return a hierarchical data structure with table dependencies.
        """
        LOGGER.debug("current package: %s", object_name)
        object_type = self.get_object_type(
            object_name=object_name,
            schema=schema,
        )
        query = self.dba_deps_query
        cursor = self.connection.cursor()
        cursor.execute(
            query,
            object_name=object_name,
            schema=schema,
        )
        cur_results = cursor.fetchall()
        deps = []
        for cur_result in cur_results:
            dep = types.DBDependencyMapping(
                object_name=cur_result[0],
                object_type=types.ObjectType[cur_result[1]],
                object_schema=cur_result[2],
                dependency_list=[],
            )
            deps.append(dep)

        package_deps = types.DBDependencyMapping(
            object_name=object_name,
            object_type=object_type,
            object_schema=schema,
            dependency_list=deps,
        )

        if self.processed_objects.exists(
            object_name=object_name,
            object_type=object_type,
        ):
            package_deps.dependency_list = []
        else:
            self.processed_objects.add(
                object_name=object_name,
                object_type=object_type,
            )
        return package_deps

    def get_trigger_deps(
        self,
        object_name: str,
        schema: str,
        *,
        parent_obj: str | None = None,
    ) -> list[types.DBDependencyMapping]:
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

        # query database for trigger dependencies
        query = """
            SELECT REFERENCED_NAME, REFERENCED_TYPE, REFERENCED_OWNER
            FROM DBA_DEPENDENCIES
            WHERE
                name=:object_name AND
                owner=:schema AND
                (TYPE != 'TRIGGER' OR REFERENCED_OWNER != 'SYS') AND
                (REFERENCED_NAME!=:table_name OR
                    REFERENCED_TYPE != 'TABLE') AND NOT
                    ( REFERENCED_OWNER = 'PUBLIC' AND REFERENCED_NAME = 'DUAL'
                        AND REFERENCED_TYPE = 'SYNONYM' ) AND NOT
                    ( REFERENCED_OWNER = 'MDSYS' AND
                      REFERENCED_NAME = 'SDO_GEOMETRY' AND
                      REFERENCED_TYPE = 'TYPE' )
        """
        cursor = self.connection.cursor()
        cursor.execute(
            query,
            object_name=object_name,
            schema=schema,
            table_name=parent_obj,
        )
        cur_results = cursor.fetchall()
        deps = []
        # iterating over the trigger dependencies
        for cur_result in cur_results:
            LOGGER.debug(
                "trigger name: %s depends on %s, of type %s",
                object_name,
                cur_result[0],
                cur_result[1],
            )
            object_type = types.ObjectType[cur_result[1]]
            deps.append(
                types.DBDependencyMapping(
                    object_name=cur_result[0],
                    object_type=object_type,
                    object_schema=cur_result[2],
                    dependency_list=[],
                ),
            )

        trigger_dep = types.DBDependencyMapping(
            object_name=object_name,
            object_type=types.ObjectType.TRIGGER,
            object_schema=schema,
            dependency_list=deps,
        )

        if self.processed_objects.exists(
            object_name=object_name,
            object_type=types.ObjectType.TRIGGER,
        ):
            trigger_dep.dependency_list = []
        else:
            self.processed_objects.add(
                object_name=object_name,
                object_type=types.ObjectType.TRIGGER,
            )

        return trigger_dep

    def get_view_deps(
        self,
        object_name: str,
        schema: str,
        *,
        parent_obj: str | None = None,  # noqa: ARG002
    ) -> list[types.DBDependencyMapping]:
        """
        Return a dependency object for a view.

        :param object_name: name of the view to retrieve dependencies for.
        :type object_name: str
        :param schema: name of the schema where the object can be found.
        :type schema: str
        :param parent_obj: The parent object of the view, defaults to None
        :type parent_obj: str, optional
        :return: a dependency object that describes the dependencies of the
            view.
        :rtype: list[types.DBDependencyMapping]
        """
        LOGGER.debug("current view: %s", object_name)
        query = self.dba_deps_query
        cursor = self.connection.cursor()
        cursor.execute(
            query,
            package_name=object_name,
            schema=schema,
        )
        cur_results = cursor.fetchall()
        deps = []
        for cur_result in cur_results:
            dep = types.DBDependencyMapping(
                object_name=cur_result[0],
                object_type=types.ObjectType[cur_result[1]],
                object_schema=cur_result[2],
                dependency_list=[],
            )
            deps.append(dep)

        package_deps = types.DBDependencyMapping(
            object_name=object_name,
            object_type=types.ObjectType.VIEW,
            object_schema=schema,
            dependency_list=deps,
        )
        LOGGER.debug("package_deps: %s", package_deps)
        return package_deps

    def get_ddl(
        self,
        db_object: types.Dependency,
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
        LOGGER.debug("object name: %s", db_object.object_name)
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
            object_name=db_object.object_name.upper(),
            object_type=db_object.object_type.name.upper(),
            object_schema=db_object.object_schema.upper(),
        )
        ddl_result_cell = cursor.fetchone()
        if db_object.object_type == types.ObjectType.TYPE:
            ddl_str = ddl_result_cell[0].read() + ";\n"
        else:
            ddl_str = ddl_result_cell[0].read() + "\n"
        LOGGER.debug("ddl for %s is %s", db_object.object_name, ddl_str)
        cursor.close()
        return [ddl_str]

    def create_migrations(
        self,
        relationships: types.DBDependencyMapping,
    ) -> DDLCache:
        """
        Create database migrations for the input relationships.

        Iterates over the relationships starting with the outer edges of the
        recursive structure, creating migrations in the order which they
        need to be created in order to maintain referential integrity.

        :param relationships: _description_
        :type relationships: types.ObjectStoreParameters
        """
        ddl_cache = DDLCache(ora_obj=self)
        for relation in relationships.dependency_list:
            dependency_obj = typing.cast(types.Dependency, relation)
            # no dependencies and not already exported
            if (
                not relation.dependency_list
                and not self.exported_objects.exists(dependency_obj)
            ):
                self.exported_objects.add_object(dependency_obj)
                # create the migration here now
                object_ddls = self.get_ddl(
                    dependency_obj,
                )
                ddl_cache.add_ddl(
                    db_object_type=dependency_obj.object_type,
                    ddl=object_ddls,
                )
                LOGGER.debug("DDL: TABLE: %s", relation.object_name)
            # dependencies and not already exported, recurse...
            elif not self.exported_objects.exists(
                dependency_obj,
            ):
                # first call itself to ensure the migrations have been
                # created that need to take place first.
                self.exported_objects.add_object(dependency_obj)
                ddl_cache.merge_caches(
                    self.create_migrations(
                        relationships=relation,
                    ),
                )
                LOGGER.debug("DDL: TABLE: %s", relation.object_name)
                object_ddls = self.get_ddl(dependency_obj)
                ddl_cache.add_ddl(
                    db_object_type=dependency_obj.object_type,
                    ddl=object_ddls,
                )

        dependency_obj = typing.cast(types.Dependency, relationships)
        if not self.exported_objects.exists(
            dependency_obj,
        ):
            self.exported_objects.add_object(
                dependency_obj,
            )

            LOGGER.debug("DDL: TABLE: %s", relationships.object_name)
            object_ddls = self.get_ddl(dependency_obj)
            ddl_cache.add_ddl(
                db_object_type=dependency_obj.object_type,
                ddl=object_ddls,
            )
        return ddl_cache


class ProcessedObjects:
    """
    Keep track of what objects have already been processed.

    This class is designed to prevent recursive infinite loops when iterating
    over dependencies.

    :return: _description_
    :rtype: _type_
    """

    def __init__(self) -> None:
        """
        Construct object used to track db objects that have been processed.
        """
        self.processed_objects = {}

    def add(self, object_name: str, object_type: types.ObjectType) -> None:
        """
        Add a new object to the list of objects that have been processed.

        :param object_name: name of the database object
        :type object_name: str
        :param object_type: type of the database object
        :type object_type: types.ObjectType
        """
        self.check_type(object_type)
        if not self.exists(object_name=object_name, object_type=object_type):
            self.processed_objects[object_type.name].append(object_name.upper())

    def check_type(self, object_type: types.ObjectType) -> None:
        """
        Create a dictionary for the type in one does not exist.

        Each different object type has its own dictionary to track what has
        been processed.  These dictionaries are built on an as needed basis.
        This method checks and declares the dictionary for the specified type
        if one does not exist.

        :param object_type: database object type.
        :type object_type: types.ObjectType
        """
        if object_type.name not in self.processed_objects:
            self.processed_objects[object_type.name] = []

    def exists(self, object_name: str, object_type: types.ObjectType) -> bool:
        """
        Identify if an object has already been processed.

        :param object_name: name of the object.
        :type object_name: str
        :param object_type: type of the object.
        :type object_type: types.ObjectType
        :return: a boolean that indicates if the object has already been
            processed.
        :rtype: bool
        """
        self.check_type(object_type=object_type)
        exists = False
        if object_name.upper() in self.processed_objects[object_type.name]:
            exists = True
        LOGGER.debug(
            "object: %s type: %s exists: %s",
            object_name,
            object_type,
            exists,
        )
        LOGGER.debug("existing: %s", self.processed_objects)
        return exists


class ExportedObjects:
    """
    Used to keep track of what objects have been added to a migration files.

    This is used to prevent duplicate migrations from being created, which would
    create an error when the migrations are run.
    """

    def __init__(self) -> None:
        """
        Class constructor.
        """
        self.exported_objects = []

    def add_object(self, db_object: types.Dependency) -> None:
        """
        Add table to the exported tables list.

        :param table_name: input table name
        :type table_name:
        :param schema: Input schema name
        :type schema: str
        """
        self.exported_objects.append(db_object)

    def add_objects(self, db_objects: list[types.Dependency]) -> None:
        """
        Add list of tables to the exported tables list.

        :param tables: list of table to add to the exported tables list, that
            is used to keep track of table that have already been exported.
        :type tables: list[types.Table]
        """
        for db_object in db_objects:
            self.add_object(db_object=db_object)

    def exists(self, db_object: types.Dependency) -> bool:
        """
        Check if table exists in the exported tables list.

        :param table_name: _description_
        :type table_name: str
        :param schema: _description_
        :type schema: str
        :return: _description_
        :rtype: bool
        """
        exists = False
        for obj in self.exported_objects:
            if (
                obj.object_name.lower() == db_object.object_name.lower()
                and obj.object_schema.lower() == db_object.object_schema.lower()
                and obj.object_type == db_object.object_type
            ):
                exists = True
                break
        return exists

    def table_exists(self, table_name: str, table_schema: str) -> bool:
        """
        Check if table exists in the exported tables list.

        :param table_name: _description_
        :type table_name: str
        :param schema: _description_
        :type schema: str
        :return: _description_
        :rtype: bool
        """
        db_object = types.Dependency(
            object_name=table_name,
            object_schema=table_schema,
            object_type=types.ObjectType.TABLE,
        )
        return self.exists(db_object)
