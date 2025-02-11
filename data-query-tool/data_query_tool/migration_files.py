"""
Create migration files.
"""

import logging
import pathlib
import re

import packaging.version
import sql_metadata
import sqlparse
import sqlparse.sql

from . import types

LOGGER = logging.getLogger(__name__)


class MigrationFile:
    """
    Provide utility methods for migration file names.
    """

    def __init__(
        self,
        version: packaging.version.Version,
        description: str,
        migration_folder_str: pathlib.Path,
    ) -> None:
        """
        Initialize the migration file.

        Checks to make sure there is not already a migration file with this
        name, increments the version number for the migration file so that it
        is the next migration to be run, if existing migrations already exist.

        :param version: If there is a specific version id to use for the
            migration file that will be created by this migration.
        :type version: packaging.version.Version
        :param description: A description string for the migration, this is
            used in the name of the migration file.
        :type description: str
        :param migration_folder_str: The folder where the migration file should
            be created.  This parameter is also used to scan existing migrations
            to ensure that newly created migrations do not attempt to create
            existing objects.
        :type migration_folder_str: pathlib.Path
        """
        self.version = version
        self.description = description
        # if the provided migration folder is a relative folder then
        # make it relative to this file

        self.migration_folder = pathlib.Path(migration_folder_str)
        if not self.migration_folder.is_absolute():
            self.migration_folder = (
                pathlib.Path(__file__).parent / ".." / migration_folder_str
            ).resolve()
            LOGGER.debug(
                "migration_folder is not absolute: %s",
                self.migration_folder,
            )
        self.migration_folder.mkdir(parents=True, exist_ok=True)
        LOGGER.debug("created folder: %s", self.migration_folder)

    def get_migration_file(self) -> pathlib.Path:
        """
        Return the name of the migration file to create.

        :param migrations_folder: the folder where the migration file should be
            created
        :type migrations_folder: pathlib.Path
        :param migration_version: the migration version number
        :type migration_version: packaging.version.Version
        """
        version = self.get_latest_migration_file_version()
        migration_file_name = (
            "V" + str(version) + "__" + self.description + ".sql"
        )
        migration_file = self.migration_folder / migration_file_name
        if migration_file.exists():
            LOGGER.warning("migration file already exists: %s", migration_file)
            self.version = self.increment_version(self.version)
            migration_file = self.get_migration_file()

            LOGGER.info("incrementing the version file to: %s", self.version)
        LOGGER.debug("migration_file is: %s", migration_file)
        return migration_file

    def get_migration_file_by_type(
        self,
        base_migration_file: pathlib.Path,
        migration_type: types.DDLType,
    ) -> pathlib.Path:
        """
        Return the name of the migration file to create.

        :param migrations_folder: the folder where the migration file should be
            created
        :type migrations_folder: pathlib.Path
        :param migration_version: the migration version number
        :type migration_version: packaging.version.Version
        """
        version = self.extract_version(migration_file=base_migration_file)

        suffix = ""
        increment_int = 1
        if migration_type == types.DDLType.PACKAGE:
            suffix = "_P"
        elif migration_type == types.DDLType.TRIGGER:
            suffix = "_T"
            increment_int = 2
        if suffix:
            version = self.increment_version(
                current_version=version,
                increment_type=types.SupportedVersionTypes.MICRO,
                increment=increment_int,
            )

        migration_file_name = (
            "V" + str(version) + "__" + self.description + f"{suffix}.sql"
        )
        migration_file = self.migration_folder / migration_file_name
        if migration_file.exists():
            msg = f"Migration file already exists: {migration_file}"
            LOGGER.error(msg)
            raise FileExistsError(msg)
        LOGGER.debug("migration_file is: %s", migration_file)
        return migration_file

    def get_migration_file_with_suffix(
        self,
        current_migration_file: pathlib.Path,
        migration_suffix: str,
        increment: int = 1,
    ) -> pathlib.Path:
        """
        Create a migration file with a suffix and increment the version number.

        Takes the current migration file, parses out the version number and
        Increments it my a micro version specified in the parameter `increment`
        and adds the supplied suffix to the end of the migration file
        description.

        Example if:
            current_migration_file=V1.0.0__first_migration.sql
            increment=7
            migration_suffix="_CRAP"

        The resulting migration file would be:
            V1.0.7__first_migration_CRAP.sql

        :param current_migration_file: a string that represents the current
            migration file.
        :type current_migration_file: str
        :param migration_suffix: a suffix that should be added to the end of the
            migration file description.
        :type migration_suffix: str
        :param increment: The amount to increment the version number by.
            Defaults to 1.
        :type increment: int, optional
        :raises FileExistsError: If the calculated migration file already
            exists. then raise this exception.
        :return: a pathlib object that represents the migration file to create.
        :rtype: pathlib.Path
        """
        current_migration_file_version = self.extract_version(
            current_migration_file,
        )
        migration_file_version = self.increment_version(
            current_migration_file_version,
            types.SupportedVersionTypes.MICRO,
            increment=increment,
        )
        migration_file_name = (
            "V"
            + str(migration_file_version)
            + "__"
            + self.description
            + f"{migration_suffix}.sql"
        )
        LOGGER.debug(
            "migration file with suffix %s is: %s",
            migration_suffix,
            migration_file_name,
        )
        migration_file = self.migration_folder / migration_file_name
        if migration_file.exists():
            msg = f"migration file already exists: {migration_file}"
            LOGGER.warning(msg)
            raise FileExistsError(msg)
        LOGGER.debug("migration_file with suffix is: %s", migration_file)
        return migration_file

    def get_trigger_migration_file(
        self,
        current_migration_file: pathlib.Path,
    ) -> pathlib.Path:
        """
        Return the name of the trigger migration file to create.

        Takes the current migration file, parses out the version number and
        Increments it my a minor + 1 and adds '_T' to the end of the migration
        file description.

        :param current_migration_file: _description_
        :type current_migration_file: _type_
        :return: _description_
        :rtype: pathlib.Path
        """
        return self.get_migration_file_with_suffix(
            current_migration_file=current_migration_file,
            migration_suffix="_T",
            increment=2,
        )

    def get_package_migration_file(
        self,
        current_migration_file: pathlib.Path,
    ) -> pathlib.Path:
        """
        Return the name of the trigger migration file to create.

        Takes the current migration file, parses out the version number and
        Increments it my a minor + 1 and adds '_T' to the end of the migration
        file description.

        :param current_migration_file: _description_
        :type current_migration_file: _type_
        :return: _description_
        :rtype: pathlib.Path
        """
        return self.get_migration_file_with_suffix(
            current_migration_file=current_migration_file,
            migration_suffix="_P",
            increment=1,
        )

    def get_latest_migration_file_version(self) -> packaging.version.Version:
        """
        Return the latest migration file version.
        """
        migration_files = self.get_existing_migration_files()

        if migration_files:
            next_version = self.get_next_version_file(migration_files)

            LOGGER.debug(
                "latest_migration_version: %s",
                next_version,
            )
            return next_version

        return self.version

    def increment_version(
        self,
        current_version: packaging.version.Version,
        increment_type: types.SupportedVersionTypes = types.SupportedVersionTypes.MINOR,  # noqa: E501
        increment: int = 1,
    ) -> packaging.version.Version:
        """
        Increment the version number of the migration file.

        By default will increment by minor version.  If the increment_type is

        """

        # get the value that corresponds with the property that is contained in
        # the increment_type and increment it by 1.
        new_version_list = []
        zerod = False
        for version_type in ["major", "minor", "micro"]:
            version_num = getattr(current_version, version_type)
            if zerod:
                version_num = 0
            if increment_type.name.lower() == version_type:
                version_num += increment
                zerod = True
            new_version_list.append(version_num)

        new_version = packaging.version.Version(
            ".".join(map(str, new_version_list)),
        )
        LOGGER.debug(
            "new version: %s, old version %s",
            new_version,
            current_version,
        )
        return new_version

    def write_migrations(
        self,
        migration_list: list[types.DDLCachedObject],
    ) -> None:
        """
        Write the migrations to the migration files.

        Standard objects the do not include pl/sql are defined as 'DB_OBJ_DDL'
        types. These objects are written first to the migration file that is
        named exactly with the provide version number (if provided) and the
        description string.  Ie V{supplied_version_number}__{description}.sql.
        A physical migration example of the file name could be:

            `V1.0.0__first_migration.sql`

        Next a migration file is created for each pl/sql package.  It will be
        named like V{supplied_version_number + 1}__{description}_P.sql, so for
        example:

            `V1.0.1__first_migration_P.sql`

        Triggers will then go into thier own migration file... which is named
        V{supplied_version_number}__{description}_T.sql.  for example:

            `V1.0.2__first_migration_T.sql`


        :param migration_list: list of statements that should be executed as
            part of the migration.
        :type migration_list: list[types.DDLCachedObject]
        """

        # only write migrations if you have something to actually write
        LOGGER.debug("migration_list: %s", migration_list)
        if migration_list:
            # base migration file for the core DDL
            migration_file = self.get_migration_file()
            for mgr_cache in migration_list:

                # in case we are writing a trigger or package, get the correct
                # file to write that crap to.
                cur_mig_file = self.get_migration_file_by_type(
                    base_migration_file=migration_file,
                    migration_type=mgr_cache.ddl_type,
                )
                with cur_mig_file.open("w") as fh:
                    for migration_statement in mgr_cache.ddl_definition:
                        fh.write(migration_statement)

        else:
            LOGGER.info("no migrations to write: %s", migration_list)

    def get_existing_migration_files(self) -> list[pathlib.Path]:
        """
        Return a list of existing migration files.
        """
        migration_files = list(self.migration_folder.glob("*.sql"))
        migration_files.sort()
        return migration_files

    def extract_version(
        self,
        migration_file: pathlib.Path,
    ) -> packaging.version.Version:
        """
        Extract the version number from the migration file name.

        For a given flyway migration file, returns the version number as a
        packaging.version.Version object.

        :param migration_file: a migration file name that complies with the
            flyway versioning scheme.
        :type migration_file: str
        """
        migration_version_str = migration_file.stem.split("__")[0][1:]
        return packaging.version.Version(migration_version_str)

    def get_next_version_file(
        self,
        migration_files: list[pathlib.Path],
    ) -> packaging.version.Version:
        """
        Return a file name that contains the next version number.

        Gets a list of files, looks through them for files that comply with the
        flyway versioning scheme, and returns the next version number that
        should be used if creating a new migration file.

        :return: The next version number that doesn't exist in the provided list
            of files
        :rtype: packaging.version.Version
        """
        version_file_ptrn = re.compile(r"^V(\d+(?:\.\d+)*)__")

        max_version = self.version
        for migration_file in migration_files:
            # is it a flyway version file
            if version_file_ptrn.match(migration_file.name):
                migration_version = self.extract_version(migration_file)
                LOGGER.debug("migration_version: %s", migration_version)
            max_version = max(max_version, migration_version)
        next_version = self.increment_version(max_version)
        LOGGER.debug(
            "next_version_str: %s %s",
            next_version,
            type(next_version),
        )
        LOGGER.debug(
            "next version is: %s",
            next_version,
        )
        return next_version


class MigrationFileParser:
    """
    Parse a migration file to extract the object names.

    The migration file is parsed to extract the object names that are created
    by a migration file.  This is used to ensure that existing objects do not
    occur in subsequent migrations.
    """

    def __init__(self, migration_file: pathlib.Path) -> None:
        """
        Initialize the migration file parser.

        :param migration_file: the path to the migration file.
        :type migration_file: pathlib.Path
        """
        self.migration_file = migration_file
        self.get_migration_type()

    def get_migration_type(self) -> types.DDLType:
        """
        Get the type of migration file.

        Makes the assumption that migration file naming complies with the
        convension laid out by this project.  The migration file name should
        end with the following:

        V{version}_{description}_T.sql - for triggers
        V{version}_{description}_P.sql - for packages
        V{version}_{description}.sql - for all other objects

        :return: the type of migration file that is being parsed based on its
            file name.
        :rtype: types.DDLType
        """
        if self.migration_file.stem.endswith("_T"):
            return types.DDLType.TRIGGER
        if self.migration_file.stem.endswith("_P"):
            return types.DDLType.PACKAGE
        return types.DDLType.DB_OBJ_DDL

    def get_dependency(self) -> list[types.Dependency]:
        """
        Get the database dependencies of the current migration file.

        Opens the current migration file (defined in the constructor) and parses
        the content in the migration file to determine what objects are being
        created.

        This method determines what kind of migration file we are dealing with.
        This tool will separate migration files out into three types.  One that
        contains regular database DDL, another for triggers and a third for
        database packages.  Each of these types has its own method to extract
        the objects that are being created in the migration file.

        The reason for the separation is parsing out regular database DDL and
        DDL that contains PL/SQL code is difficult.  Separating the types
        is a way to simple way around having to write a complex parser.

        :raises ValueError: This error gets raised if a unknown migration types
            is passed to this method.  This should never happen.
        :return: a list of database dependency objects that are created in said
            migration file.
        :rtype: list[types.Dependency]
        """
        migration_type = self.get_migration_type()
        if migration_type == types.DDLType.TRIGGER:
            return self.get_dependency_trigger()
        if migration_type == types.DDLType.PACKAGE:
            return self.get_dependency_package()
        if migration_type == types.DDLType.DB_OBJ_DDL:
            return self.get_dependency_db_obj_ddl()
        # if we get here then we have a migration type that is not recognized
        msg = f"migration type: {migration_type} not recognized"
        raise ValueError(msg)

    def get_dependency_package(self) -> list[types.Dependency]:
        """
        Get dependencies of a migration file containing package definitions.

        Specific method to extract package defs from a migration file.

        :return: A database dependency object that describes all the packages
            that are created in the migration file.
        :rtype: list[types.Dependency]
        """

        with self.migration_file.open("r", encoding="utf-8") as file:
            sql_content = file.read()

        # Regex pattern to match Oracle CREATE PACKAGE statements
        regex_str = (
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:EDITIONABLE\s+)?PACKAGE\s+(?!BODY\b)"  # noqa: ISC003
            + r'(?:("?\w+"?)\.)?("?\w+"?)'
        )
        if isinstance(regex_str, tuple):
            LOGGER.debug("regx_str is of type tuple")
            regex_str = " ".join(list(regex_str))
        LOGGER.debug("regx_str is of type %s", type(regex_str))

        package_pattern = re.compile(
            regex_str,
            re.IGNORECASE,
        )

        deps = []

        for match in package_pattern.finditer(sql_content):
            LOGGER.debug("match: %s", match)
            LOGGER.debug("match.group(1): %s", match.group(1))
            LOGGER.debug("match.group(2): %s", match.group(2))
            schema = match.group(1) or None  # Use "default_schema" if missing
            if schema:
                schema = schema.strip('"')

            package_name = match.group(2)
            package_name = package_name.strip('"')

            dep = types.Dependency(
                object_name=package_name,
                object_type=types.ObjectType.PACKAGE,
                object_schema=schema,
            )
            deps.append(dep)

        return deps

    def get_dependency_trigger(self) -> list[types.Dependency]:
        """
        Get the dependencies of the migration file.

        Get the object names and types that are created by the migration file.

        :return: a list of Dependency objects that describe the object names
                and types that are created by the migration file.
        :rtype: list[types.Dependency]
        """
        with self.migration_file.open("r", encoding="utf-8") as file:
            sql_content = file.read()

        # Captures optional schema and trigger name
        trigger_pattern = re.compile(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:EDITIONABLE\s+)?TRIGGER\s+"
            r'(?:("?\w+"?)\.)?("?\w+"?)',
            re.IGNORECASE,
        )

        deps = []

        for match in trigger_pattern.finditer(sql_content):
            schema = match.group(1) or None  # Use "default_schema" if missing
            schema = schema.strip('"')

            trigger_name = match.group(2)
            trigger_name = trigger_name.strip('"')

            LOGGER.debug("schema: %s, trigger_name: %s", schema, trigger_name)
            dep = types.Dependency(
                object_name=trigger_name,
                object_type=types.ObjectType.TRIGGER,
                object_schema=schema,
            )
            deps.append(dep)
        return deps

    def get_dependency_db_obj_ddl(self) -> list[types.Dependency]:
        """
        Get the dependencies of the migration file.

        Get the object names and types that are created by the migration file.

        :return: a list of Dependency objects that describe the object names
                and types that are created by the migration file.
        :rtype: list[types.Dependency]
        """
        db_objects = []

        with self.migration_file.open("r") as fh:
            migration_str = fh.read()
        statements = sqlparse.split(migration_str)

        for statement in statements:
            parsed = sqlparse.parse(statement)

            # leaving this code here for now as it helps me understand the
            # sqlparse library...
            # statement_obj = \  # noqa: ERA001, RUF100
            #     sqlparse.sql.Statement(parsed[0].tokens)  # noqa: ERA001
            # statement_type = statement_obj.get_type()  # noqa: ERA001

            is_valid_object = False

            ids = sqlparse.sql.Identifier(parsed[0].tokens)
            is_table = False  # noqa: F841
            for parse_id in ids:
                LOGGER.debug("id: %s %s", parse_id, type(parse_id))
                # if id.is_keyword and id.value == "TABLE": # unique shows as ttype=Token.Keyword  # noqa: E501
                if (
                    parse_id.is_keyword
                    and parse_id.value in types.ObjectType.__members__
                ):
                    is_valid_object = True
                    object_type = types.ObjectType[parse_id.value]  # noqa: F841
                    break
            if is_valid_object:
                search_stack_ttypes = [
                    [sqlparse.tokens.Keyword.DDL, "ddl_keyword", "value"],
                    [sqlparse.tokens.Keyword, "object_type", "value"],
                    [None, "object_name", "normalized"],
                ]
                cur_idx = 0
                data_dict = {}
                for token in parsed[0].tokens:
                    LOGGER.debug("token: %s", token)
                    LOGGER.debug("token.ttype: %s", token.ttype)
                    if token.ttype == search_stack_ttypes[cur_idx][0]:
                        LOGGER.debug("DDL token: %s", token)
                        data_dict[search_stack_ttypes[cur_idx][1]] = getattr(
                            token,
                            search_stack_ttypes[cur_idx][2],
                        )
                        cur_idx += 1
                    if cur_idx >= len(search_stack_ttypes):
                        break
                    LOGGER.debug("token ttype type: %s", type(token.ttype))
                    LOGGER.debug("token.value: %s", token.value)
                    LOGGER.debug("token type: %s", type(token))
                    # if token.ttype == search_stack_ttypes[cur_idx][0]:
                # get rid of unnecessary quotes
                data_dict["object_name"] = data_dict["object_name"].replace(
                    '"',
                    "",
                )
                LOGGER.debug("object name: %s", data_dict["object_name"])
                if "." in data_dict["object_name"]:
                    schema, object_name = data_dict["object_name"].split(".")
                else:
                    schema = None
                    object_name = data_dict["object_name"]
                    LOGGER.warning(
                        "no schema found for object %s in the file: %s",
                        object_name,
                        self.migration_file,
                    )
                # only write objects that are defined
                if data_dict["object_type"] in types.ObjectType.__members__:
                    object_def = types.DbObject(
                        object_name=object_name,
                        object_schema=schema,
                        object_type=types.ObjectType[data_dict["object_type"]],
                    )
                    db_objects.append(object_def)
        return db_objects

    def get_tables(self) -> list[types.Table]:
        """
        Extract the tables that are created by the migration file.

        Iterates over each statement, determines if the statement is a 'CREATE'
        and the type of object is a 'TABLE'. If so, the table name is extracted
        from the query.

        :return: a list of the tables that will be created by the migration
            file.
        :rtype: list[str]
        """
        tables = []
        with self.migration_file.open("r") as fh:
            migration_str = fh.read()
        statements = sqlparse.split(migration_str)

        for statement in statements:
            parsed = sqlparse.parse(statement)
            statement_obj = sqlparse.sql.Statement(parsed[0].tokens)
            statement_type = statement_obj.get_type()
            ids = sqlparse.sql.Identifier(parsed[0].tokens)
            is_table = False
            for parse_id in ids:
                if parse_id.is_keyword and parse_id.value == "TABLE":
                    is_table = True
                    break
                    # tables = sql_metadata.Parser(statement).tables  # noqa: ERA001, E501
                    # print(tables)  # noqa: ERA001
            if statement_type == "CREATE" and is_table:
                statement_tables = sql_metadata.Parser(statement).tables
                for statement_table in statement_tables:
                    statement_table_list = statement_table.split(".")
                    table_def = types.Table(
                        table_name=statement_table_list[1],
                        schema=statement_table_list[0],
                    )
                    tables.append(table_def)

        return tables
