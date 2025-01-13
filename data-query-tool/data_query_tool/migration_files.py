"""
Create migration files.
"""

import logging
import pathlib

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

    def get_latest_migration_file_version(self) -> packaging.version.Version:
        """
        Return the latest migration file version.
        """
        migration_files = self.get_existing_migration_files()
        if migration_files:
            current_migration_file = migration_files[-1]
            current_migration_version = current_migration_file.stem.split("__")[
                0
            ][1:]
            current_version = packaging.version.Version(
                current_migration_version,
            )
            latest_migration_version = self.increment_version(current_version)
            LOGGER.debug(
                "latest_migration_version: %s",
                latest_migration_version,
            )
            return latest_migration_version

        return self.version

    def increment_version(
        self,
        version: packaging.version.Version,
    ) -> packaging.version.Version:
        """
        Increment the version number of the migration file.
        """
        version_parts = list(version.release)
        version_parts[-1] += 1
        new_version = packaging.version.Version(
            ".".join(map(str, version_parts)),
        )
        LOGGER.debug(
            "new version: %s",
            new_version,
        )
        return new_version

    def write_migrations(self, migration_list: list[str]) -> None:
        """
        Write the migrations to the migration file.

        Recieves a list of migrations statements, and writes them to the
        migration file.

        :param migration_list: list of statements that should be executed as
            part of the migration.
        :type migration_list: list[str]
        """
        # only write migrations if you have something to actually write
        LOGGER.debug("migration_list: %s", migration_list)
        if migration_list:
            migration_file = self.get_migration_file()
            LOGGER.debug("migration_file : %s", migration_file)
            with migration_file.open("w") as fh:
                for migration in migration_list:
                    LOGGER.debug("migration: %s %s", type(migration), migration)
                    fh.write(migration)
        else:
            LOGGER.info("no migrations to write: %s", migration_list)

    def get_existing_migration_files(self) -> list[pathlib.Path]:
        """
        Return a list of existing migration files.
        """
        migration_files = list(self.migration_folder.glob("*.sql"))
        migration_files.sort()
        return migration_files


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

    def get_dependency(self) -> list[types.Dependency]:
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
