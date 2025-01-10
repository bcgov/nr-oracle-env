"""
Create migration files.
"""

import logging
import pathlib

import packaging.version
import sql_metadata
import sqlparse

from . import types

LOGGER = logging.getLogger(__name__)


class MigrationFile:

    def __init__(
        self,
        version: packaging.version.Version,
        description: str,
        migration_folder: pathlib.Path,
    ):
        self.version = version
        self.description = description
        self.migration_folder = migration_folder

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
        migration_file_name = "V" + str(version) + "__" + self.description + ".sql"
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
            current_migration_version = current_migration_file.stem.split("__")[0][1:]
            current_version = packaging.version.Version(current_migration_version)
            latest_migration_version = self.increment_version(current_version)
            return latest_migration_version
        else:
            return self.version

    def increment_version(
        self, version: packaging.version.Version
    ) -> packaging.version.Version:
        """
        Increment the version number of the migration file.
        """
        version_parts = list(version.release)
        version_parts[-1] += 1
        new_version = packaging.version.Version(".".join(map(str, version_parts)))
        return new_version

    def write_migrations(self, migration_list: list[str]) -> None:
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

    def __init__(self, migration_file: pathlib.Path):
        self.migration_file = migration_file

    def get_dependency(self) -> list[types.Dependency]:

        db_objects = []

        with self.migration_file.open("r") as fh:
            migration_str = fh.read()
        statements = sqlparse.split(migration_str)

        for statement in statements:
            parsed = sqlparse.parse(statement)
            statement_obj = sqlparse.sql.Statement(parsed[0].tokens)
            statement_type = statement_obj.get_type()
            ids = sqlparse.sql.Identifier(parsed[0].tokens)
            is_table = False
            for id in ids:
                # if id.is_keyword and id.value == "TABLE":
                if id.is_keyword and id.value in types.ObjectType.__members__:
                    is_valid_object = True
                    object_type = types.ObjectType[id.value]
                    break

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
                        token, search_stack_ttypes[cur_idx][2]
                    )
                    cur_idx += 1
                if cur_idx >= len(search_stack_ttypes):
                    break
                LOGGER.debug("token ttype type: %s", type(token.ttype))
                LOGGER.debug("token.value: %s", token.value)
                LOGGER.debug("token type: %s", type(token))
                # if token.ttype == search_stack_ttypes[cur_idx][0]:
            # get rid of unnecessary quotes
            data_dict["object_name"] = data_dict["object_name"].replace('"', "")
            schema, object_name = data_dict["object_name"].split(".")
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

        :return: a list of the tables that will be created by the migration file.
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
            for id in ids:
                if id.is_keyword and id.value == "TABLE":
                    is_table = True
                    break
                    # tables = sql_metadata.Parser(statement).tables
                    # print(tables)
            if statement_type == "CREATE" and is_table:
                statement_tables = sql_metadata.Parser(statement).tables
                for statement_table in statement_tables:
                    statement_table = statement_table.split(".")
                    table_def = types.Table(
                        table_name=statement_table[1], schema=statement_table[0]
                    )
                    tables.append(table_def)

        return tables


if __name__ == "__main__":
    migration_path_str = "/home/kjnether/fsa_proj/nr-fsa-orastruct/data-query-tool/data/migrations/V1.0.0__first_migration.sql"
    migration_file = pathlib.Path(migration_path_str)

    migration = MigrationFileParser(migration_file)
    migration.get_tables()
