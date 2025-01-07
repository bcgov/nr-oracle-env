"""
Create migration files.
"""

import logging
import pathlib

import packaging.version

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
        migration_file_name = 'V' + str(self.version) + "__" + self.description + ".sql"
        migration_file = self.migration_folder / migration_file_name
        LOGGER.debug("migration_file is: %s", migration_file)
        return migration_file

    def write_migrations(self, migration_list: list[str]) -> None:
        migration_file = self.get_migration_file()
        LOGGER.debug("migration_file : %s", migration_file)
        with migration_file.open("w") as fh:
            for migration in migration_list:
                LOGGER.debug("migration: %s %s", type(migration), migration)
                fh.write(migration)
