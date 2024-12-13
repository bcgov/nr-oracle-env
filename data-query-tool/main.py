"""
Start script to run the data query tool.
"""

import logging.config
import pathlib

import packaging.version
from data_query_tool import constants, migration_files, oralib

LOGGER = None

if __name__ == "__main__":
    # configure the logger
    logging.config.fileConfig("logging.config")
    LOGGER = logging.getLogger(__name__)

    # migrations folder
    migrations_folder = pathlib.Path(__file__).parent / "data" / "migrations"

    # Getting the dependencies for a seed table by querying the foreign key
    # constraints
    db_cons = constants.get_database_connection_parameters()
    ora = oralib.Oracle(db_cons)
    tabs = ora.get_related_tables(table_name="SEEDLOT", schema="THE")
    LOGGER.debug("Related tables: %s", tabs)

    initial_migration_version = packaging.version.parse("1.0.0")
    current_migration_file = migration_files.MigrationFile(
        version=initial_migration_version,
        description="first_migration",
        migration_folder=migrations_folder,
    )

    migrations_list = ora.create_migrations(
        tabs,
    )

    # now write migrations to a migration file
    current_migration_file.write_migrations(migrations_list)
