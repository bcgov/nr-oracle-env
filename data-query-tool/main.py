"""
Start script to run the data query tool.
"""

import logging.config  # noqa: I001
import pathlib
import pprint

import packaging.version
from data_query_tool import constants, migration_files, oralib

LOGGER = None

if __name__ == "__main__":
    # configure the logger
    log_conf_path = pathlib.Path(__file__).parent / "logging.config"

    logging.config.fileConfig(log_conf_path)
    LOGGER = logging.getLogger(__name__)

    # migrations folder
    migrations_folder = pathlib.Path(__file__).parent / "data" / "migrations"

    # Getting the dependencies for a seed table by querying the foreign key
    # constraints
    db_cons = constants.get_database_connection_parameters()
    ora = oralib.Oracle(db_cons)
    tabs = ora.get_related_tables_sa(table_name="SEEDLOT", schema="THE")

    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(tabs)

    # # create migrations from the table relationship structure
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
