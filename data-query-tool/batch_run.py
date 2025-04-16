"""
Support bach run of dependencies and migrations.
"""

import json
import logging.config
import pathlib

import packaging.version
from data_query_tool import constants, migration_files, oralib

LOGGER = None


class BatchRun:
    """
    Batch run dependencies or migrations.
    """

    def __init__(self, migration_folder, table_list_file) -> None:
        """
        Construct a BatchRun object.
        """
        self.logging_config()
        self.batch_file = table_list_file
        self.migration_folder = migration_folder

        LOGGER.debug("migration path: %s", self.migration_folder)
        self.schema = "THE"
        # force a new major version
        self.initial_migration_str = "1.0.0"

    def logging_config(self) -> None:
        """
        Configure the logging for the cli.
        """
        log_conf_path = pathlib.Path(__file__).parent / "logging.config"
        logging.config.fileConfig(log_conf_path)
        logger = logging.getLogger(__name__)
        logger.debug("testing logger config...")
        global LOGGER  # noqa: PLW0603
        LOGGER = logger

    def run_all(self) -> None:
        """
        Generate migrtations for all the tables in the supplied list.
        """
        # iterates over each new table creating a new migration
        with self.batch_file.open("r") as f:
            for record in f:
                table_name = record.split(",")[0]
                table_name = table_name.strip()
                self.dump_migrations(table_name)

    def dump_migrations(self, seed_table: str) -> None:
        """
        Generate a migration file for the seed table.

        :param seed_table: name of the seed table, generates a migration for
                           for this table and any dependencies it may have.
        :type seed_table: str
        """
        LOGGER.debug("seed table: %s", seed_table)
        migration_folder = self.migration_folder
        schema = self.schema.lower()
        LOGGER.debug("schema: %s", schema)
        migration_version = self.initial_migration_str

        migration_name = seed_table.lower()

        db_cons = constants.get_database_connection_parameters()
        LOGGER.debug("db_cons: %s", db_cons)
        ora = oralib.Oracle(db_cons)
        tabs = ora.get_related_tables_sa(table_name=seed_table, schema=schema)

        # create migrations from the table relationship structure
        # no intelligence at the moment, but down the road may want to add the
        # ability to increment the version number based on the existing
        # migration

        # get existing migration files
        # extract the tables that are defined in those files
        # add those tables to the ora objects exported_tables properties

        initial_migration_version = packaging.version.parse(migration_version)
        LOGGER.debug("migration folder: %s", migration_folder)
        current_migration_file = migration_files.MigrationFile(
            version=initial_migration_version,
            description=migration_name,
            migration_folder_str=str(migration_folder),
        )
        existing_migration_files = (
            current_migration_file.get_existing_migration_files()
        )
        LOGGER.debug("existing_migration_files: %s", existing_migration_files)

        for migration_file in existing_migration_files:
            migration_file_parser = migration_files.MigrationFileParser(
                migration_file,
            )
            db_objects = migration_file_parser.get_dependency()
            LOGGER.debug("db objects: %s", db_objects)
            ora.exported_objects.add_objects(db_objects)

        # add the existing tables to the ora object so that it doesn't generate
        # duplicate migrations
        ddl_cache = ora.create_migrations(
            tabs,
        )
        migrations_list = ddl_cache.get_ddl()
        # now write migrations to a migration file
        current_migration_file.write_migrations(migrations_list)
        LOGGER.info("PROCESSED %s", seed_table)


if __name__ == "__main__":
    # config:
    migration_folder = (
        pathlib.Path("__file__").parent
        / ".."
        / "project_specific"
        / "silva"
        / "migrations2"
    )

    batch_table_2_run = (
        pathlib.Path("__file__").parent
        / ".."
        / "project_specific"
        / "silva"
        / "misc"
        / "origlist_and_ron.txt"
    )
    # /home/kjnether/fsa_proj/nr-fsa-orastruct/project_specific/silva/misc/origlist_and_ron.txt
    print("table_list", batch_table_2_run)
    print("migration foler", migration_folder)
    # raise
    batchrun = BatchRun(
        migration_folder=migration_folder,
        table_list_file=batch_table_2_run,
    )
    batchrun.run_all()
