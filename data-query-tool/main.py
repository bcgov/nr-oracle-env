"""
Start script to run the data query tool.
"""

import logging.config  # noqa: I001
import pathlib
import pprint
import json
import click
import packaging.version
import sys
from data_query_tool import constants, migration_files, oralib

LOGGER = None


def configure_logging():
    """
    Configure the logging for the application.
    """
    global LOGGER
    log_conf_path = pathlib.Path(__file__).parent / "logging.config"
    logging.config.fileConfig(log_conf_path)
    LOGGER = logging.getLogger(__name__)

@click.command()
@click.option(
    "--seed-table",
    multiple=False,
    required=True,
    help="Specify the seed table to use to identify dependencies.")
@click.option(
    "--schema",
    default="THE",
    type=str,
    help="Specify the schema to use to identify dependencies.")
@click.option(
    "--out-format",
    type=click.Choice(
        ["json", "text"],
    ),
    default="text",
    help="Specify the output format to use to identify dependencies. Choices:\n"
         "  json: Output in JSON format.\n"
         "  text: Output in plain readable text format."
)
def show_deps(seed_table, schema, out_format):
    configure_logging()
    LOGGER.debug("seed tables: %s", seed_table)
    LOGGER.debug("schema: %s", schema)
    LOGGER.debug("out_format: %s", out_format)

    # get database connection parameters from the environment
    db_cons = constants.get_database_connection_parameters()
    ora = oralib.Oracle(db_cons)
    # get_related_tables_sa will get all dependencies including other tables
    # that have foreign key constraints to the specified seed table and vise
    # versa.
    tabs = ora.get_related_tables_sa(
            table_name=seed_table, schema=schema.upper())

    if out_format == "text":
        LOGGER.debug("got here")
        # LOGGER.debug("tabs: %s", tabs)
        text = tabs.to_str()
        print(text)
    elif out_format == "json":
        out_dict = tabs.to_dict()
        tabs_json = json.dumps(out_dict, indent=4)
        click.echo(tabs_json)

@click.command()
@click.option(
    "--seed-table",
    multiple=False,
    required=True,
    help="Specify the seed table to use to identify dependencies.")
@click.option(
    "--schema",
    default="THE",
    type=str,
    help="Specify the schema to use to identify dependencies.")
@click.option(
    "--migration-folder",
    type=str,
    required=False,
    help="Directory where the migration file will be created, defaults to the "
         "data/migrations directory")
@click.option(
    "--migration-version",
    type=str,
    required=False,
    default="1.0.0",
    help="The version of the migration file to create, defaults to 1.0.0")
@click.option(
    "--migration-name",
    type=str,
    required=False,
    default="first_migration",
    help="The name to append to the version number of the migration file that "
         "is being generated by this script. \n defaults to first_migration")

def create_migrations(seed_table, schema, migration_folder, migration_version, migration_name):
    configure_logging()
    if not migration_folder:
        migrations_folder = pathlib.Path(__file__).parent / "data" / "migrations"

    db_cons = constants.get_database_connection_parameters()
    ora = oralib.Oracle(db_cons)
    tabs = ora.get_related_tables_sa(table_name=seed_table, schema=schema)

    # create migrations from the table relationship structure
    # no intelligence at the moment, but down the road may want to add the
    # ability to increment the version number based on the existing migration

    # get existing migration files
    # extract the tables that are defined in those files
    # add those tables to the ora objects exported_tables properties

    initial_migration_version = packaging.version.parse(migration_version)

    current_migration_file = migration_files.MigrationFile(
        version=initial_migration_version,
        description=migration_name,
        migration_folder=migrations_folder,
    )
    existing_migration_files = current_migration_file.get_existing_migration_files()
    LOGGER.debug("existing_migration_files: %s", existing_migration_files)

    existing_tables = []
    for migration_file in existing_migration_files:
        migration_file_parser = migration_files.MigrationFileParser(migration_file)
        tables = migration_file_parser.get_tables()
        LOGGER.debug("tables: %s", tables)
        ora.exported_tables.add_tables(tables)
    # add the existing tables to the ora object so that it doesn't generate
    # duplicate migrations
    # ora.exported_tables.add_tables(tables=existing_tables, schema='THE')

    migrations_list = ora.create_migrations(
            tabs,
        )
    # now write migrations to a migration file
    current_migration_file.write_migrations(migrations_list)



@click.group()
def cli():
    pass

cli.add_command(show_deps)
cli.add_command(create_migrations)

if __name__ == "__main__":
    cli()

    # get_dependencies()


    # migrations folder

    # Getting the dependencies for a seed table by querying the foreign key
    # constraints
    # db_cons = constants.get_database_connection_parameters()
    # ora = oralib.Oracle(db_cons)
    # tabs = ora.get_related_tables_sa(table_name="SEEDLOT", schema="THE")

    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(tabs)

    # # # create migrations from the table relationship structure
    # initial_migration_version = packaging.version.parse("1.0.0")
    # current_migration_file = migration_files.MigrationFile(
    #     version=initial_migration_version,
    #     description="first_migration",
    #     migration_folder=migrations_folder,
    # )

    # migrations_list = ora.create_migrations(
    #     tabs,
    # )

    # # now write migrations to a migration file
    # current_migration_file.write_migrations(migrations_list)
