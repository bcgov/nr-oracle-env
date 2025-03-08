"""
Start script to run the data query tool.
"""

import json
import logging.config
import pathlib

import click
import packaging.version
from data_query_tool import constants, migration_files, oralib, oralib2, types


def configure_logging() -> logging.Logger:
    """
    Configure the logging for the cli.
    """
    log_conf_path = pathlib.Path(__file__).parent / "logging.config"
    logging.config.fileConfig(log_conf_path)
    logger = logging.getLogger(__name__)
    logger.debug("testing logger config...")
    return logger


LOGGER = configure_logging()


@click.group()
def cli() -> None:
    """
    Create a cli object that can be added to for the click interface.
    """
    LOGGER.debug("created a cli objects")


@click.command()
@click.option(
    "--seed-object",
    multiple=False,
    required=True,
    help="Specify the seed object to use to identify dependencies.",
)
@click.option(
    "--type",
    type=click.Choice([otype.name for otype in types.ObjectType]),
    default="TAB",
    required=True,
    help="Specify the object type specified in the seed-object parameter.",
)
@click.option(
    "--schema",
    default="THE",
    type=str,
    help="Specify the schema to use to identify dependencies.",
)
@click.option(
    "--out-format",
    type=click.Choice(
        ["json", "text"],
    ),
    default="text",
    help="Specify the output format to use to identify dependencies. Choices:\n"
    "  json: Output in JSON format.\n"
    "  text: Output in plain readable text format.",
)
def show_deps(
    seed_object: str,
    type: str,
    schema: str,
    out_format: str,
) -> None:
    """
    Show a table or packages dependencies.

    :param seed_object: name of the seed database object to retrieve
        dependencies for.
    :type seed_object: str
    :param type: the database object type.
    :type seed_package: str
    :param schema: the schema in which the seed table resides.
    :type schema: str
    :param out_format: the output format to use to display the dependencies.
        (json, text)
    :type out_format: str
    """
    configure_logging()
    LOGGER.debug("seed object: %s", seed_object)
    LOGGER.debug("seed object type: %s", type)
    LOGGER.debug("schema: %s", schema)
    LOGGER.debug("out_format: %s", out_format)
    LOGGER.info(
        "have patience, working on getting dependencies for the table: %s",
        seed_object,
    )

    # get database connection parameters from the environment
    db_cons = constants.get_database_connection_parameters()
    ora = oralib2.Oracle(db_cons)
    # get_related_tables_sa will get all dependencies including other tables
    # that have foreign key constraints to the specified seed table and vise
    # versa.
    if type in [otype.name for otype in types.ObjectType]:
        deps = ora.get_db_object_deps(
            object_name=seed_object,
            schema=schema.upper(),
            object_type=types.ObjectType[type],
        )
    else:
        msg = f"you specified a --type {type} which is not supported."
        LOGGER.debug(msg)
        raise ValueError(msg)

    if out_format == "text":
        text = deps.to_str()
        print(text)  # noqa: T201
    elif out_format == "json":
        out_dict = deps.to_dict()
        deps_json = json.dumps(out_dict, indent=4)
        click.echo(deps_json)


@click.command()
@click.option(
    "--seed-table",
    multiple=False,
    required=True,
    help="Specify the seed table to use to identify dependencies.",
)
@click.option(
    "--schema",
    default="THE",
    type=str,
    help="Specify the schema to use to identify dependencies.",
)
@click.option(
    "--migration-folder",
    type=str,
    required=False,
    help="Directory where the migration file will be created, defaults to the "
    "data/migrations directory",
    default="data/migrations",
)
@click.option(
    "--migration-version",
    type=str,
    required=False,
    default="1.0.0",
    help="The version of the migration file to create, defaults to 1.0.0",
)
@click.option(
    "--migration-name",
    type=str,
    required=False,
    default="first_migration",
    help="The name to append to the version number of the migration file that "
    "is being generated by this script. \n defaults to first_migration",
)
def create_migrations(
    seed_table: str,
    schema: str,
    migration_folder: str,
    migration_version: str,
    migration_name: str,
) -> None:
    """
    Create a migration file for a table and its dependencies.

    :param seed_table: The name of the table to generate a migration for.
    :type seed_table: str
    :param schema: The schema of the table to generate a migration for.
    :type schema: str
    :param migration_folder: The folder to write the migration file to.
    :type migration_folder: str
    :param migration_version: The name of the version for the migration file.
        In most cases you want to not use this parameter and rely on the script
        to identify the next version number based on the existing migration
        files that are found the migration_folder.
    :type migration_version: str
    :param migration_name: A description string for the migration file.  This
        string is appended to the version number of the migration file and
        suffixed by .sql.
    :type migration_name: str
    """
    configure_logging()
    if not migration_folder:
        migration_folder = pathlib.Path(__file__).parent / "data" / "migrations"

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


cli.add_command(show_deps)
cli.add_command(create_migrations)

ALIASES = {
    "sd": show_deps,
    "cm": create_migrations,
}

if __name__ == "__main__":
    cli()
