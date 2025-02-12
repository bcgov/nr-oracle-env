"""

Extract data from the oracle database and cache in parquet files in object storage.

Start the docker database
-------------------------
docker compose up oracle-migrations

Start the VPN
-----------------------
start the VPN to allow access to the database

Resolve WSL / VPN network issues
--------------------------------
different by computer / I run the following powershell commands:
    Get-NetAdapter | Where-Object {$_.InterfaceDescription -Match "Cisco AnyConnect"} | Set-NetIPInterface -InterfaceMetric 6000
    Get-NetIPInterface -InterfaceAlias "vEthernet (WSL)" | Set-NetIPInterface -InterfaceMetric 1

Create / Activate the poetry environment
----------------------------------------
poetry install - to create
source $(poetry env info --path)/bin/activate - to activate

Populate the following environment variables
--------------------------------------------
ORACLE_USER - user to connect to the database with
ORACLE_PASSWORD - password for that user
ORACLE_HOST - host for the database
ORACLE_PORT - port for the database
ORACLE_SERVICE - database service

Run the script
--------------
python data_prep/pull_ora_objstr.py

:return: _description_
:rtype: _type_


reference: https://www.andrewvillazon.com/quickly-load-data-db-python/

"""  # noqa: E501

import logging
import logging.config
import pathlib
import sys
from concurrent import futures  # noqa: F401

import click
import constants
import main_common

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument(
    "source",
    type=click.Choice(
        ["SPAR", "ORA"],
        case_sensitive=False,
    ),
    required=True,
)
@click.argument(
    "environment",
    type=click.Choice(
        ["TEST", "PROD"],
        case_sensitive=False,
    ),
    required=True,
)
@click.option(
    "--refresh", is_flag=True, help="Refresh the environment configuration."
)
def main(source, environment, refresh):
    """
    Extract data a SPAR database.

    Source:

        * SPAR  - Extract data from the SPAR postgres database hosted on oc.
        * ORA   - Extract data from the ORACLE database hosted on prem.

    Environment:

        * TEST - Extract data from the test environment.
        * PROD - Extract data from the production environment.

    --refresh: set this flag if you want to purge and recreate local and remote
               (object store) cached data.
    """
    global LOGGER
    environment = environment.upper()  # Ensure uppercase for consistency
    click.echo(f"Selected environment: {environment}")

    # db_type = constants.DBType.ORA
    db_type = constants.DBType[source]  # Convert string to enum value

    common_util = main_common.Utility(environment, db_type)
    common_util.configure_logging()
    logger_name = pathlib.Path(__file__).stem
    LOGGER = logging.getLogger(logger_name)

    if refresh:
        click.echo("Refresh flag is enabled. Refreshing configuration...")
    else:
        click.echo("Refresh flag is not enabled.")

    LOGGER.debug("refresh: %s %s", refresh, type(refresh))
    common_util.run_extract(refresh=refresh)


if __name__ == "__main__":
    if len(sys.argv) == 1:  # No arguments provided
        sys.argv.append("--help")  # Force help text if no args provided
    main()

    # # dealing with args
    # # NOTE: if this gets more complex use a CLI framework
    # env_str = "TEST"
    # if len(sys.argv) > 1:
    #     env_str = sys.argv[1]

    # common_util = main_common.Utility(env_str, constants.DBType.ORA)
    # common_util.configure_logging()
    # logger_name = pathlib.Path(__file__).stem
    # LOGGER = logging.getLogger(logger_name)
    # LOGGER.debug("log message in main")
    # common_util.run_extract()
