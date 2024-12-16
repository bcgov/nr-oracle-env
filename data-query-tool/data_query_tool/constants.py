"""
Constants for the module.

Constants used by the module, as well as functions to populate constants from
the environment.

"""

import logging
import os

from . import types

LOGGER = logging.getLogger(__name__)


def get_database_connection_parameters() -> types.ConnectionParameters:
    """
    Get a connection to the database.

    """
    connection_params = types.ConnectionParameters(
        username=os.getenv("ORACLE_USERNAME"),
        password=os.getenv("ORACLE_PASSWORD"),
        host=os.getenv("ORACLE_HOST"),
        port=os.getenv("ORACLE_PORT"),
        service_name=os.getenv("ORACLE_SERVICE_NAME"),
    )
    LOGGER.debug(
        "retrieved connection parameters to the db %s@%s:%s",
        connection_params.service_name,
        connection_params.host,
        connection_params.port,
    )
    return connection_params
