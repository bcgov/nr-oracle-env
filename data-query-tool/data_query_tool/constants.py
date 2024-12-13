
import logging
from . import types
import os
LOGGER = logging.getLogger(__name__)


def get_database_connection_parameters() -> types.ConnectionParameters:
    """
    Get a connection to the database.

    """
    connection_params = types.ConnectionParameters(
        username=os.getenv('ORACLE_USERNAME'),
        password=os.getenv('ORACLE_PASSWORD'),
        host=os.getenv('ORACLE_HOST'),
        port=os.getenv('ORACLE_PORT'),
        service_name=os.getenv('ORACLE_SERVICE_NAME')
    )
    return connection_params
