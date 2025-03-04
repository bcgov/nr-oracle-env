"""
Fixtures used to support testing of docker db.
"""

import logging

import db_env_utils.env_config as env_config
import pytest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def docker_connection_params():
    """
    Connection parameters for docker database.
    """
    conn_params = env_config.ConnectionParameters
    conn_params.username = "postgres"
    # ignore security warning for this... its a dev database that gets spun up
    # only to support local dev.
    conn_params.password = "default"  # NOSONAR
    conn_params.host = "localhost"
    conn_params.port = 5432
    conn_params.service_name = "spar"
    conn_params.schema_to_sync = "spar"
    yield conn_params


@pytest.fixture(scope="module")
def docker_connection_params_ora():
    conn_params = env_config.ConnectionParameters
    conn_params.username = "the"
    # ignore security warning for this... its a dev database that gets spun up
    # only to support local dev.
    conn_params.password = "default"  # NOSONAR
    conn_params.host = "localhost"
    conn_params.port = 1521
    conn_params.service_name = "dbdock_01"
    conn_params.schema_to_sync = "the"
    yield conn_params
