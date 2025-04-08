"""
Fixtures used to support testing of docker db.
"""

import logging
import os
import pathlib

import db_env_utils.env_config as env_config
import dotenv
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
    conn_params.port = 1523
    conn_params.service_name = "dbdock_test_01"
    conn_params.schema_to_sync = "the"
    yield conn_params


@pytest.fixture(scope="module")
def ora_TEST_env():
    """
    Connection parameters for docker database.
    """
    dotenvFile = pathlib.Path(__file__).parent / ".." / ".." / ".env"
    dotenv.load_dotenv(dotenv_path=dotenvFile)
    LOGGER.debug("ora host for test: %s", os.getenv("ORACLE_HOST_TEST", "junk"))
    env = env_config.Env("TEST")
    yield env


@pytest.fixture(scope="module")
def ora_PROD_env():
    """
    Connection parameters for docker database.
    """
    dotenvFile = pathlib.Path(__file__).parent / ".." / ".." / ".env"
    dotenv.load_dotenv(dotenv_path=dotenvFile)
    LOGGER.debug("ora host for test: %s", os.getenv("ORACLE_HOST_PROD", "junk"))
    env = env_config.Env("PROD")
    yield env


@pytest.fixture(scope="module")
def ora_TEST_db_connection_params_from_env(ora_TEST_env):
    """
    Connection parameters for docker database.
    """
    conn_params = ora_TEST_env.get_ora_db_env_constants()
    yield conn_params


@pytest.fixture(scope="module")
def ora_PROD_db_connection_params_from_env(ora_PROD_env):
    """
    Connection parameters for docker database.
    """
    conn_params = ora_PROD_env.get_ora_db_env_constants()
    yield conn_params
