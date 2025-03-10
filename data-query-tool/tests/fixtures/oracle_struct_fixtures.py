"""
Assumes that the oracle database is running, fixtures
to help provide connectivity to oracle.
"""

import logging
import os
import pathlib

import dotenv
import oracledb
import pytest
from data_query_tool import constants, oralib, oralib2, types

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def oracle_params():
    LOGGER.debug("getting connection parameters")
    cur_path = pathlib.Path(__file__).parent
    env_path = (cur_path / ".." / ".." / ".." / ".env").resolve()
    dotenv.load_dotenv(env_path)
    LOGGER.debug("the path is: %s", env_path.resolve())


@pytest.fixture(scope="module")
def ora_lib_fixture(oracle_params):
    db_cons = constants.get_database_connection_parameters()
    LOGGER.debug("db cons: %s", db_cons)
    ora = oralib.Oracle(db_cons)
    yield ora
    ora.connection.close()


@pytest.fixture(scope="module")
def ora_lib2_fixture(oracle_params):
    db_cons = constants.get_database_connection_parameters()
    LOGGER.debug("db cons: %s", db_cons)
    ora = oralib2.Oracle(db_cons)
    yield ora
    ora.connection.close()


@pytest.fixture(scope="module")
def type_dependency():
    db_dep = types.Dependency(
        object_type=types.ObjectType.TYPE,
        object_name="SIL_ERROR_PARAMS",
        object_schema="THE",
    )
    yield db_dep
