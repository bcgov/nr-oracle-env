"""
Test the docker_parser module.
"""

import logging

import data_prep.db_env_utils.docker_parser as docker_parser

LOGGER = logging.getLogger(__name__)


def test_get_ora_conn_params() -> None:
    """
    Verify parameters can be extracted from the docker compose file.
    """
    dp = docker_parser.ReadDockerCompose()
    ora_params = dp.get_ora_conn_params()
    LOGGER.info(ora_params)
    LOGGER.info(ora_params.host)

    assert ora_params.username.upper() == "THE"
    assert ora_params.password == "default"
    assert ora_params.host == "localhost"
    assert ora_params.port == "1521"
    assert ora_params.service_name == "DBDOCK_01"


def test_get_spar_conn_params() -> None:
    """
    Verify docker compose parameters for local spar docker database.
    """
    dp = docker_parser.ReadDockerCompose()
    spar_params = dp.get_spar_conn_params()

    assert spar_params.username == "postgres"
    assert spar_params.password == "default"
    assert spar_params.host == "localhost"
    assert str(spar_params.port) == "5432"
    assert spar_params.service_name == "spar"
