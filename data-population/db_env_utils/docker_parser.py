"""
Parsing class for this projects docker compose file.

A class that makes it easy to extract database connection parameters from the
docker compose file.

"""

from __future__ import annotations

import logging
import os
import pathlib

import env_config
import yaml

LOGGER = logging.getLogger(__name__)


class ReadDockerCompose:
    """

    Methods to parse docker compose files.

    Utility methods to extract information from the docker compose file used for
    this project to create an ephemeral oracle database.

    """

    def __init__(self, compose_file_path: str | None = None) -> None:
        """
        Create object to parsedocker compose files.

        A series of methods to extract docker compose connection parameters for
        the oracle databases that have been spun up.

        :param compose_file_path: The path to the docker-compose to read, if no
            path is supplied assumes the docker-compose.yaml is in the directory
            that the script was executed from.
        :type compose_file_path: str, path
        """

        self.compose_file_path = self.get_composefile_path(compose_file_path)

        with pathlib.Path(self.compose_file_path).open("r") as fh:
            self.docker_comp = yaml.safe_load(fh)

    def get_composefile_path(self, comp_file_path: str) -> str:
        """
        Return the path to the docker-compose file.

        Expects the docker compose file to exist in either the current working
        or back a single directory relative to this script.

        :return: the path to the docker-compose file
        :rtype: str
        """
        if not comp_file_path:
            docker_comp_file_path = pathlib.Path(
                "./docker-compose.yml"
            ).resolve()
        else:
            docker_comp_file_path = pathlib.Path(comp_file_path)

        if not docker_comp_file_path.exists():
            LOGGER.debug(
                "1. docker compose file does not exist: %s",
                docker_comp_file_path,
            )

            # try to find the docker-compose file one file back from this files
            # path
            docker_comp_file_path = (
                pathlib.Path(__file__)
                .parent.joinpath("..", "docker-compose.yml")
                .resolve()
            )

            if not docker_comp_file_path.exists():
                LOGGER.debug(
                    "2. docker compose file does not exist: %s",
                    docker_comp_file_path,
                )
                # try going back one more directory
                docker_comp_file_path = (
                    pathlib.Path(__file__)
                    .parent.joinpath("..", "..", "docker-compose.yml")
                    .resolve()
                )
                if not docker_comp_file_path.exists():
                    LOGGER.debug(
                        "3. docker compose file does not exist: %s",
                        docker_comp_file_path,
                    )

                    # try going back one more directory
                    docker_comp_file_path = (
                        pathlib.Path(__file__)
                        .parent.joinpath(
                            "..",
                            "..",
                            "..",
                            "docker-compose.yml",
                        )
                        .resolve()
                    )

        if not docker_comp_file_path.exists():
            raise FileNotFoundError(
                "Could not find the docker-compose file in the current or parent directory, %s",
                docker_comp_file_path,
            )

        LOGGER.info(f"Using docker-compose file: {docker_comp_file_path}")
        return docker_comp_file_path

    def get_spar_conn_params(self) -> env_config.ConnectionParameters:
        """
        Return postgres connection parameters.

        Reads the postgres connection parameters from the docker-compose file
        and returns them as a oracledb.ConnectionTuple.

        :return: a oracledb.ConnectionTuple populated with the connection
            parameters
        :rtype: oradb_lib.ConnectionTuple
        """

        conn_tuple = env_config.ConnectionParameters
        conn_tuple.username = self.docker_comp["x-postgres-vars"][
            "POSTGRES_USER"
        ]
        conn_tuple.password = self.docker_comp["x-postgres-vars"][
            "POSTGRES_PASSWORD"
        ]
        conn_tuple.port = self.docker_comp["x-postgres-vars"]["POSTGRES_PORT"]

        conn_tuple.service_name = self.docker_comp["x-postgres-vars"][
            "POSTGRES_DB"
        ]
        # using localhost because the connection is going to be made external
        # to the docker container
        conn_tuple.host = os.getenv("POSTGRES_HOST", "localhost")
        conn_tuple.schema_to_sync = "spar"
        return conn_tuple

    def get_ora_conn_params(self) -> env_config.ConnectionParameters:
        """
        Return oracle connection parameters.

        Reads the oracle connection parameters from the docker-compose file and
        returns them as a oracledb.ConnectionTuple.

        :return: a oracledb.ConnectionTuple populated with the connection
            parameters
        :rtype: oradb_lib.ConnectionTuple
        """
        # extract variables from parsing the docker compose file
        dcr_user_name = self.docker_comp["x-oracle-vars"]["APP_USER"]
        dcr_user_password = self.docker_comp["x-oracle-vars"][
            "APP_USER_PASSWORD"
        ]
        dcr_port = self.docker_comp["services"]["oracle-spar"]["ports"][
            0
        ].split(":",)[0]
        dcr_service_name = self.docker_comp["x-oracle-vars"]["ORACLE_DATABASE"]

        # this is setup to facilitate development of this code.  It will first
        # check to see if the environment has been populated.  If it has not
        # then it will use the values that have been extracted from the docker
        # compose.  The one exception is host. When host (ORACLE_HOST) has not
        # been populated it will default to localhost.
        #
        # defaulting to localhost allows development of the script using the
        # docker database, when the script is NOT being executed from within
        # docker-compose!
        conn_tuple = env_config.ConnectionParameters
        conn_tuple.username = os.getenv("ORACLE_USER", dcr_user_name)
        conn_tuple.password = os.getenv("ORACLE_USER", dcr_user_password)
        conn_tuple.service_name = os.getenv("ORACLE_DATABASE", dcr_service_name)
        # this method will return the host for the docker container which is
        # expected to be running locally and is therefor 'localhost'
        conn_tuple.host = os.getenv("ORACLE_HOST", "localhost")
        conn_tuple.port = os.getenv("ORACLE_PORT", dcr_port)
        return conn_tuple
