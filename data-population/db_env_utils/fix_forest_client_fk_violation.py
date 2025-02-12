"""
Identify and delete postgres seedlot table records with fk violations.

Identifies records in postgres seedlot table that have client references that
do not exist in test.

Once we have the client_number/location codes (from this script) that cause
the issues, we can change them to ones that exist in TEST.

Example:
update seedlot
set
    applicant_client_number = '00149081',
    applicant_locn_code = '22'
where
    applicant_client_number = '00196805' and
    applicant_locn_code = '02';

Alternatively, we can delete the records that have the fk violations but that
can result in removing a lot of tables due to fk constraints.

"""

import logging

import constants
import docker_parser
import env_config
import main_common
import oradb_lib
import postgresdb_lib

LOGGER = logging.getLogger(__name__)


class PostgresSeedlot:
    """Interfact to postgres data."""

    def __init__(self) -> None:
        """
        Database constructor.
        """
        self.db = None
        self.db_connect()

    def db_connect(self) -> None:
        """
        Connect to the database.
        """
        dcr = docker_parser.ReadDockerCompose()
        LOGGER.debug("connecting to database... ")
        local_db_params = dcr.get_spar_conn_params()
        self.db = postgresdb_lib.PostgresDatabase(local_db_params)

    def get_seedlot_fc_records(self) -> set:
        """
        Get unique list of client_number and location code from seedlot table.

        :return: a list of tuples containing client_number and location code.
        :rtype: set
        """
        query = """
        select
            distinct
                applicant_client_number,
                applicant_locn_code
        from
            spar.seedlot
        """
        LOGGER.debug("query: %s", query)
        self.db.get_connection()
        cursor = self.db.connection.cursor()
        cursor.execute(query)
        cur_list = cursor.fetchall()
        return set(cur_list)


class OracleSeedlot:
    """
    Interface to oracle data.
    """

    def __init__(self) -> None:
        """
        Oracle constructor.
        """
        self.db = None
        self.env_obj = env_config.Env("TEST")
        self.db_connect()

    def db_connect(self) -> None:
        """
        Create a connection to the oracle database.
        """
        dcr = docker_parser.ReadDockerCompose()

        local_db_params = dcr.get_ora_conn_params()

        self.db = oradb_lib.OracleDatabase(local_db_params)
        self.db.schema_to_sync = self.env_obj.get_schema_to_sync()

    def get_seedlot_fc_records(self) -> None:
        """
        Get unique list of client_number and location code from seedlot table.
        """
        query = """
        select
            distinct
                CLIENT_NUMBER,
                CLIENT_LOCN_CODE
        from
            the.CLIENT_LOCATION
        """
        self.db.get_connection()
        cursor = self.db.connection.cursor()
        cursor.execute(query)
        cur_list = cursor.fetchall()
        return set(cur_list)


if __name__ == "__main__":
    # only using to setup logging
    util = main_common.Utility("TEST", constants.DBType.SPAR)
    util.configure_logging()
    LOGGER.setLevel(logging.DEBUG)

    pgs = PostgresSeedlot()
    pg_distinct = pgs.get_seedlot_fc_records()

    oras = OracleSeedlot()
    ora_distinct = oras.get_seedlot_fc_records()
    records_to_delete = pg_distinct - ora_distinct

    match_cnt = 0
    for record in pg_distinct:
        if record in ora_distinct:
            match_cnt += 1
        else:
            LOGGER.debug("record to delete: %s", record)
    LOGGER.debug("match count: %s", match_cnt)
    LOGGER.debug("pg distinct count: %s", len(pg_distinct))
    LOGGER.debug("records to delete: %s", records_to_delete)
