"""
fixtures to help with database tests
"""

import datetime
import logging
import pathlib

import db_env_utils.app_paths as app_paths
import db_env_utils.env_config as env_config
import db_env_utils.oradb_lib as oradb_lib
import numpy as np
import pandas
import pytest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def app_paths_fixture():
    env = env_config.Env("PROD")
    # TODO: should mock this up further so that the app paths point to test data
    app_path_obj = app_paths.AppPaths(env)
    yield app_path_obj


@pytest.fixture(scope="module")
def db_connection_fixture(docker_connection_params_ora, app_paths_fixture):
    ora = oradb_lib.OracleDatabase(
        connection_params=docker_connection_params_ora,
        app_paths=app_paths_fixture,
    )
    yield ora


@pytest.fixture(scope="module")
def test_table_name_fixture():
    yield "TEST_TABLE"


@pytest.fixture(scope="module")
def with_clob_raw_tab(db_connection_fixture, test_table_name_fixture):
    # create the table
    table_name = test_table_name_fixture
    schema = None
    if "." in table_name:
        parsed_table_name = table_name.split(".")
        table_name = parsed_table_name[1]
        schema = parsed_table_name[0]
    if not schema:
        schema = "THE"
    query = """
    SELECT TABLE_NAME FROM ALL_TABLES
    WHERE
        TABLE_NAME = :table_name and
        OWNER = :schema
    """
    db_connection_fixture.get_connection()
    cursor = db_connection_fixture.connection.cursor()
    cursor.execute(query, table_name=table_name, schema=schema)
    rows = cursor.fetchall()
    LOGGER.debug(rows)

    if (not rows) and len(rows) == 0:
        create_table = f"""
            CREATE TABLE {schema}.{table_name} (
                    opening_attachment_file_id NUMBER(19),
                    opening_id NUMBER(19),
                    attachment_name CLOB,
                    attachment_description CLOB,
                    mime_type_code CLOB,
                    attachment_data CLOB,
                    entry_userid CLOB,
                    entry_timestamp DATE,
                    update_userid CLOB,
                    update_timestamp DATE,
                    revision_count NUMBER(19),
                    opening_attachment_guid CLOB
            )"""
        cursor.execute(create_table)
    cursor.close()

    yield db_connection_fixture


@pytest.fixture(scope="module")
def clob_data_2_load():
    # create a parquet file with some data in it to load

    row1 = {
        "opening_attachment_file_id": np.int64(182934),
        "opening_id": np.int64(-241390000),
        "attachment_name": "82N054_147_VISUAL_2021_FG_RPF.pdf",
        "attachment_description": "2021_SU_FG_PLOT",
        "mime_type_code": "PDF",
        "attachment_data": None,
        "entry_userid": "IDIR\\KASTOUT",
        "entry_timestamp": pandas.Timestamp("2022-02-25 15:10:51"),
        "update_userid": "IDIR\\KASTOUT",
        "update_timestamp": pandas.Timestamp("2022-02-25 15:10:51"),
        "revision_count": np.int64(1),
        "opening_attachment_guid": "0c7ce8ef17ba2fa7e06332b3228edb4f",
    }
    row2 = {
        "opening_attachment_file_id": np.int64(182948),
        "opening_id": np.int64(-241490000),
        "attachment_name": "82N054_155_FG_2021_FG_NETOUT_RPF.pdf",
        "attachment_description": "2021_SU_FG_PLOT",
        "mime_type_code": "PDF",
        "attachment_data": None,
        "entry_userid": "IDIR\\KASTOUT",
        "entry_timestamp": pandas.Timestamp("2022-02-25 15:45:24"),
        "update_userid": "IDIR\\KASTOUT",
        "update_timestamp": pandas.Timestamp("2022-02-25 15:45:24"),
        "revision_count": np.int64(1),
        "opening_attachment_guid": "0c7ce8ef17bb2fa7e06332b3228edb4f",
    }
    row3 = {
        "opening_attachment_file_id": np.int64(183522),
        "opening_id": np.int64(1758673),
        "attachment_name": "2022 FCI Stella 2022-01.pdf",
        "attachment_description": "Fertilization Prescription",
        "mime_type_code": "PDF",
        "attachment_data": None,
        "entry_userid": "BCEID\\BSHAWWFP",
        "entry_timestamp": pandas.Timestamp("2022-03-04 12:07:45"),
        "update_userid": "BCEID\\BSHAWWFP",
        "update_timestamp": pandas.Timestamp("2022-03-04 12:07:45"),
        "revision_count": np.int64(1),
        "opening_attachment_guid": "0c7ce8ef17bc2fa7e06332b3228edb4f",
    }
    row4 = {
        "opening_attachment_file_id": np.int64(196049),
        "opening_id": np.int64(121053),
        "attachment_name": "93B029-170 Report Combined.pdf",
        "attachment_description": "2018 Survey",
        "mime_type_code": "PDF",
        "attachment_data": None,
        "entry_userid": "IDIR\\ASCHAAD",
        "entry_timestamp": pandas.Timestamp("2023-07-20 15:24:35"),
        "update_userid": "IDIR\\ASCHAAD",
        "update_timestamp": pandas.Timestamp("2023-07-20 15:24:35"),
        "revision_count": np.int64(1),
        "opening_attachment_guid": "0c7ce8ef3ed32fa7e06332b3228edb4f",
    }

    df = pandas.DataFrame([row1, row2, row3, row4])
    LOGGER.debug("dataframe columns: %s", df.columns)
    yield df


@pytest.fixture(scope="module")
def clob_parquet_file(clob_data_2_load):
    parquet_file = (
        pathlib.Path(__file__).parent / ".." / "test_data" / "test.parquet"
    )
    if parquet_file.exists():
        LOGGER.debug("deleting the parquet file: %s", parquet_file)
        parquet_file.unlink(parquet_file)

    clob_data_2_load.to_parquet(path=parquet_file)
    yield parquet_file
