import logging
import pathlib

import db_env_utils.app_paths
import db_env_utils.oradb_lib
import pytest

LOGGER = logging.getLogger(__name__)


def create_extractor_and_export(table_name, table_schema_name, ora):
    export_file = (
        pathlib.Path(__file__).parent
        / ".."
        / "test_data"
        / f"{table_name.lower()}.ddb"
    )
    LOGGER.debug("file 2 extract: %s", export_file)

    extractor = db_env_utils.oradb_lib.Extractor(
        table_name=table_name,
        db_schema=table_schema_name,
        oradb=ora,
        export_file=export_file,
    )
    return [extractor, export_file]


@pytest.fixture(scope="module")
def extractor_inst(docker_connection_params_ora, db_connection_fixture):
    ora = db_connection_fixture
    conn_params = docker_connection_params_ora
    table_name = "AGE_CLASS_CODE"
    extract_list = create_extractor_and_export(
        table_name,
        conn_params.schema_to_sync,
        ora,
    )
    yield extract_list


@pytest.fixture(scope="module")
def extractor_inst_spatial(docker_connection_params_ora, db_connection_fixture):
    ora = db_connection_fixture
    conn_params = docker_connection_params_ora
    table_name = "FOREST_COVER_GEOMETRY"
    extract_list = create_extractor_and_export(
        table_name,
        conn_params.schema_to_sync,
        ora,
    )
    yield extract_list


@pytest.fixture(scope="module")
def extractor_inst_spatial_TEST(
    ora_TEST_db_connection_params_from_env, ora_TEST_env
):
    conn_params = ora_TEST_db_connection_params_from_env
    cur_app_paths = db_env_utils.app_paths.AppPaths(ora_TEST_env)

    table_name = "FOREST_COVER_GEOMETRY"
    db_schema = conn_params.schema_to_sync

    ora = db_env_utils.oradb_lib.OracleDatabase(
        connection_params=conn_params, app_paths=cur_app_paths
    )
    extract_list = create_extractor_and_export(
        table_name,
        conn_params.schema_to_sync,
        ora,
    )
    yield extract_list


@pytest.fixture(scope="module")
def extractor_inst_application_PROD(
    ora_PROD_db_connection_params_from_env, ora_PROD_env
):
    conn_params = ora_PROD_db_connection_params_from_env
    cur_app_paths = db_env_utils.app_paths.AppPaths(ora_PROD_env)

    table_name = "APPLICATION"
    db_schema = conn_params.schema_to_sync

    ora = db_env_utils.oradb_lib.OracleDatabase(
        connection_params=conn_params, app_paths=cur_app_paths
    )
    extract_list = create_extractor_and_export(
        table_name,
        conn_params.schema_to_sync,
        ora,
    )
    yield extract_list
