import logging
import pathlib

import db_env_utils.oradb_lib as oradb_lib
import duckdb
import pytest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def duckdb_database_path():
    test_ddb_path = (
        pathlib.Path(__file__).parent
        / ".."
        / "test_data"
        / "age_class_code.ddb"
    )
    if not test_ddb_path.exists():
        raise FileNotFoundError(
            f"Test database file does not exist: {test_ddb_path}"
        )
    LOGGER.debug("duckdb path: %s", test_ddb_path)
    yield test_ddb_path


@pytest.fixture(scope="module")
def duckdb_spatial_empty():
    """
    Return duckdb that returns empty spatial table in the ddb util object."""

    ddb_path = (
        pathlib.Path(__file__).parent
        / ".."
        / "test_data"
        / "sample_spatial.ddb"
    )
    if ddb_path.exists():
        LOGGER.debug("delete the sampe_spatial.ddb file, and will recreate")
        ddb_path.unlink()

    table_name = "SAMPLE_SPATIAL"
    conn = duckdb.connect(str(ddb_path))
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    create_table_sql = (
        f"CREATE TABLE {table_name} (ID INTEGER, SPATIAL_COLUMN GEOMETRY);"
    )
    conn.execute(create_table_sql)
    insert_data_query = f"""
        INSERT INTO {table_name} (ID, SPATIAL_COLUMN)
        VALUES (1, ST_GeomFromText('POINT(1 1)')),
            (2, ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'));
    """
    conn.execute(insert_data_query)
    conn.close()

    ddb_util = oradb_lib.DuckDbUtil(
        ddb_path,
        table_name,
    )
    yield ddb_util

    ddb_util.close()
    ddb_path.unlink()


@pytest.fixture(scope="module")
def duckdbutil_non_spatial(duckdb_database_path) -> oradb_lib.DuckDbUtil:
    """
    Return duckdb that returns age class codes table in the ddb util object."""
    table_name = "AGE_CLASS_CODE"
    ddb_util = oradb_lib.DuckDbUtil(
        duckdb_database_path,
        table_name,
    )
    yield ddb_util
