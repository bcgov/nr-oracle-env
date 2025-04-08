import logging

import constants
import duckdb
import oradb_lib

LOGGER = logging.getLogger(__name__)


def test_extractor(extractor_inst):
    extract_obj = extractor_inst[0]
    extract_file = extractor_inst[1]
    if extract_file.exists():
        extract_file.unlink()
    extract_obj.extract(extract_file)

    conn = duckdb.connect(extract_file)
    table_name = extract_file.stem.upper()
    LOGGER.debug("table name: %s", table_name)
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    LOGGER.debug("row count: %s", row_count)
    assert row_count > 0, "Row count should be greater than 0"


def test_extractor_spatial(extractor_inst_spatial):
    extract_obj = extractor_inst_spatial[0]
    extract_file = extractor_inst_spatial[1]
    if extract_file.exists():
        extract_file.unlink()
    extract_obj.extract(extract_file)
    conn = duckdb.connect(extract_file)
    table_name = extract_file.stem.upper()
    LOGGER.debug("table name: %s", table_name)
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    LOGGER.debug("row count: %s", row_count)
    assert row_count > 0, "Row count should be greater than 0"


def test_env(extractor_inst_spatial_TEST):
    """
    Need the TEST env.

    :param extractor_inst_spatial_TEST: _description_
    :type extractor_inst_spatial_TEST: _type_
    """
    LOGGER.debug("env test")
    extract_obj = extractor_inst_spatial_TEST[0]
    extract_file = extractor_inst_spatial_TEST[1]
    if extract_file.exists():
        extract_file.unlink()

    extract_obj.extract(extract_file, chunk_size=100, max_records=500)

    conn = duckdb.connect(extract_file)
    table_name = extract_file.stem.upper()
    LOGGER.debug("table name: %s", table_name)
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    LOGGER.debug("row count: %s", row_count)
    assert row_count > 0, "Row count should be greater than 0"


def test_masking(extractor_inst, monkeypatch):
    extract_obj = extractor_inst[0]
    extract_file = extractor_inst[1]
    table_name = extract_file.stem.upper()

    mask_data = [
        constants.data_types.DataToMask(
            table_name=table_name,
            schema="THE",
            column_name="DESCRIPTION",
            faker_method="fake.words(nb=3)",
            percent_null=0,
        ),
    ]

    monkeypatch.setattr(constants, "DATA_TO_MASK", mask_data)

    # verify that the monkeypatch worked
    assert constants.DATA_TO_MASK[0].table_name == table_name
    extract_obj.log_mask_config()
    if extract_file.exists():
        extract_file.unlink()
    extract_obj.extract(extract_file)


def test_extractor_application(extractor_inst_application_PROD):
    extract_obj = extractor_inst_application_PROD[0]
    extract_file = extractor_inst_application_PROD[1]
    if extract_file.exists():
        extract_file.unlink()
    extract_obj.extract(extract_file)
    conn = duckdb.connect(extract_file)
    table_name = extract_file.stem.upper()
    LOGGER.debug("table name: %s", table_name)
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    LOGGER.debug("row count: %s", row_count)
    assert row_count > 0, "Row count should be greater than 0"
