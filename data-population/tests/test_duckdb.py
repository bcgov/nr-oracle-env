import logging

import constants

LOGGER = logging.getLogger(__name__)


def test_get_columns(duckdbutil_non_spatial):
    LOGGER.debug("test_get_columns")
    columns = duckdbutil_non_spatial.get_columns()

    expected_columns = [
        "age_class_code",
        "description",
        "effective_date",
        "expiry_date",
        "update_timestamp",
    ]
    expected_columns.sort()
    columns.sort()

    assert columns == expected_columns

    # LOGGER.debug("spatial: %s", spatial)
    # assert not spatial
    # pass


def test_spatial_columns(duckdb_spatial_empty, duckdbutil_non_spatial):
    LOGGER.debug("test_spatial_columns")
    spatial = duckdb_spatial_empty.get_spatial_columns()
    assert spatial

    ns_cols = duckdbutil_non_spatial.get_spatial_columns()
    assert not ns_cols

    # LOGGER.debug("spatial: %s", spatial)
    # assert not spatial
    # pass
