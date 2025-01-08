import logging

LOGGER = logging.getLogger(__name__)

def test_get_related_tables(oracle_conn):
    """
    Verify the object store path for parquet files is correct.
    """
    LOGGER.debug("got here")
