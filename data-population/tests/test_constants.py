import logging
import pathlib

import constants

LOGGER = logging.getLogger(__name__)


def test_get_parquet_directory_ostore_path():
    """
    Verify the object store path for parquet files is correct.
    """
    ostore_dir = constants.get_export_ostore_path(
        constants.DBType.SPAR,
    )
    LOGGER.debug("ostore_dir: %s", ostore_dir)
    assert ostore_dir
    assert ostore_dir == pathlib.Path("pyetl", constants.DBType.SPAR.name)
