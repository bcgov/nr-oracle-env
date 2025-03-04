import logging
import pathlib

import app_paths
import constants
import env_config

LOGGER = logging.getLogger(__name__)


def test_get_parquet_directory_ostore_path():
    """
    Verify the object store path for parquet files is correct.
    """
    env = env_config.Env("TEST")
    app_paths_obj = app_paths.AppPaths(env)
    ostore_dir = app_paths_obj.get_export_ostore_path(
        constants.DBType.SPAR,
    )
    LOGGER.debug("ostore_dir: %s", ostore_dir)
    assert ostore_dir
    assert ostore_dir == pathlib.Path("pyetl", constants.DBType.SPAR.name)
