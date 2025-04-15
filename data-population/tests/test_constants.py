import logging
import os
import pathlib
from unittest import mock  # Added for mocking environment variables

import app_paths
import constants
import env_config

LOGGER = logging.getLogger(__name__)


def test_get_parquet_directory_ostore_path():
    """
    Verify the object store path for parquet files is correct.
    """
    mock_val = "dummy"
    # os.environ, {"OBJECT_STORE_DATA_DIRECTORY": "mockeddum_value"}
    env = env_config.Env("TEST")
    with mock.patch.object(constants, "OBJECT_STORE_DATA_DIRECTORY", mock_val):
        app_paths_obj = app_paths.AppPaths(env)
        ostore_dir = app_paths_obj.get_export_ostore_path(
            constants.DBType.OC_POSTGRES,
        )
        LOGGER.debug("ostore_dir: %s", ostore_dir)
        test_path = pathlib.Path(mock_val, constants.DBType.OC_POSTGRES.name)
        LOGGER.debug("test_path: %s", test_path)

        assert ostore_dir
        assert ostore_dir == test_path
