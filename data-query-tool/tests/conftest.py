import logging
import os
import pathlib
import sys

import pytest

file_dir = pathlib.Path(__file__).parent

sys.path.append(str((file_dir / "..").resolve()))
sys.path.append(str(file_dir))

print("directory: ", file_dir)
print("abs path %s", file_dir.resolve())
print("sys.path: ", sys.path)

LOGGER = logging.getLogger(__name__)
LOGGER.info("project root dir: %s", file_dir)

from data_query_tool import constants

pytest_plugins = [
    "fixtures.oracle_struct_fixtures",
    "fixtures.migration_file_fixtures",
    "fixtures.generics_fixtures",
]
