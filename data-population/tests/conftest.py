import logging
import os
import pathlib
import sys

import pytest

faker_loger = logging.getLogger("faker.factory")
faker_loger.setLevel(logging.CRITICAL)

LOGGER = logging.getLogger(__name__)

# setting up the paths for the tests so they can import local modules
back_one_path = pathlib.Path(__file__).resolve().parents[1]
print("back_one_path: %s", back_one_path)
sys.path.append(str(back_one_path))

db_env_dir = back_one_path / "db_env_utils"
sys.path.append(str(db_env_dir))

sys.path.append(os.path.join(os.path.dirname(__file__), "."))


LOGGER = logging.getLogger(__name__)

pytest_plugins = [
    "fixtures.demo_fixtures",
    "fixtures.db_connection_params",
    "fixtures.db_fixtures",
    "fixtures.db_ora_extractor",
    "fixtures.duckdb_fixtures",
    "fixtures.dataclassification_fixtures",
]

testSession = None
