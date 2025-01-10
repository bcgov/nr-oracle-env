import logging
import pathlib

import pytest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def migration_files_list(data_dir) -> list[pathlib.Path]:
    migration_file_dir = pathlib.Path(data_dir, "test_data")
    LOGGER.debug("migration_file_dir: %s", migration_file_dir)
    migration_files = list(migration_file_dir.glob("*.sql"))
    LOGGER.debug("migration_files: %s", migration_files)
    for migration_file in migration_files:
        LOGGER.debug("migration_file: %s", migration_file)
    yield migration_files
