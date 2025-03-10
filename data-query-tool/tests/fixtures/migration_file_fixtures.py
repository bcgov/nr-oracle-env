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


@pytest.fixture(scope="module")
def simple_migration_file(data_dir) -> pathlib.Path:
    migration_file = pathlib.Path(data_dir, "test_data", "test_migration.sql")
    yield migration_file


@pytest.fixture(scope="module")
def migration_file_w_trigger(data_dir) -> pathlib.Path:
    migration_file = pathlib.Path(data_dir, "test_data", "test_migration2.sql")
    yield migration_file


@pytest.fixture(scope="module")
def migration_file_w_multiple_idx(data_dir) -> pathlib.Path:
    migration_file = pathlib.Path(data_dir, "test_data", "test_migration3.sql")
    yield migration_file


@pytest.fixture(scope="module")
def migration_file_triggers(data_dir) -> pathlib.Path:
    migration_file = pathlib.Path(
        data_dir, "test_data", "V1.0.2__first_migration_TRG.sql"
    )
    yield migration_file


@pytest.fixture(scope="module")
def migration_file_packages(data_dir) -> pathlib.Path:
    migration_file = pathlib.Path(
        data_dir, "test_data", "V1.0.1__first_migration_PKG.sql"
    )
    yield migration_file


@pytest.fixture(scope="module")
def migration_file_types(data_dir) -> pathlib.Path:
    migration_file = pathlib.Path(
        data_dir,
        "test_data",
        "V2.1.2__msd_tmbr_mrk_st_TYP.sql",
    )
    yield migration_file


@pytest.fixture(scope="module")
def migration_file_w_view(data_dir) -> pathlib.Path:
    migration_file = pathlib.Path(
        data_dir,
        "test_data",
        "V1.3.0__msd_tmbr_mrk_st.sql",
    )
    yield migration_file
