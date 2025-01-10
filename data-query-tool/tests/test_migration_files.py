import logging
import pathlib
import shutil

from data_query_tool import migration_files, types

LOGGER = logging.getLogger(__name__)


def test_get_dependency(simple_migration_file):
    LOGGER.info("migration_file: %s", simple_migration_file)
    mg_file = migration_files.MigrationFileParser(simple_migration_file)
    deps = mg_file.get_dependency()
    LOGGER.info("deps: %s", deps)
    assert len(deps) == 2
    assert deps[0].object_type == types.ObjectType.TABLE
    assert deps[0].object_name == "BEC_ZONE_CODE"
    assert deps[0].object_schema == "THE"

    assert deps[1].object_type == types.ObjectType.SEQUENCE
    assert deps[1].object_name == "SAUD_SEQ"
    assert deps[1].object_schema == "THE"

    # assert deps[2].object_type == "TRIGGER"
    # assert deps[2].object_name == "SPR_SEEDLOT_AR_IUD_TRG"
    # assert deps[2].object_schema == "THE"


def test_get_dependencies_w_mult_idx(migration_file_w_multiple_idx):
    mg_file = migration_files.MigrationFileParser(migration_file_w_multiple_idx)
    deps = mg_file.get_dependency()


def test_get_dependencies_w_sequence(migration_file_w_trigger):
    mg_file = migration_files.MigrationFileParser(migration_file_w_trigger)
    deps = mg_file.get_dependency()


def test_init_migration_folder():
    test_folder = pathlib.Path(__file__).parent / "test_migration_folder"
    LOGGER.debug("test_folder: %s", test_folder)
    migration = migration_files.MigrationFile(
        version="9.9.9",
        description="test_migration",
        migration_folder_str=str(test_folder),
    )
    assert migration.migration_folder.exists()
    shutil.rmtree(migration.migration_folder)

    relative_folder = "testfolder/data"
    LOGGER.debug("relative_folder: %s", relative_folder)
    migration = migration_files.MigrationFile(
        version="9.9.9",
        description="test_migration",
        migration_folder_str=relative_folder,
    )
    assert migration.migration_folder.exists()
    # cleanup the remnants of the test
    shutil.rmtree(migration.migration_folder)
    shutil.rmtree(migration.migration_folder.parent)
