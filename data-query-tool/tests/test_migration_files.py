"""
Tests for the migration_files module.

Nowhere near a reasonable code coverage, mostly used to support development.
"""

import logging
import pathlib
import shutil

import packaging.version
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
    LOGGER.info("deps: %s", deps)


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


def test_init_migration_folder():
    test_folder = pathlib.Path(__file__).parent / "test_migration_folder"
    test_folder.mkdir(parents=True, exist_ok=True)
    for i in range(1, 15):
        file_name = f"V1.0.{i}__dummy_migration.sql"
        pathlib.Path(test_folder / file_name).touch()

    migration = migration_files.MigrationFile(
        version=packaging.version.Version("1.0.0"),
        description="test_migration",
        migration_folder_str=test_folder,
    )
    new_file = migration.get_migration_file()
    LOGGER.debug("new_file: %s", new_file)
    assert new_file.name == "V1.1.0__test_migration.sql"
    LOGGER.debug("new_file: %s", new_file)

    shutil.rmtree(test_folder)


def test_get_next_version_file():
    test_folder = pathlib.Path(__file__).parent / "test_migration_folder"

    file_names = []
    for i in range(1, 15):
        file_name = f"V1.0.{i}__dummy_migration.sql"
        file_names.append(pathlib.Path(file_name))

    migration = migration_files.MigrationFile(
        version=packaging.version.Version("1.0.0"),
        description="test_migration",
        migration_folder_str=test_folder,
    )

    next_version = migration.get_next_version_file(file_names)
    LOGGER.debug("next_version: %s", next_version)
    assert next_version == packaging.version.Version("1.1.0")


def test_get_migration_type(
    migration_file_triggers, migration_file_w_multiple_idx
):
    mg_file = migration_files.MigrationFileParser(migration_file_triggers)
    mg_type = mg_file.get_migration_type()
    assert mg_type == types.DDLType.TRIGGER

    mg_file = migration_files.MigrationFileParser(migration_file_w_multiple_idx)
    mg_type = mg_file.get_migration_type()
    assert mg_type == types.DDLType.DB_OBJ_DDL


def test_get_dependency_triggers(migration_file_triggers):
    mg_file = migration_files.MigrationFileParser(migration_file_triggers)
    mg_type = mg_file.get_migration_type()
    assert mg_type == types.DDLType.TRIGGER
    deps = mg_file.get_dependency()

    LOGGER.debug("deps: %s", deps)

    assert len(deps) == 3
    expect_dep1 = types.Dependency(
        object_name="RESULTS_CBOA_AR_IUD_TRG",
        object_type=types.ObjectType.TRIGGER,
        object_schema="THE",
    )
    expect_dep2 = types.Dependency(
        object_name="FTA_CPFILL_CBOA",
        object_type=types.ObjectType.TRIGGER,
        object_schema="THE",
    )
    expect_dep3 = types.Dependency(
        object_name="RESULTS_JUNK_TRIG",
        object_type=types.ObjectType.TRIGGER,
        object_schema="THE",
    )

    not_in = types.Dependency(
        object_name="DUMMY",
        object_type=types.ObjectType.TRIGGER,
        object_schema="THE",
    )
    assert expect_dep1 in deps
    assert expect_dep2 in deps
    assert expect_dep3 in deps
    assert not_in not in deps


def test_get_dependency_packages(migration_file_packages):
    mg_file = migration_files.MigrationFileParser(migration_file_packages)
    mg_type = mg_file.get_migration_type()
    assert mg_type == types.DDLType.PACKAGE
    deps = mg_file.get_dependency()
    LOGGER.debug("deps: %s", deps)

    assert len(deps) == 5

    expect_dep1 = types.Dependency(
        object_name="RESULTS_OPENING",
        object_type=types.ObjectType.PACKAGE,
        object_schema="THE",
    )
    expect_dep2 = types.Dependency(
        object_name="RESULTS_AUDIT",
        object_type=types.ObjectType.PACKAGE,
        object_schema="THE",
    )
    expect_dep3 = types.Dependency(
        object_name="RESULTS_GLOBALS",
        object_type=types.ObjectType.PACKAGE,
        object_schema="THE",
    )
    expect_dep4 = types.Dependency(
        object_name="PKG_SIL_DATE_CONVERSION",
        object_type=types.ObjectType.PACKAGE,
        object_schema="THE",
    )
    expect_dep5 = types.Dependency(
        object_name="SIL_DATE_CONVERSION",
        object_type=types.ObjectType.PACKAGE,
        object_schema="THE",
    )
    not_in = types.Dependency(
        object_name="SIL_DATE_CONVERSION2",
        object_type=types.ObjectType.PACKAGE,
        object_schema="THE",
    )
    not_in2 = types.Dependency(
        object_name="SIL_DATE_CONVERSION",
        object_type=types.ObjectType.TRIGGER,
        object_schema="THE",
    )

    assert expect_dep1 in deps
    assert expect_dep2 in deps
    assert expect_dep3 in deps
    assert expect_dep4 in deps
    assert expect_dep5 in deps
    assert not_in not in deps
    assert not_in2 not in deps


def test_get_migration_type(
    migration_file_triggers,
    migration_file_packages,
    migration_file_w_multiple_idx,
):
    mg_file = migration_files.MigrationFileParser(migration_file_triggers)
    mg_type = mg_file.get_migration_type()
    assert mg_type == types.DDLType.TRIGGER

    mg_file = migration_files.MigrationFileParser(migration_file_packages)
    mg_type = mg_file.get_migration_type()
    assert mg_type == types.DDLType.PACKAGE

    mg_file = migration_files.MigrationFileParser(migration_file_w_multiple_idx)
    mg_type = mg_file.get_migration_type()
    assert mg_type == types.DDLType.DB_OBJ_DDL


def test_get_dependency_types(migration_file_types):
    mg_file = migration_files.MigrationFileParser(migration_file_types)
    mg_type = mg_file.get_migration_type()
    LOGGER.debug("migration file type: %s", mg_type)
    # DDLType.DB_OBJ_DDL
    # assert mg_type == types.DDLType.DB_TYPES

    deps = mg_file.get_dependency()
    LOGGER.debug("deps: %s", deps)


def test_get_dependency_views(migration_file_w_view):
    mg_file = migration_files.MigrationFileParser(migration_file_w_view)
    mg_type = mg_file.get_migration_type()
    LOGGER.debug("migration file type: %s", mg_type)
    # DDLType.DB_OBJ_DDL
    # assert mg_type == types.DDLType.DB_TYPES

    deps = mg_file.get_dependency()
    LOGGER.debug("deps: %s", deps)
