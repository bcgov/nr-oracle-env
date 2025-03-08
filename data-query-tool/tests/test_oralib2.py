import logging
import pathlib

import packaging
import sqlalchemy
from data_query_tool import migration_files, types

LOGGER = logging.getLogger(__name__)


def test_db_connection_params(ora_lib2_fixture):
    """
    Verify that the we can connect to the database.
    """
    test_query = "SELECT 'TEST' FROM DUAL"
    cur = ora_lib2_fixture.connection.cursor()
    cur.execute(test_query)
    results = cur.fetchall()
    cur.close()
    LOGGER.debug("results: %s", results)
    assert ("TEST",) in results


def test_get_table_deps(ora_lib2_fixture):
    """
    Test the get_table_deps method.
    """
    # HARVESTING_HAULING_XREF HARVESTING_AUTHORITY APPRAISAL_DATA_SUBMISSION
    # APPRAISAL_DATA_SUBMISSION
    table_name = "APPRAISAL_DATA_SUBMISSION"
    schema = "THE"
    result = ora_lib2_fixture.get_db_object_deps(
        object_name=table_name,
        object_type=types.ObjectType.TABLE,
        schema=schema,
    )
    LOGGER.debug("result: %s", result)
    assert result.object_name == table_name
    assert result.object_schema == schema
    assert result.object_type == types.ObjectType.TABLE
    LOGGER.debug("result.dependency_list: %s", result.dependency_list)
    LOGGER.debug("\n" + result.to_str())
    # assert len(result.dependency_list) == 11
    # assert result.dependency_list == []


def test_get_procedure_deps_1(ora_lib2_fixture):
    # MSD_TIMBER_MARK_SITES FTA_GET_FSJ_HVA_TRIGGER_INFO
    proc_name = "MSD_TIMBER_MARK_SITES"
    result = ora_lib2_fixture.get_deps(
        object_name=proc_name,
        schema="THE",
    )
    LOGGER.debug("result: \n%s", result.to_str())


def test_get_procedure_deps(ora_lib2_fixture):
    # LEXIS_FEE_IN_LIEU  MSD_TIMBER_MARK_SITES
    proc_name = "MSD_TIMBER_MARK_SITES"
    schema = "THE"
    result = ora_lib2_fixture.get_db_object_deps(
        object_name=proc_name,
        object_type=types.ObjectType.PACKAGE,
        schema=schema,
    )
    LOGGER.debug("result: %s", result)
    # assert result.object_name == proc_name
    # assert result.object_schema == schema
    # assert result.object_type == types.ObjectType.TABLE
    # LOGGER.debug("result.dependency_list: %s", result.dependency_list)
    LOGGER.debug("\n" + result.to_str())
    # assert len(result.dependency_list) == 11
    # assert result.dependency_list == []


def test_get_object_type(ora_lib2_fixture):
    object_name = "ORG_UNIT"
    schema = "THE"

    object_name = ora_lib2_fixture.get_object_type(
        object_name=object_name,
        schema=schema,
    )
    assert object_name == types.ObjectType.TABLE

    object_name = "CLIENT_FOREST_CLIENT"
    obj_type = ora_lib2_fixture.get_object_type(
        object_name=object_name,
        schema=schema,
    )
    assert obj_type == types.ObjectType.PACKAGE


def test_sa_connect(ora_lib2_fixture):
    ora_lib2_fixture.connect_sa()
    with ora_lib2_fixture.sa_engine.connect() as connection:
        result = connection.execute(
            sqlalchemy.text("SELECT CURRENT_TIMESTAMP"),
        )
        assert result


def test_create_migrations(ora_lib2_fixture):
    #  PROV_FOREST_USE HARVESTING_HAULING_XREF MSD_TIMBER_MARK_SITES
    #  prov_forest_use harv_haul_xr            msd_tmbr_mrk_st
    object_name = "MSD_TIMBER_MARK_SITES"
    schema = "THE"
    version = "2.0.0"
    object_type = types.ObjectType.PACKAGE
    migration_name = "msd_tmbr_mrk_st"

    deps = ora_lib2_fixture.get_db_object_deps(
        object_name=object_name, schema=schema, object_type=object_type
    )
    LOGGER.debug(deps.to_str())
    ddl_cache = ora_lib2_fixture.create_migrations(
        deps,
    )
    migrations_list = ddl_cache.get_ddl()
    # tests/test_data
    migration_folder = pathlib.Path(__file__).parent / "test_data"
    # now write migrations to a migration file
    current_migration_file = migration_files.MigrationFile(
        version=packaging.version.parse(version),
        description=migration_name,
        migration_folder_str=str(migration_folder),
    )

    current_migration_file.write_migrations(migrations_list)
