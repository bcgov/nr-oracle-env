import logging

from data_query_tool import migration_files

LOGGER = logging.getLogger(__name__)


def test_get_dependency(migration_files_list):
    for cur_mig_file in migration_files_list:
        LOGGER.info("migration_file: %s", cur_mig_file)
        mg_file = migration_files.MigrationFileParser(cur_mig_file)
        deps = mg_file.get_dependency()
        LOGGER.info("deps: %s", deps)
        assert len(deps) == 3
        assert deps[0].object_type == "TABLE"
        assert deps[0].object_name == "BEC_ZONE_CODE"
        assert deps[0].object_schema == "THE"

        assert deps[1].object_type == "SEQUENCE"
        assert deps[1].object_name == "SAUD_SEQ"
        assert deps[1].object_schema == "THE"

        assert deps[2].object_type == "TRIGGER"
        assert deps[2].object_name == "SPR_SEEDLOT_AR_IUD_TRG"
        assert deps[2].object_schema == "THE"
