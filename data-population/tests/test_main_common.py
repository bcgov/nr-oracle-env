import logging

import constants
import main_common

LOGGER = logging.getLogger(__name__)


def test_get_tables_from_spar():
    """
    Verify tables can be extracted from the local spar database.
    """
    util = main_common.Utility("TEST", constants.DBType.SPAR)
    table_list = util.get_tables()
    assert table_list
    assert table_list

    expected_tables = [
        "SEEDLOT_REGISTRATION_A_CLASS_SAVE",
        "USER_PROFILE",
        "SEEDLOT_OWNER_QUANTITY",
        "ACTIVE_ORCHARD_SPU",
        "CONE_COLLECTION_METHOD_LIST",
        "GENETIC_CLASS_LIST",
        "GENETIC_WORTH_LIST",
        "GAMETIC_METHODOLOGY_LIST",
        "METHOD_OF_PAYMENT_LIST",
        "SEEDLOT_STATUS_LIST",
        "SEEDLOT_COLLECTION_METHOD",
        "FAVOURITE_ACTIVITY",
        "SEEDLOT_SOURCE_LIST",
        "SEEDLOT_GENETIC_WORTH",
        "SEEDLOT_PARENT_TREE",
        "SEEDLOT_PARENT_TREE_GEN_QLTY",
        "SEEDLOT_PARENT_TREE_SMP_MIX",
        "SMP_MIX",
        "SMP_MIX_GEN_QLTY",
        "SEEDLOT_SMP_MIX",
        "SEEDLOT",
        "SEEDLOT_AUDIT",
        "SEEDLOT_SEED_PLAN_ZONE",
        "SEEDLOT_ORCHARD",
        "ETL_EXECUTION_MAP",
        "ETL_EXECUTION_LOG",
        "ETL_EXECUTION_SCHEDULE",
        "ETL_EXECUTION_LOG_HIST",
    ]
    for expect_table in expected_tables:
        LOGGER.debug("expected table: %s", expect_table)
        assert expect_table in table_list
