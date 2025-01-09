import logging

LOGGER = logging.getLogger(__name__)


def test_db_connection_params(ora_lib_fixture):
    """
    Verify that the we can connect to the database.
    """
    test_query = "SELECT 'TEST' FROM DUAL"
    cur = ora_lib_fixture.connection.cursor()
    cur.execute(test_query)
    results = cur.fetchall()
    cur.close()
    LOGGER.debug("results: %s", results)
    assert ("TEST",) in results


def test_get_triggers(ora_lib_fixture):
    """
    Test the get_related_tables_sa method.
    """
    ora = ora_lib_fixture
    LOGGER.debug("got here")
    LOGGER.debug("ora: %s", ora)
    results = ora_lib_fixture.get_triggers(schema="THE", table_name="SEEDLOT")
    assert results
    assert results[0][0] == "SPR_SEEDLOT_AR_IUD_TRG"


def test_get_trigger_deps(ora_lib_fixture):
    ora = ora_lib_fixture
    results = ora_lib_fixture.get_trigger_deps(schema="THE", table_name="SEEDLOT")
    LOGGER.debug("dependencies: %s", results)
