import logging
import os
import pathlib
import re

import constants
import env_config
import oradb_lib

LOGGER = logging.getLogger(__name__)


def test_get_triggers_with_sequnces(docker_connection_params_ora):
    """
    Verify sequences can be fixed.
    """
    LOGGER.debug("docker_connection_params: %s", docker_connection_params_ora)
    db = oradb_lib.OracleDatabase(docker_connection_params_ora)
    fix_seq = oradb_lib.FixOracleSequences(db)

    LOGGER.debug("db: %s %s", db, type(db))
    trig_seq_list = fix_seq.get_triggers_with_sequences()
    LOGGER.debug("triggers/sequences: %s", trig_seq_list)
    for trig_seq in trig_seq_list:
        LOGGER.debug("trig_seq: %s", trig_seq)
        column = fix_seq.get_trigger_sequence_column(
            trig_seq["trigger_name"],
            trig_seq["owner"],
        )
        LOGGER.debug("column: %s", column)
    assert True


def test_regextest(docker_connection_params_ora):
    trigger_body = """
      DECLARE
        v_spar_audit_code    seedlot_owner_quantity_audit.spar_audit_code%TYPE;
      BEGIN
        IF INSERTING THEN
          v_spar_audit_code := 'I';
        ELSIF UPDATING THEN
          v_spar_audit_code := 'U';
        ELSE
          v_spar_audit_code := 'D';
        END IF;

        IF INSERTING OR UPDATING THEN
          --Put the new row into the audit table
          INSERT INTO seedlot_owner_quantity_audit (
                  soq_audit_id
                , audit_date
                , spar_audit_code
                , seedlot_number
                , client_number
                , client_locn_code
                , original_pct_owned
                , original_pct_rsrvd
                , original_pct_srpls
                , qty_reserved
                , qty_rsrvd_cmtd_pln
                , qty_rsrvd_cmtd_apr
                , qty_surplus
                , qty_srpls_cmtd_pln
                , qty_srpls_cmtd_apr
                , authorization_code
                , method_of_payment_code
                , spar_fund_srce_code)
              VALUES (
                  soqa_seq.NEXTVAL
                , SYSDATE
                , v_spar_audit_code
                , :NEW.seedlot_number
                , :NEW.client_number
                , :NEW.client_locn_code
                , :NEW.original_pct_owned
                , :NEW.original_pct_rsrvd
                , :NEW.original_pct_srpls
                , :NEW.qty_reserved
                , :NEW.qty_rsrvd_cmtd_pln
                , :NEW.qty_rsrvd_cmtd_apr
                , :NEW.qty_surplus
                , :NEW.qty_srpls_cmtd_pln
                , :NEW.qty_srpls_cmtd_apr
                , :NEW.authorization_code
                , :NEW.method_of_payment_code
                , :NEW.spar_fund_srce_code);
        ELSE
          --DELETING: Put the last row into the audit table before deleting
          INSERT INTO seedlot_owner_quantity_audit (
                  soq_audit_id
                , audit_date
                , spar_audit_code
                , seedlot_number
                , client_number
                , client_locn_code
                , original_pct_owned
                , original_pct_rsrvd
                , original_pct_srpls
                , qty_reserved
                , qty_rsrvd_cmtd_pln
                , qty_rsrvd_cmtd_apr
                , qty_surplus
                , qty_srpls_cmtd_pln
                , qty_srpls_cmtd_apr
                , authorization_code
                , method_of_payment_code
                , spar_fund_srce_code)
              VALUES (
                  soqa_seq.NEXTVAL
                , SYSDATE
                , v_spar_audit_code
                , :OLD.seedlot_number
                , :OLD.client_number
                , :OLD.client_locn_code
                , :OLD.original_pct_owned
                , :OLD.original_pct_rsrvd
                , :OLD.original_pct_srpls
                , :OLD.qty_reserved
                , :OLD.qty_rsrvd_cmtd_pln
                , :OLD.qty_rsrvd_cmtd_apr
                , :OLD.qty_surplus
                , :OLD.qty_srpls_cmtd_pln
                , :OLD.qty_srpls_cmtd_apr
                , :OLD.authorization_code
                , :OLD.method_of_payment_code
                , :OLD.spar_fund_srce_code);
        END IF;
      END SOQ_AOU_TRG;
      """

    table_name = "seedlot_owner_quantity_audit"
    regex_exp = (
        rf"INSERT\s+INTO\s+\w*.*{table_name}\s*\(.*?\)\s*VALUES\s*\(.*?\);"
    )
    db = oradb_lib.OracleDatabase(docker_connection_params_ora)
    fix_seq = oradb_lib.FixOracleSequences(db)

    inserts = fix_seq.extract_inserts(trigger_body, table_name)
    assert len(inserts) == 1
    # LOGGER.debug("inserts: %s", inserts)
    LOGGER.debug("number of inserts: %s", len(inserts))
    for insert in inserts:
        column = fix_seq.extract_sequence_column(insert, table_name)
        LOGGER.debug("column: %s", column)
        assert column.lower() == "soq_audit_id"


def test_max_row(docker_connection_params_ora):
    """
    Verify max rowid can be found.
    """
    LOGGER.debug("docker_connection_params: %s", docker_connection_params_ora)
    db = oradb_lib.OracleDatabase(docker_connection_params_ora)
    fix_seq = oradb_lib.FixOracleSequences(db)

    max_rowid = fix_seq.get_max_value(
        schema="the",
        table="seedlot_owner_quantity_audit",
        column="soq_audit_id",
    )
    LOGGER.debug("max_rowid: %s", max_rowid)
    assert True


def test_get_sequence_nextval(docker_connection_params_ora):
    """
    Verify sequence nextval can be found.
    """
    LOGGER.debug("docker_connection_params: %s", docker_connection_params_ora)
    db = oradb_lib.OracleDatabase(docker_connection_params_ora)
    fix_seq = oradb_lib.FixOracleSequences(db)

    nextval = fix_seq.get_sequence_nextval(
        sequence_name="SAUD_SEQ", sequence_owner="THE"
    )
    LOGGER.debug("nextval: %s", nextval)
    assert True


def test_fix_sequences(docker_connection_params_ora):
    """
    Verify triggers can be fixed.
    """
    LOGGER.debug("docker_connection_params: %s", docker_connection_params_ora)
    db = oradb_lib.OracleDatabase(docker_connection_params_ora)
    fix_seq = oradb_lib.FixOracleSequences(dbcls=db)

    fix_seq.fix_sequences()
    assert True


def test_sdo_data_extract(docker_connection_params_ora):
    """
    Verify sdo data can be extracted.
    """
    LOGGER.debug("docker_connection_params: %s", docker_connection_params_ora)

    print(f"env var is: {os.getenv('MY_ENV_VAR')}")
    db_params = env_config.ConnectionParameters(
        username=os.getenv("ORACLE_USER_TEST"),
        password=os.getenv("ORACLE_PASSWORD_TEST"),
        host=os.getenv("ORACLE_HOST_TEST"),
        port=os.getenv("ORACLE_PORT_TEST"),
        service_name=os.getenv("ORACLE_SERVICE_TEST"),
        schema_to_sync=os.getenv("ORACLE_SCHEMA_TO_SYNC_TEST"),
    )

    db = oradb_lib.OracleDatabase(db_params)

    # Oracle connection details
    db.get_connection()

    db.extract_data_SDO_GEOMETRY(
        table="INVASIVE_PLANT_SUBMISSION",
        export_file="invas_plt_sub_spatial.parquet",
        overwrite=True,
    )


def test_data_load(docker_connection_params_ora):
    """
    Verify data can be loaded.
    """
    LOGGER.debug("docker_connection_params: %s", docker_connection_params_ora)
    db = oradb_lib.OracleDatabase(docker_connection_params_ora)
    # /home/kjnether/fsa_proj/nr-fsa-orastruct/data-population/oracle_data_spatial.parquet

    parquet_file = (
        "/home/kjnether/fsa_proj/nr-fsa-orastruct/invas_plt_sub_spatial.parquet"
    )
    db.load_data(
        table="INVASIVE_PLANT_SUBMISSION",
        import_file="invas_plt_sub_spatial.parquet",
        purge=False,
    )
    assert True


def test_is_parquet(docker_connection_params_ora):
    parquet_file = pathlib.Path(
        "/home/kjnether/fsa_proj/nr-fsa-orastruct/invas_plt_sub_spatial.parquet"
    )
    LOGGER.debug("docker_connection_params: %s", docker_connection_params_ora)
    db = oradb_lib.OracleDatabase(docker_connection_params_ora)
    is_geoparq = db.is_geoparquet(parquet_file)
    LOGGER.debug("is geoparq? :%s", is_geoparq)
    assert is_geoparq

    non_parq = pathlib.Path(
        "/home/kjnether/fsa_proj/nr-fsa-orastruct/data-population/data/TEST/ORA/ACTIVITY_TREATMENT_UNIT.parquet"
    )
    is_geoparq = db.is_geoparquet(non_parq)
    LOGGER.debug("is geoparq? :%s", is_geoparq)
    assert not is_geoparq
