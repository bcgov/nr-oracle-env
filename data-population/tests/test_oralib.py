import logging
import os
import pathlib
import re

import constants
import env_config
import geopandas
import oracledb
import oradb_lib
import pandas

LOGGER = logging.getLogger(__name__)


def test_get_triggers_with_sequences(docker_connection_params_ora):
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


def test_raw_clob_data(
    with_clob_raw_tab,
    clob_data_2_load,
    clob_parquet_file,
    test_table_name_fixture,
):
    ora = with_clob_raw_tab
    ora.load_data_with_raw_blobby(
        table=test_table_name_fixture, import_file=clob_parquet_file, purge=True
    )

    cur = ora.connection.cursor()
    query = (
        f"Select count(*) from {ora.schema_2_sync}.{test_table_name_fixture}"
    )
    LOGGER.debug("query: %s", query)
    cur.execute(query)
    data = cur.fetchall()
    LOGGER.debug("rows returned: %s", data)
    # data will not show up if you do not commit the connection
    # ora.connection.commit()


def test_opening_attachement_load(db_connection_fixture):
    ora = db_connection_fixture
    # override the chunk size used for reading from parquet.
    # chunks determine how many rows are written at a time.
    # ora.ora_cur_arraysize = 5
    # ora.chunk_size = 5
    # THE.OPENING_ATTACHMENT STOCKING_STANDARD_UNIT
    # HARVESTING_AUTHORITY PROV_FOREST_USE HAULING_AUTHORITY
    table = "STOCKING_STANDARD_UNIT"
    parquet_file = f"/home/kjnether/fsa_proj/nr-fsa-orastruct/data-population/data/PROD/ORA/{table}.parquet"
    ora.load_data_with_raw_blobby(
        table=table,
        import_file=parquet_file,
        purge=True,
    )


def test_get_pyarrow_schema(db_connection_fixture):
    ora = db_connection_fixture
    table_name = "STOCKING_STANDARD_UNIT"
    table_obj = ora.get_table_object(table_name)
    for column in table_obj.columns:
        LOGGER.debug(
            "column name: %s column type: %s",
            column.name,
            column.type.python_type,
        )
    pyarrow_schema = ora.get_pyarrow_schema(table_obj)
    LOGGER.debug("pyarrow_schema: %s", pyarrow_schema)


def test_parquet_from_duck(db_connection_fixture):
    ora = db_connection_fixture

    table_name = "FOREST_COVER_GEOMETRY"

    output_parquet_file = (
        pathlib.Path(__file__).parent / "test_data" / f"{table_name}_T.parquet"
    )

    import duckdb

    database_file = pathlib.Path(
        pathlib.Path(__file__).parent / "test_data" / "junk.duckdb"
    )
    # database_file = "/home/kjnether/fsa_proj/nr-fsa-orastruct/data-population/tests/test_data/junk.duckdb"
    ddb_con = duckdb.connect(database=database_file)
    ddb_con.install_extension("spatial")
    ddb_con.load_extension("spatial")
    # ST_GeomFromText
    query = f"""
        COPY (
            select forest_cover_id, ST_GeomFromWKB(GEOMETRY) AS GEOMETRY,
              feature_area, feature_perimeter, capture_method_code,
                data_source_code, feature_class_skey, observation_date,
                  data_quality_comment, entry_userid, entry_timestamp,
                    update_userid, update_timestamp, revision_count
                    FROM my_table)
                    TO '{output_parquet_file}' (FORMAT PARQUET);
    """
    ddb_con.sql(query)
    ddb_con.close()

    is_geo = ora.is_geoparquet(output_parquet_file)
    LOGGER.debug("is geoparquet: %s", is_geo)

    spatial_column = ora.get_geoparquet_spatial_column(output_parquet_file)
    LOGGER.debug(f"spatial_column: {spatial_column}")


def test_extract_data(db_connection_on_prem_prod_fixture):
    ora = db_connection_on_prem_prod_fixture
    table_name = "FOREST_COVER_GEOMETRY"

    output_parquet_file = (
        pathlib.Path(__file__).parent / "test_data" / f"{table_name}.parquet"
    )
    if output_parquet_file.exists():
        LOGGER.debug("nuke the test parquet file: %s", output_parquet_file)
        output_parquet_file.unlink()
    # parquet_file = f"/home/kjnether/fsa_proj/nr-fsa-orastruct/data-population/data/PROD/ORA/{table}.parquet"

    # ora.extract_data(
    #     table=table_name,
    #     export_file=output_parquet_file,
    #     chunk_size=100,
    #     max_records=300,
    # )

    ora.extract_data_sdogeometry_ddb(
        table=table_name,
        export_file=output_parquet_file,
        chunk_size=100,
        max_records=300,
    )
    is_geo = ora.is_geoparquet(output_parquet_file)
    LOGGER.debug("is geoparquet: %s", is_geo)

    # verify the file
    # df = pandas.read_parquet(str(output_parquet_file))
    # LOGGER.debug("schema of parquet: %s", df.dtypes.to_dict())
    gdf = geopandas.read_file(output_parquet_file, ignore_geometry=True)

    # row count
    # LOGGER.debug("len(df.index): %s", len(df.index))


def test_debug_load_problem_tables(db_connection_fixture):
    ora = db_connection_fixture
    table = "FOREST_COVER_GEOMETRY"
    parquet_file = f"/home/kjnether/fsa_proj/nr-fsa-orastruct/data-population/data/PROD/ORA/{table}.parquet"
    # parquet_file = f"/home/kjnether/Downloads/{table}.parquet"
    # parquet_file = str(
    #     pathlib.Path(__file__).parent / "test_data" / f"{table}.parquet"
    # )

    # table_obj = ora.get_table_object(table)
    # pyarrow_schema = ora.get_pyarrow_schema(table_obj)

    # LOGGER.debug("df columns: %s", df.columns)

    from pyarrow.parquet import ParquetFile

    # ParquetFile(source).metadata
    LOGGER.debug("schema from file: %s", ParquetFile(parquet_file).schema)

    # NET_AREA
    # df.query()
    # LOGGER.debug("")
    ora.load_data(table=table, import_file=parquet_file, purge=True)


def test_load_raw_data(db_connection_fixture):
    db_connection_fixture.get_connection()
    conn = db_connection_fixture.connection
    row = [
        1,
        -631760000,
        "bednesti north Block Summaries 2002.xls",
        "bednesti north Block Summaries",
        "XLS",
        "IDIR\\ADELLAVI",
        pandas.Timestamp("2004-07-23 10:04:10"),
        "IDIR\\ADELLAVI",
        pandas.Timestamp("2004-07-23 10:04:28"),
        2,
        b'\x0c|\xe8\xec\x7f\xd5/\xa7\xe0c2\xb3"\x8e\xdbO',
        # None,
    ]
    # b'\x0c|\xe8\xec\x7f\xd5/\xa7\xe0c2\xb3"\x8e\xdbO',

    row2 = [
        2,
        -631760000,
        "Bednesti-2003.pdf",
        "bednesti PDF document",
        "PDF",
        "IDIR\\ADELLAVI",
        pandas.Timestamp("2004-07-23 10:06:26"),
        "IDIR\\ADELLAVI",
        pandas.Timestamp("2004-07-23 10:06:28"),
        1,
        b'\x0c|\xe8\xec\x7f\xd6/\xa7\xe0c2\xb3"\x8e\xdbO',
        # None,
    ]

    # u_string = codecs.decode(b_string, 'utf-8')
    # oacle AL16UTF16
    import codecs

    # LOGGER.debug("decode string: %s", codecs.decode(row2[11], "utf-16"))
    LOGGER.debug("raw data row 1: %s", row[10].hex())
    LOGGER.debug("raw data row 2: %s %s", row2[10].hex(), type(row2[10]))

    hex_data = "0C7CE8EC7FD52FA7E06332B3228EDB4F"
    raw_data = bytes.fromhex(hex_data)
    LOGGER.debug("raw_data, %s", raw_data)

    "0c7ce8ec7fd52fa7e06332b3228edb4f"
    LOGGER.debug(
        "back to bytestring: %s",
        bytes.fromhex("0c7ce8ec7fd52fa7e06332b3228edb4f"),
    )

    # INSERT INTO your_table (blob_column, text_column)
    #     VALUES (EMPTY_BLOB(), :1)
    #     RETURNING blob_column INTO :blob_var

    ins = """ \
    INSERT INTO OPENING_ATTACHMENT
    (
        opening_attachment_file_id,
        opening_id, attachment_name,
        attachment_description,
        mime_type_code,
        entry_userid,
        entry_timestamp,
        update_userid,
        update_timestamp,
        revision_count,
        opening_attachment_guid,
        attachment_data)
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, EMPTY_BLOB() )
    """
    #    RETURNING blob_column INTO :blob_var

    with conn.cursor() as cursor:
        blob_var = cursor.var(oracledb.BLOB)
        # blob_vars = [blob_var for _ in [row]]

        cursor.executemany(ins, [row])


def test_enable_integrity_constraint(db_connection_fixture):
    ora = db_connection_fixture
    ora.get_connection()
    cons_list = ora.get_fk_constraints()
    LOGGER.debug("cons_list length: %s", len(cons_list))
    ora.enable_constraints(cons_list)


def test_load_sdo_data(db_connection_fixture):
    ora = db_connection_fixture
    # try code table
    # OPENING_ATTACHMENT FOREST_COVER_GEOMETRY SILV_COMMENT_SOURCE_CODE OPENING_COMMENT_LINK
    # CLIENT_ID_TYPE_CODE CLIENT_LOCATION
    table = "OPENING"

    cons = ora.get_fk_constraints()
    ora.disable_fk_constraints(cons)
    ora.truncate_table(table=table)

    parquet_file = (
        pathlib.Path(__file__).parent
        / ".."
        / "data"
        / "PROD"
        / "ORA"
        / f"{table}.ddb"
    )
    # parquet_file = (
    #     pathlib.Path(__file__).parent / "test_data" / f"{table}.parquet"
    # )
    LOGGER.debug("source file: %s", parquet_file)
    ora.get_connection()

    ora.load_data(table=table, import_file=parquet_file)


def test_debug_data_insert(db_connection_fixture):
    ora = db_connection_fixture
    # try code table
    # OPENING_ATTACHMENT FOREST_COVER_GEOMETRY SILV_COMMENT_SOURCE_CODE OPENING_COMMENT_LINK
    table = "FOREST_CLIENT"
    ora.truncate_table(table=table)
    from pandas import Timestamp

    data = [
        (
            "00000001",
            "MANAGEMENT ABEYANCE",
            None,
            None,
            "DAC",
            "C",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Timestamp("1989-11-26 08:51:50"),
            "CONV",
            70.0,
            Timestamp("2023-02-13 16:21:39"),
            "IDIR\\PWEHRELL",
            70.0,
            3.0,
        )
    ]

    data = (
        "00001003",
        "ABBOTT",
        "Matthew",
        "Eric",
        "ACT",
        "I",
        "1935-08-08 00:00:00.00",
        "ABDL",
        "688 052 315",
        None,
        None,
        None,
        None,
        None,
        None,
        "1989-11-26 00:51:52",
        "CONV",
        70.0,
        "1997-11-07 07:47:27",
        "CLIADMIN",
        70.0,
        1.0,
    )

    LOGGER.debug("length of data: %s", len(data))
    cur = ora.connection.cursor()
    stmt = "INSERT INTO FOREST_CLIENT (CLIENT_NUMBER, CLIENT_NAME, LEGAL_FIRST_NAME, LEGAL_MIDDLE_NAME, CLIENT_STATUS_CODE, CLIENT_TYPE_CODE, BIRTHDATE, CLIENT_ID_TYPE_CODE, CLIENT_IDENTIFICATION, REGISTRY_COMPANY_TYPE_CODE, CORP_REGN_NMBR, CLIENT_ACRONYM, WCB_FIRM_NUMBER, OCG_SUPPLIER_NMBR, CLIENT_COMMENT, ADD_TIMESTAMP, ADD_USERID, ADD_ORG_UNIT, UPDATE_TIMESTAMP, UPDATE_USERID, UPDATE_ORG_UNIT, REVISION_COUNT) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22)"
    # date_time_between_dates
    cur.executemany(stmt, data)
