"""
Declare constants for the data_prep package.

"""

import datetime
import decimal
import logging
import os
import pathlib
from enum import Enum

import data_types
import numpy as np
import oracledb
import pyarrow
import sqlalchemy

LOGGER = logging.getLogger(__name__)

# the name of the directory where data downloads will be cached before they
# get uploaded to object store, and where they are cached when pulled from
# object store
DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data")

TEMP_DIR = os.getenv("TEMP_DIR", "temp")

# constraint backup directory
CONSTRAINT_BACKUP_DIR = "fk_constraint_backup"

# when running the script these are the different key words for describing
# different environments.
VALID_ENVS = ("DEV", "TEST", "PROD", "LOCAL")

PARQUET_SUFFIX = "parquet"
SQL_DUMP_SUFFIX = "sql.gz"
DUCKDB_TMP_FILE = "donald.duckdb"
DUCK_DB_MEM_LIM = "5GB"

# name of the directory in object store where the data backup files reside
OBJECT_STORE_DATA_DIRECTORY = os.getenv("OBJECT_STORE_DATA_DIRECTORY", "pyetl")

# database filter string
DB_FILTER_STRING = os.getenv("DB_FILTER_STRING", "nr-spar-{env_str}-databaseee")
# the port to use for the local port when establishing a port forward, and then
# for connecting to the database that is in kubernetes
DB_LOCAL_PORT = 5433


# database types, used to identify which database (oc_postgres or oracle) is
# to be worked with
class DBType(Enum):
    """
    Define the database types that are supported.

    An enumeration of the different database types that are supported by the
    scripts in this project.

    """

    ORA = 1
    OC_POSTGRES = 2


PYTHON_PYARROW_TYPE_MAP = {
    str: pyarrow.string(),
    int: pyarrow.int64(),
    float: pyarrow.float64(),
    decimal.Decimal: pyarrow.float64(),
    datetime.datetime: pyarrow.timestamp("s"),
    bytes: pyarrow.binary(),
}

DB_TYPE_PYARROW_MAP = {
    oracledb.DB_TYPE_VARCHAR: pyarrow.string(),
    oracledb.DB_TYPE_CHAR: pyarrow.string(),
    oracledb.DB_TYPE_NVARCHAR: pyarrow.string(),
    oracledb.DB_TYPE_NCHAR: pyarrow.string(),
    oracledb.DB_TYPE_CLOB: pyarrow.string(),
    oracledb.DB_TYPE_NUMBER: pyarrow.float64(),  # Can also be pyarrow.int64() if no decimals
    oracledb.DB_TYPE_BINARY_FLOAT: pyarrow.float32(),
    oracledb.DB_TYPE_BINARY_DOUBLE: pyarrow.float64(),
    oracledb.DB_TYPE_DATE: pyarrow.timestamp("s"),
    oracledb.DB_TYPE_TIMESTAMP: pyarrow.timestamp("ns"),
    oracledb.DB_TYPE_TIMESTAMP_LTZ: pyarrow.timestamp(
        "ns"
    ),  # Oracle's Local Time Zone
    oracledb.DB_TYPE_TIMESTAMP_TZ: pyarrow.timestamp("ns", tz="UTC"),
    oracledb.DB_TYPE_BLOB: pyarrow.binary(),
    oracledb.DB_TYPE_RAW: pyarrow.binary(),
    oracledb.DB_TYPE_LONG_RAW: pyarrow.binary(),
    oracledb.DB_TYPE_BOOLEAN: pyarrow.bool_(),
    oracledb.DB_TYPE_INTERVAL_DS: pyarrow.duration(
        "s"
    ),  # Interval Day to Second
    oracledb.DB_TYPE_INTERVAL_YM: pyarrow.int32(),  # Interval Year to Month (int is closest)
}


DATA_TO_MASK = [
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CLIENT_LOCN_NAME",
        faker_method="fake.words(nb=3)",
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="ADDRESS_1",
        faker_method="fake.address().split('\n')[0]",
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="ADDRESS_2",
        faker_method="fake.address().split('\n')[1].split(',')[0]",
        percent_null=80,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="ADDRESS_3",
        faker_method="fake.address().split('\n')[1].split(',')[1]",
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CITY",
        faker_method="fake.city()",
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="POSTAL_CODE",
        faker_method="fake.postalcode()",
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="HOME_PHONE",
        faker_method="''.join(filter(str.isdigit, fake.phone_number()))",
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CELL_PHONE",
        faker_method="''.join(filter(str.isdigit, fake.phone_number()))",
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="FAX_NUMBER",
        faker_method="''.join(filter(str.isdigit, fake.phone_number()))",
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="EMAIL_ADDRESS",
        faker_method="fake.ascii_email()",
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CLI_LOCN_COMMENT",
        faker_method="None",
        percent_null=40,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="LEGAL_FIRST_NAME",
        faker_method="fake.first_name()",
        percent_null=40,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="LEGAL_MIDDLE_NAME",
        faker_method="fake.first_name()",
        percent_null=60,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="LEGAL_LAST_NAME",
        faker_method="fake.last_name()",
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="BIRTHDATE",
        faker_method="fake.date_time_between_dates(datetime_start='-99y', datetime_end='-20y').strftime('%Y-%m-%d 00:00:00.00')",
        percent_null=80,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_COMMENT",
        faker_method="None",
        percent_null=80,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_ID_TYPE_CODE",
        faker_method="fake.word(ext_word_list=['SIN', 'BCDL', 'ABDL', 'PSPT'])",
        percent_null=85,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_IDENTIFICATION",
        faker_method="fake.ssn()",
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="REGISTRY_COMPANY_TYPE_CODE",
        faker_method="''.join(random.choices(string.ascii_letters, k=4)).upper()",
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CORP_REGN_NMBR",
        faker_method="str(random.randrange(1000, 10000))",
        percent_null=60,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_ACRONYM",
        faker_method="''.join(random.choices(string.ascii_letters, k=8)).upper()",
        percent_null=80,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="WCB_FIRM_NUMBER",
        faker_method="str(random.randrange(1000, 100000))",
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="OCG_SUPPLIER_NMBR",
        faker_method="str(random.randrange(1000, 100000000))",
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_COMMENT",
        faker_method="fake.sentence()",
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_COMMENT",
        faker_method="fake.sentence()",
        percent_null=95,
    ),
]


class ORACLE_TYPES(Enum):
    """
    Define the database types that are supported.

    An enumeration of the different database types that are supported by the
    scripts in this project.

    """

    VARCHAR2 = 1
    DATE = 2
    NUMBER = 3
    CHAR = 4
    SDO_GEOMETRY = 5
    RAW = 6
    UNDEFINED = 7
    BINARY_DOUBLE = 8
    BLOB = 9
    CLOB = 10
    TIMESTAMP = 11
    LONG = 12


OracleMaskValuesMap = {
    ORACLE_TYPES.VARCHAR2: "'1'",
    ORACLE_TYPES.DATE: datetime.datetime.strptime(
        "0001-01-01 01:01:01", "%Y-%m-%d %H:%M:%S"
    ),
    ORACLE_TYPES.NUMBER: 1,
    ORACLE_TYPES.CHAR: "'1'",
    ORACLE_TYPES.TIMESTAMP: datetime.datetime.strptime(
        "0001-01-01 01:01:01", "%Y-%m-%d %H:%M:%S"
    ),
    ORACLE_TYPES.LONG: "'1'",
}
