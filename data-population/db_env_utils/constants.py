"""
Declare constants for the data_prep package.

"""

import datetime
import decimal
import logging
import os
import random
import string
from enum import Enum

import data_types
import faker
import oracledb
import pyarrow

LOGGER = logging.getLogger(__name__)
fake = faker.Faker("en_CA")


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
DUCK_DB_SUFFIX = "ddb"

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
    # Can also be pyarrow.int64() if no decimals
    oracledb.DB_TYPE_NUMBER: pyarrow.float64(),
    oracledb.DB_TYPE_BINARY_FLOAT: pyarrow.float32(),
    oracledb.DB_TYPE_BINARY_DOUBLE: pyarrow.float64(),
    oracledb.DB_TYPE_DATE: pyarrow.timestamp("s"),
    oracledb.DB_TYPE_TIMESTAMP: pyarrow.timestamp("ns"),
    oracledb.DB_TYPE_TIMESTAMP_LTZ: pyarrow.timestamp(
        "ns",
    ),  # Oracle's Local Time Zone
    oracledb.DB_TYPE_TIMESTAMP_TZ: pyarrow.timestamp("ns", tz="UTC"),
    oracledb.DB_TYPE_BLOB: pyarrow.binary(),
    oracledb.DB_TYPE_RAW: pyarrow.binary(),
    oracledb.DB_TYPE_LONG_RAW: pyarrow.binary(),
    oracledb.DB_TYPE_BOOLEAN: pyarrow.bool_(),
    oracledb.DB_TYPE_INTERVAL_DS: pyarrow.duration(
        "s",
    ),  # Interval Day to Second
    # Interval Year to Month (int is closest)
    oracledb.DB_TYPE_INTERVAL_YM: pyarrow.int32(),
}


DATA_TO_MASK = [
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CLIENT_LOCN_NAME",
        faker_method=lambda: " ".join(fake.words(nb=3)),
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="ADDRESS_1",
        faker_method=lambda: fake.address().split("\n")[0],
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="ADDRESS_2",
        faker_method=lambda: fake.address().split("\n")[1].split(",")[0],
        percent_null=80,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="ADDRESS_3",
        faker_method=lambda: fake.address().split("\n")[1].split(",")[1],
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CITY",
        faker_method=lambda: fake.city(),
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="POSTAL_CODE",
        faker_method=lambda: fake.postalcode(),
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="HOME_PHONE",
        faker_method=lambda: "".join(filter(str.isdigit, fake.phone_number())),
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CELL_PHONE",
        faker_method=lambda: "".join(filter(str.isdigit, fake.phone_number())),
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="FAX_NUMBER",
        faker_method=lambda: "".join(filter(str.isdigit, fake.phone_number())),
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="EMAIL_ADDRESS",
        faker_method=lambda: fake.ascii_email(),
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="CLIENT_LOCATION",
        schema="THE",
        column_name="CLI_LOCN_COMMENT",
        faker_method=lambda: fake.sentence(),
        percent_null=40,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="LEGAL_FIRST_NAME",
        faker_method=lambda: fake.first_name(),
        percent_null=40,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="LEGAL_MIDDLE_NAME",
        faker_method=lambda: fake.first_name(),
        percent_null=60,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="LEGAL_LAST_NAME",
        faker_method=lambda: fake.last_name(),
        percent_null=0,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="BIRTHDATE",
        faker_method=lambda: fake.date_time_between_dates(
            datetime_start="-99y",
            datetime_end="-20y",
        ).strftime("%Y-%m-%d 00:00:00"),
        percent_null=80,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_COMMENT",
        faker_method=lambda: fake.sentence(),
        percent_null=80,
    ),
    # data_types.DataToMask(
    #     table_name="FOREST_CLIENT",
    #     schema="THE",
    #     column_name="CLIENT_ID_TYPE_CODE",
    #     faker_method=lambda: fake.word(
    #         ext_word_list=["SIN", "BCDL", "ABDL", "PSPT"]
    #     ),
    #     percent_null=85,
    # ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_IDENTIFICATION",
        faker_method=lambda: fake.ssn(),
        percent_null=95,
    ),
    # data_types.DataToMask(
    #     table_name="FOREST_CLIENT",
    #     schema="THE",
    #     column_name="REGISTRY_COMPANY_TYPE_CODE",
    #     faker_method=lambda: "".join(
    #         random.choices(string.ascii_letters, k=4)
    #     ).upper(),
    #     percent_null=90,
    # ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CORP_REGN_NMBR",
        faker_method=lambda: str(
            random.randrange(1000, 10000),  # noqa: S311
        ),
        percent_null=60,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_ACRONYM",
        faker_method=lambda: "".join(
            random.choices(string.ascii_letters, k=8),  # noqa: S311
        ).upper(),
        percent_null=80,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="WCB_FIRM_NUMBER",
        faker_method=lambda: str(
            random.randrange(1000, 100000),  # noqa: S311
        ),
        percent_null=90,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="OCG_SUPPLIER_NMBR",
        faker_method=lambda: str(
            random.randrange(1000, 100000000),  # noqa: S311
        ),
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_COMMENT",
        faker_method=lambda: fake.sentence(),
        percent_null=95,
    ),
    data_types.DataToMask(
        table_name="FOREST_CLIENT",
        schema="THE",
        column_name="CLIENT_COMMENT",
        faker_method=lambda: fake.sentence(),
        percent_null=95,
    ),
]


class ORACLE_TYPES(Enum):  # noqa: N801
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
    ORACLE_TYPES.DATE: f"'{
        datetime.datetime.strptime('0001-01-01 01:01:01', '%Y-%m-%d %H:%M:%S')
    }'",  # noqa: DTZ007
    ORACLE_TYPES.NUMBER: 1,
    ORACLE_TYPES.CHAR: "'1'",
    ORACLE_TYPES.TIMESTAMP: f"'{
        datetime.datetime.strptime('0001-01-01 01:01:01', '%Y-%m-%d %H:%M:%S')
    }'",  # noqa: DTZ007
    ORACLE_TYPES.LONG: "'1'",
}


class DUCK_DB_TYPES(Enum):  # noqa: N801
    """
    Define the duckdb database types that are supported.

    :param Enum: enumeration with duckdb data types that are supported by the
        scripts in this project.
    :type Enum: Enum
    """

    DECIMAL = 1
    DOUBLE = 2
    BIGINT = 3
    FLOAT = 4
    VARCHAR = 5
    TEXT = 6
    TIMESTAMP = 7
    BLOB = 8
    GEOMETRY = 9


ORACLE_TYPES_TO_DDB_TYPES = {
    ORACLE_TYPES.VARCHAR2: DUCK_DB_TYPES.VARCHAR,
    ORACLE_TYPES.DATE: DUCK_DB_TYPES.TIMESTAMP,
    ORACLE_TYPES.NUMBER: DUCK_DB_TYPES.DECIMAL,
    ORACLE_TYPES.CHAR: DUCK_DB_TYPES.VARCHAR,
    ORACLE_TYPES.SDO_GEOMETRY: DUCK_DB_TYPES.GEOMETRY,
    ORACLE_TYPES.RAW: DUCK_DB_TYPES.BLOB,
    ORACLE_TYPES.BLOB: DUCK_DB_TYPES.BLOB,
    ORACLE_TYPES.CLOB: DUCK_DB_TYPES.TEXT,
    ORACLE_TYPES.TIMESTAMP: DUCK_DB_TYPES.TIMESTAMP,
}


ORACLE_TYPES_DEFAULT_FAKER = {
    ORACLE_TYPES.VARCHAR2: lambda: fake.word(),
    ORACLE_TYPES.DATE: lambda: fake.date_time_this_century().strftime(
        "%Y-%m-%d 00:00:00",
    ),
    ORACLE_TYPES.NUMBER: lambda: random.randint(0, 100000),  # noqa: S311
    ORACLE_TYPES.CHAR: lambda: fake.word(),
    ORACLE_TYPES.TIMESTAMP: lambda: fake.date_time_this_century().strftime(
        "%Y-%m-%d 00:00:00",
    ),
    ORACLE_TYPES.LONG: lambda: fake.word(),
}

# data import config
#  Some tables are really big and we do not need to load all the data.
#  The following are extra where clauses that get applied when the
# data is extracted and loaded
BIG_DATA_FILTERS = [
    data_types.DataFilter(
        table_name="RESULTS_AUDIT_DETAIL",
        schema="THE",
        ora_where_clause=(
            "ENTRY_TIMESTAMP > TO_DATE('2024-01-01', 'YYYY-MM-DD')"
        ),
        ddb_where_clause="ENTRY_TIMESTAMP > make_date(2024, 01, 01)",
    ),
    data_types.DataFilter(
        table_name="RESULTS_AUDIT_EVENT",
        schema="THE",
        ora_where_clause=(
            "ENTRY_TIMESTAMP > TO_DATE('2024-01-01', 'YYYY-MM-DD')"
        ),
        ddb_where_clause="ENTRY_TIMESTAMP > make_date(2024, 01, 01)",
    ),
]
