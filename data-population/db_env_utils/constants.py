"""
Declare constants for the data_prep package.

"""

import datetime
import decimal
import logging
import os
import pathlib
from enum import Enum

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
