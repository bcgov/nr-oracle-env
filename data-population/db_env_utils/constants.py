"""
Declare constants for the data_prep package.

"""

import logging
import os
import pathlib
from enum import Enum

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
