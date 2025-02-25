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


def get_default_export_file_ostore_path(
    table: str,
    db_type: DBType,
) -> pathlib.Path:
    """
    Return the path to the export file in object storage.

    Different databases have different file types that are used to export data.
    This method will determine the database type and return the path to the
    correct file reference.

    :param table: name of the database table who's corresponding data file in
        object storage is to be retrieved.
    :type table: str
    :param db_type: an enumeration of the database type, either ORA or
        OC_POSTGRES
    :type db_type: DBType
    :return: a path object that refers to the object storage location for the
        specified table.
    :rtype: pathlib.Path
    """
    if db_type == DBType.ORA:
        suffix = PARQUET_SUFFIX
    elif db_type == DBType.OC_POSTGRES:
        suffix = SQL_DUMP_SUFFIX
    parquet_file_name = f"{table}.{suffix}"
    ostore_dir = get_export_ostore_path(db_type)
    full_path = pathlib.Path(
        ostore_dir,
        parquet_file_name,
    )
    LOGGER.debug("parquet file name: %s", full_path)
    return full_path


def get_default_export_file_path(
    table: str,
    env_str: str,
    db_type: DBType,
) -> pathlib.Path:
    """
    Return the path to the default export file.

    Different databases have different file types that are used to export data.
    example: oracle will use parquet
    postgres will use sql file dumped from pg_dump
    """
    return_table = None
    if db_type == DBType.ORA:
        return_table = get_parquet_file_path(table, env_str, db_type)
    elif db_type == DBType.OC_POSTGRES:
        return_table = get_sql_dump_file_path(table, env_str, db_type)
    return return_table


def get_parquet_file_path(
    table: str,
    env_str: str,
    db_type: DBType,
) -> pathlib.Path:
    """
    Return path to parquet file that corresponds with a table.

    :param table: name of an oracle table
    :type table: str
    :param env_str: an environment string valid values DEV/TEST/PROD
    :type env_str: str
    :return: the path to the parquet file that corresponds with the table name
    :rtype: pathlib.Path
    """
    parquet_file_name = f"{table}.{PARQUET_SUFFIX}"
    return_path = pathlib.Path(
        DATA_DIR,
        env_str,
        db_type.name,
        parquet_file_name,
    )
    LOGGER.debug("parquet file name: %s", return_path)
    return return_path


def get_parquet_file_ostore_path(table: str, db_type: DBType) -> pathlib.Path:
    """
    Get path for data table in object store.

    Calculates the object store path for a table's data file.

    :param table: The name of the table who's corresponding data file is to be
        retrieved.
    :type table: str
    :return: path in object storage for the table's data file
    :rtype: pathlib.Path
    """
    parquet_file_name = f"{table}.{PARQUET_SUFFIX}"
    ostore_dir = get_export_ostore_path(db_type)
    full_path = pathlib.Path(
        ostore_dir,
        parquet_file_name,
    )
    LOGGER.debug("parquet file name: %s", full_path)
    return full_path


def get_export_ostore_path(db_type: DBType) -> pathlib.Path:
    """
    Return the directory path in object storage where the data files are stored.

    :param db_type: the type of database, either ORA or OC_POSTGRES
    :type db_type: DBType
    :return: the directory where the data files are located in object storage
        for the specified database type
    :rtype: pathlib.Path
    """
    full_path = pathlib.Path(
        OBJECT_STORE_DATA_DIRECTORY,
        db_type.name,
    )
    LOGGER.debug("object store data path: %s", full_path)
    return full_path


def get_sql_dump_file_path(
    table: str,
    env_str: str,
    db_type: DBType,
) -> pathlib.Path:
    """
    Return path to sql dump file that corresponds with a table.

    :param table: name of an oracle table
    :type table: str
    :param env_str: an environment string valid values DEV/TEST/PROD
    :type env_str: str
    :return: the path to the sql dump file that corresponds with the table name
    :rtype: pathlib.Path
    """
    parquet_file = get_parquet_file_path(
        table=table,
        env_str=env_str,
        db_type=db_type,
    )
    sql_dump_file = parquet_file.with_suffix("." + SQL_DUMP_SUFFIX)
    LOGGER.debug("sql dump file name: %s", sql_dump_file)
    return sql_dump_file
