"""
Starting to move any path methods to this module.
"""

import logging
import os
import pathlib
import tempfile

import constants
import env_config

LOGGER = logging.getLogger(__name__)


class AppPaths:
    """
    Class to manage paths for the application.
    """

    def __init__(self, env: env_config.Env) -> None:
        """
        Construct an instance of the AppPaths class.

        :param env: an env object that identifies what env to calculate paths
                    for.
        :type env: env_config.Env
        """
        self.env = env

    def get_temp_parquet_file(
        self,
        prefix: str = "tmp_",
    ) -> pathlib.Path:
        """
        Generate a path to a temporary parquet file.

        """
        tmpdir = self.get_temp_dir()
        fd, tmp_parquet_file = tempfile.mkstemp(
            suffix=".parquet",
            dir=tmpdir,
            prefix=prefix,
        )
        if os.path.exists(tmp_parquet_file):
            os.remove(tmp_parquet_file)
        LOGGER.debug("tmp_parquet_file: %s", tmp_parquet_file)
        LOGGER.debug("fd: %s %s", fd, type(fd))
        return pathlib.Path(tmp_parquet_file)

    def get_data_dir(self, *, create=True) -> pathlib.Path:
        """
        Generate a path to the data directory.

        Generates a path in the directory on below the directory that this
        file is found in.

        :return: _description_
        :rtype: pathlib.Path
        """
        data_dir = pathlib.Path(__file__).parent.parent / constants.DATA_DIR
        if create:
            data_dir.mkdir(exist_ok=True)
        LOGGER.debug("data dir is: %s", data_dir)
        return data_dir

    def get_log_config_dir(self) -> pathlib.Path:
        """
        Get the directory path where the log config should be.

        :return: _description_
        :rtype: pathlib.Path
        """
        log_config_dir = pathlib.Path(__file__).parent
        LOGGER.debug("log config dir is: %s", log_config_dir)
        return log_config_dir

    def get_temp_dir(self, *, create: bool = True) -> pathlib.Path:
        """
        Generate a path to the temporary directory.

        Generates a path in the directory on below the directory that this
        file is found in.

        Creates the directory also.

        :return: _description_
        :rtype: pathlib.Path
        """
        tmp_dir = self.get_data_dir() / constants.TEMP_DIR
        LOGGER.debug("tmp dir is: %s", tmp_dir)
        if create:
            tmp_dir.mkdir(exist_ok=True)
        return tmp_dir

    def get_parquet_file_path(
        self,
        table: str,
        env_str: str,
        db_type: constants.DBType,
    ) -> pathlib.Path:
        """
        Return path to parquet file that corresponds with a table.

        :param table: name of an oracle table
        :type table: str
        :param env_str: an environment string valid values DEV/TEST/PROD
        :type env_str: str
        :return: the path to the parquet file that corresponds with the table
                 name
        :rtype: pathlib.Path
        """
        parquet_file_name = f"{table}.{constants.PARQUET_SUFFIX}"
        return_path = pathlib.Path(
            constants.DATA_DIR,
            env_str,
            db_type.name,
            parquet_file_name,
        )
        LOGGER.debug("parquet file name: %s", return_path)
        return return_path

    def get_sql_dump_file_path(
        self,
        table: str,
        env_str: str,
        db_type: constants.DBType,
    ) -> pathlib.Path:
        """
        Return path to sql dump file that corresponds with a table.

        :param table: name of an oracle table
        :type table: str
        :param env_str: an environment string valid values DEV/TEST/PROD
        :type env_str: str
        :return: the path to the sql dump file that corresponds with the table
                 name
        :rtype: pathlib.Path
        """
        parquet_file = self.get_parquet_file_path(
            table=table,
            env_str=env_str,
            db_type=db_type,
        )
        sql_dump_file = parquet_file.with_suffix(
            "." + constants.SQL_DUMP_SUFFIX,
        )
        LOGGER.debug("sql dump file name: %s", sql_dump_file)
        return sql_dump_file

    def get_default_export_file_ostore_path(
        self,
        table: str,
        db_type: constants.DBType,
    ) -> pathlib.Path:
        """
        Return the path to the export file in object storage.

        Different databases have different file types that are used to export
        data.  This method will determine the database type and return the path
        to the correct file reference.

        :param table: name of the database table who's corresponding data file
                      in object storage is to be retrieved.
        :type table: str
        :param db_type: an enumeration of the database type, either ORA or
            OC_POSTGRES
        :type db_type: DBType
        :return: a path object that refers to the object storage location for
                 the specified table.
        :rtype: pathlib.Path
        """
        if db_type == constants.DBType.ORA:
            suffix = constants.PARQUET_SUFFIX
        elif db_type == constants.DBType.OC_POSTGRES:
            suffix = constants.SQL_DUMP_SUFFIX
        parquet_file_name = f"{table}.{suffix}"
        ostore_dir = self.get_export_ostore_path(db_type)
        full_path = pathlib.Path(
            ostore_dir,
            parquet_file_name,
        )
        LOGGER.debug("parquet file name: %s", full_path)
        return full_path

    def get_default_export_file_path(
        self,
        table: str,
        env_str: str,
        db_type: constants.DBType,
    ) -> pathlib.Path:
        """
        Return the path to the default export file.

        Different databases have different file types that are used to export
        data. Example: oracle will use parquet, postgres will use sql file
        dumped from pg_dump.
        """
        return_table = None
        if db_type == constants.DBType.ORA:
            return_table = self.get_parquet_file_path(table, env_str, db_type)
        elif db_type == constants.DBType.OC_POSTGRES:
            return_table = self.get_sql_dump_file_path(table, env_str, db_type)
        return return_table

    def get_parquet_file_ostore_path(
        self,
        table: str,
        db_type: constants.DBType,
    ) -> pathlib.Path:
        """
        Get path for data table in object store.

        Calculates the object store path for a table's data file.

        :param table: The name of the table who's corresponding data file is to
                      be retrieved.
        :type table: str
        :return: path in object storage for the table's data file
        :rtype: pathlib.Path
        """
        parquet_file_name = f"{table}.{constants.PARQUET_SUFFIX}"
        ostore_dir = self.get_export_ostore_path(db_type)
        full_path = pathlib.Path(
            ostore_dir,
            parquet_file_name,
        )
        LOGGER.debug("parquet file name: %s", full_path)
        return full_path

    def get_export_ostore_path(self, db_type: constants.DBType) -> pathlib.Path:
        """
        Return the object store path for the data files.

        Return the directory path in object storage where the data files are
        stored.

        :param db_type: the type of database, either ORA or OC_POSTGRES
        :type db_type: DBType
        :return: the directory where the data files are located in object
                 storage for the specified database type
        :rtype: pathlib.Path
        """
        full_path = pathlib.Path(
            constants.OBJECT_STORE_DATA_DIRECTORY,
            db_type.name,
        )
        LOGGER.debug("object store data path: %s", full_path)
        return full_path
