"""
Module to define data types used in the data population process.
"""

from dataclasses import dataclass


@dataclass
class DataToMask:
    """
    Table / Schema combination.

    * table_name: the name of the table to mask
    * schema: the schema of the table to mask
    * column_name: the column name to mask
    * faker_method: the faker method to use to mask the column, this is python
        code that will be executed exactly as is.
    * percent_null: the percentage of null values used as a target when
        populating the column.
    """

    table_name: str
    schema: str
    column_name: str
    faker_method: str
    percent_null: int


@dataclass
class DataFilter:
    """
    Structure used to store the filter information.

    When loading data either from oracle to duckdb or vise versa, this data
    structure is used to store the filter information.  In a nutshell it
    stores the where clause to prevent unnecessary data from being loaded /
    extracted.
    """

    table_name: str
    schema: str
    ora_where_clause: str
    ddb_where_clause: str


@dataclass
class TableConstraints:
    """
    Data class for storing constraints.

    Model / types for storing database constraints when queried from the
    database.
    """

    constraint_name: str
    table_name: str
    column_names: list[str]
    r_constraint_name: str
    referenced_table: str
    referenced_columns: list[str]
