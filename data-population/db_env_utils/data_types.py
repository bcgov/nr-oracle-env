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
