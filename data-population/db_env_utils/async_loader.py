"""
This was never completed, but leaving the code in the repo,  in case the
current approach to loading data from a pandas dataframe are not fast enough.

"""

import oradb_lib.OracleDatabase
import oracledb
import csv
from pypika import Table
from pypika import OracleQuery as Query
import logging
from concurrent import futures
import queue
import pandas

LOGGER = logging.getLogger(__name__)


class AsyncLoader:
    """
    adapter from https://github.com/andrewvillazon/quickly-load-data-db-python/blob/master/loadcsv.py
    """

    def __init__(self, connection: oracledb.Connection):
        self.multi_row_insert_limit = 1000
        self.workers = 6
        self.connection = connection

    def execute_query(self, q):
        cursor = self.connection.cursor()

        cursor.execute(q)
        self.connection.commit()

        self.connection.close()

    def read_csv(self, csv_file):
        with open(csv_file, encoding="utf-8", newline="") as in_file:
            reader = csv.reader(in_file, delimiter="|")
            next(reader)  # Header row

            for row in reader:
                yield row

    def multi_row_insert(self, batch, table_name):
        row_expressions = []

        for _ in range(batch.qsize()):
            row_data = tuple(batch.get())
            row_expressions.append(row_data)

        table = Table(table_name)
        # oracle doesn't support inserting more than one row at a time, need to create the multiple
        # insert statements... thinking doing this would make more sense,
        insert_into = Query.into(table).insert(*row_expressions)
        LOGGER.debug(f"Insert query: {insert_into}")
        self.execute_query(str(insert_into))

    def process_row(self, row, batch, table_name):
        batch.put(row)

        if batch.full():
            self.multi_row_insert(batch, table_name)

        return batch

    def load_data(self, table_name, import_parquet_file):
        #         pandas_df = pd.read_parquet(import_file)

        batch = queue.Queue(self.multi_row_insert_limit)

        with futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            todo = []
            pandas_df = pd.read_parquet(import_parquet_file)
            for row in pandas_df.iterrows():
                # LOGGER.debug(f"row: {row}")
                future = executor.submit(
                    self.process_row, row, batch, table_name
                )
                todo.append(future)

            for future in futures.as_completed(todo):
                result = future.result()

        # Handle left overs
        if not result.empty():
            self.multi_row_insert(result, table_name)
