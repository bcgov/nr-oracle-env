"""
Analyze the data classifications against a database to identify if there are
any table/column combinations that have been identified as sensitive data that
are also identified as NON public data.

"""
import json
import logging
import logging.config
import os
import pathlib

import oracledb
import pandas as pd

logging.config.fileConfig('logging.config')
logger = logging.getLogger("main")
logger.debug("-------------------- test----------------------")

class Get_Join_Conditions:

    def __init__(self, dc_doc: pathlib.Path):
        self.connection = None
        self.dc_doc = dc_doc
        self.connect()

    def connect(self):
        """
        Connect to the database.
        """
        # schema currently hardcoded to 'THE'
        user = os.getenv("ORACLE_USERNAME")
        password = os.getenv("ORACLE_PASSWORD")
        host = os.getenv("ORACLE_HOST")
        port = os.getenv("ORACLE_PORT")
        service_name = os.getenv("ORACLE_SERVICE_NAME")
        dsn = oracledb.makedsn(host, port, service_name=service_name)
        self.connection = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn,
        )

    def compare(self):
        """
        Compare the data classifications against the database.
        """
        # Get the data classification from the JSON file
        df_dc = self.get_data_classification()
        logger.debug("columns for dc struct: %s", df_dc.columns)

        # Get the primary and foreign keys from the database
        df_pk_fk = self.get_pk_fks()

        df_pk_fk = df_pk_fk.rename(columns={'TABLE_NAME': 'table_name'})
        df_pk_fk = df_pk_fk.rename(columns={'COLUMN_NAME': 'column_name'})
        logger.debug("columns for dc struct: %s", df_dc.columns)

        # Merge the data classification DataFrame with the PK/FK DataFrame on table_name and column_name
        # Remove records from df_dc that exist in df_pk_fk based on table_name and column_name
        df_dc = df_dc.merge(
            df_pk_fk[['table_name', 'column_name']],
            on=['table_name', 'column_name'],
            how='left',
            indicator=True
        )
        df_common = df_dc[df_dc['_merge'] == 'left_only'].drop(columns=['_merge'])
        # df_common = pd.merge(
        #     df_dc,
        #     df_pk_fk,
        #     on=['table_name', 'column_name'],
        #             how='left',
        # indicator=True

        #     #how='inner'
        # )

        # df_common now contains table/column combinations present in both data classification and PK/FK info
        logger.debug(df_common)

        df_common.to_csv("tofix.csv", index=False)

        # create fixed json
        fixed_struct = {}
        for row in df_common.itertuples(index=True, name='Row'):
            logger.debug(f"Row {row.Index}:")
            # Access columns by attribute name
            logger.debug(f"  table_name: {row.table_name}")
            logger.debug(f"  column_name: {row.column_name}")
            logger.debug(f"  data_classification: {row.data_classification}")
            if not row.table_name in fixed_struct:
                fixed_struct[row.table_name] = {}
            fixed_struct[row.table_name][row.column_name] = row.data_classification

        fix_doc_path = self.dc_doc.parent / "fixed_data_classification.json"
        with fix_doc_path.open("w") as f:
            json.dump(fixed_struct, f, indent=4)

    def get_data_classification(self) -> pd.DataFrame:
        """read the data classifications from json struct

        :return: _description_
        :rtype: pd.DataFrame
        """
        with self.dc_doc.open("r") as f:
            struct = json.load(f)
        # Create a list of records
        records = []
        for table_name, columns in struct.items():
            for column_name, classification in columns.items():
                if classification.lower() != "public":
                    records.append({
                        'table_name': table_name,
                        'column_name': column_name,
                        'data_classification': classification
                    })

        # Convert to DataFrame
        df = pd.DataFrame(records)
        return df



    def get_pk_fks(self) -> pd.DataFrame:
        """
        Get the primary and foreign keys from the database
        """
        # schema currently hardcoded to 'THE'
        query = self.get_query()
        df = pd.read_sql(query, self.connection)
        return df

    def get_query(self) -> None:
        """
        Get the query to join the data classification table with the database
        """
        # schema currently hardcoded to 'THE
        query = """
            SELECT
                cols.table_name as table_name,
                cols.column_name as column_name,
                cons.constraint_type as constraint_type,
                CASE
                    WHEN cons.constraint_type = 'P' THEN 'Primary Key'
                    WHEN cons.constraint_type = 'R' THEN 'Foreign Key'
                END AS key_type,
                cons.constraint_name
            FROM user_constraints cons
            JOIN user_cons_columns cols ON cons.constraint_name = cols.constraint_name
            WHERE cons.constraint_type IN ('P', 'R')  -- P = Primary Key, R = Foreign Key
            ORDER BY cols.table_name,
                    CASE WHEN cons.constraint_type = 'P' THEN 1 ELSE 2 END,  -- Primary keys first
                    cols.column_name
        """
        return query


if __name__ == "__main__":

    dc_doc = pathlib.Path("/home/kjnether/fsa_proj/nr-fsa-orastruct/project_specific/lexis/data_classifications/consolidated_data_classification.json")
    Get_Join_Conditions(dc_doc).compare()
