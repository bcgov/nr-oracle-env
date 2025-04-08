"""
Reads the data classification spreadsheet and writes it to a duckdb table.
"""

import logging
import logging.config
import pathlib

import duckdb
import pandas as pd

LOGGER = logging.getLogger(__name__)


class DataClassificationImport:
    def __init__(self, input_excel: str):
        self.xlfile = excel_file

    def get_sheets(self) -> list[str]:
        xl_file = pd.ExcelFile(self.xlfile)
        sheets = [
            sheet_name
            for sheet_name in xl_file.sheet_names
            if sheet_name != "Classifications"
        ]
        # don't need the classifications sheet
        return sheets

    def read(self):
        sheets = self.get_sheets()
        LOGGER.debug("sheets:  %s", sheets)
        for sheet_name in sheets:
            self.read_sheet(sheet_name)

    def read_sheet(self, sheet_name):
        LOGGER.debug("reading the sheet: %s", sheet_name)
        dfs = pd.read_excel(self.xlfile, sheet_name=sheet_name)
        LOGGER.debug("columns: %s", dfs.columns)
        LOGGER.debug(
            "unique security classes: %s", dfs["INFO SECURITY CLASS"].unique()
        )
        subset_df = dfs.loc[
            dfs["INFO SECURITY CLASS"].str.lower() != "public",
            ["TABLE NAME", "COLUMN NAME", "INFO SECURITY CLASS"],
        ]
        # LOGGER.debug("subset_df: %s", subset_df.head())
        for index, row in subset_df.iterrows():
            LOGGER.debug("idx: %s row: %s", index, row)

        # need 'TABLE_NAME' / 'COLUMN NAME' / 'INFO SECURITY CLASS'
        # raise


if __name__ == "__main__":
    log_config_path = pathlib.Path(__file__).parent / "logging.config"
    logging.config.fileConfig(
        log_config_path,
        disable_existing_loggers=False,
    )

    logname = pathlib.Path(__file__).stem
    LOGGER = logging.getLogger(logname)
    LOGGER.debug("testing log message")

    excel_file = (
        pathlib.Path(__file__).parent
        / ".."
        / ".."
        / "project_specific"
        / "misc"
        / "CLIENT ECAS GAS2 ILCR ISP.xlsx"
    )
    dc = DataClassificationImport(input_excel=excel_file)
    dc.read()
