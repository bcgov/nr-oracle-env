"""Retrieve, read, and consolidate data classification information that
is originally derived from the SharePoint site.
"""

import logging
import logging.config
import pathlib
import sys
import openpyxl
import enum
import pprint
import json

logger = logging.getLogger(__name__)


class DataClasses(enum.Enum):
    PUBLIC = 1
    A = 2
    B = 3
    C = 4
    CONFIDENTIAL = 5


class DataClassifications:
    """A utility class to store data classification information.

    The structure is used to store data classification information will be a
    table_name -> column name - > security_classification mapping.
    """

    def __init__(self):
        self.struct = {}
        self.public_security_classification_values = [
            "NONE",
            "",
            "TABLE NOT REQUIRED",
        ]
        self.a_values = [
            "A",
            "PROTECTED A",
            "PROTECTED_A",
        ]
        self.b_values = [
            "B",
            "PROTECTED B",
            "PROTECTED_B",
        ]
        self.c_values = ["C", "PROTECTED C", "PROTECTED_C", "PROTECTED B OR C"]

        self.public_security_classification_values = [
            value.upper()
            for value in self.public_security_classification_values
        ]
        self.a_values = [value.upper() for value in self.a_values]
        self.b_values = [value.upper() for value in self.b_values]
        self.c_values = [value.upper() for value in self.c_values]
        # if the sec class is not set assume public
        self.public_security_classification_values.append(None)

    def add_classification(
        self, table_name, column_name_str, security_classification_str
    ):
        """Add a data classification to the structure."""
        logger.debug("sc is: %s", security_classification_str)
        security_classification = self.get_security_classification(
            security_classification_str,
        )
        if table_name not in self.struct:
            self.struct[table_name] = {}
        if column_name_str not in self.struct[table_name]:
            self.struct[table_name][column_name_str] = security_classification
        else:
            existing_security_classification = self.struct[table_name][
                column_name_str
            ]
            if (
                existing_security_classification.value
                < security_classification.value
            ):
                logger.warning(
                    "Overwriting existing security classification %s for table %s, column %s with %s",
                    existing_security_classification.name,
                    table_name,
                    column_name_str,
                    security_classification.name,
                )
                self.struct[table_name][column_name_str] = (
                    security_classification
                )
            else:
                logger.info(
                    "Not overwriting existing security classification %s for table %s, column %s",
                    existing_security_classification.name,
                    table_name,
                    column_name_str,
                )

    def get_security_classification(self, security_classification_str):
        """Get the security classification from the string."""
        logger.debug("security classification: %s", security_classification_str)
        try:
            if security_classification_str is not None:
                security_classification_str = (
                    security_classification_str.strip().upper()
                )
            if (
                security_classification_str
                in self.public_security_classification_values
            ):
                logger.debug(
                    "Security classification is public or not set, returning PUBLIC"
                )
                return DataClasses.PUBLIC
            if security_classification_str in self.a_values:
                logger.debug(
                    "Security classification value %s, is resolving to A",
                    security_classification_str,
                )
                return DataClasses.A

            if security_classification_str in self.b_values:
                logger.debug(
                    "Security classification value %s, is resolving to B",
                    security_classification_str,
                )
                return DataClasses.B
            if security_classification_str in self.c_values:
                logger.debug(
                    "Security classification value %s, is resolving to C",
                    security_classification_str,
                )
                return DataClasses.C
            return DataClasses[security_classification_str.upper()]
        except KeyError:
            logger.error(
                "Invalid security classification: %s",
                security_classification_str,
            )
            raise ValueError(
                f"Invalid security classification: {security_classification_str}"
            )

    def dump_struct(self, dest_doc, indent=2):
        """Dump the structure to a JSON file."""
        logger.info("Dumping data classification structure to %s", dest_doc)

        struct = {}
        for table_name, columns in self.struct.items():
            struct[table_name] = {}
            for column_name, security_classification in columns.items():
                struct[table_name][column_name] = security_classification.name

        with open(dest_doc, "w") as f:
            json.dump(struct, f, indent=indent)
        logger.info("Data classification structure dumped successfully.")


class Consoldate_DC:
    def __init__(
        self,
        src_dir,
        dest_doc,
    ):
        """ """
        self.src_dir = src_dir
        self.dest_doc = dest_doc

        self.struct = {}
        self.dc = DataClassifications()

        self.ignore_sheets = []

        # dictionary to link the workbook to the data classification sheet
        # that resides in the workbook... there can be other sheets in the wb,
        # but the one we care about is the data classification sheet
        self.wb_dc_sheet = {}

    def consolidate(self):
        """Consolidate data classification information from the source directory
        and write it to the destination document.
        """
        logger.info("Consolidating data classification information...")
        logger.debug("Source directory: %s", self.src_dir)
        suffix = ".xlsx"
        xl_files = list(self.src_dir.glob(f"*{suffix}"))
        logger.debug("xl files: %s", xl_files)

        for xl_file in xl_files:
            self.read_xl_file(xl_file)

        self.dc.dump_struct(
            self.dest_doc,
            indent=2,
        )

    def read_xl_file(self, xl_file: pathlib.Path):
        """Read an Excel file and return its content."""
        logger.info("Reading Excel file: %s", xl_file)
        # Implement the logic to read the Excel file
        # For now, just return a placeholder
        # wb = xlrd.open_workbook(xl_file)
        wb = openpyxl.load_workbook(xl_file, read_only=True)
        sheet_names = wb.sheetnames
        logger.debug("Sheet names: %s", sheet_names)
        wb_verified = False
        wb_verified_sheet = None
        for sheet_name in sheet_names:
            sheet = wb[sheet_name]
            logger.debug("Processing sheet: %s", sheet)
            # ws = wb.sheet_by_name(sheet)  # or wb.sheet_by_index(0)
            if self.has_data(sheet):
                # Process the worksheet if it has data
                logger.debug("Processing sheet: %s", sheet_name)
                # Here you would implement the logic to process the worksheet
                # For example, count non-empty cells or extract specific data
                # count = self.count_non_empty_cells(ws)
                # logger.debug("Non-empty cells in %s: %d", sheet, count)
                if self.verify_sheet(sheet):
                    wb_verified = True
                    wb_verified_sheet = sheet
                    if xl_file.name not in self.wb_dc_sheet:
                        self.wb_dc_sheet[xl_file.name] = []
                    self.wb_dc_sheet[xl_file.name].append(sheet_name)

        if not wb_verified:
            logger.error(
                "No valid worksheet found in workbook %s with required columns",
                xl_file,
            )
            raise ValueError(
                f"No valid worksheet found in workbook {xl_file} with required"
                "columns"
            )
        # pprint.PrettyPrinter(indent=2).pprint(self.wb_dc_sheet)
        for sheet_name in self.wb_dc_sheet[xl_file.name]:
            sheet = wb[sheet_name]
            logger.debug("Extracting data from sheet: %s", sheet_name)
            # Extract data from the verified sheet
            self.extract_data(sheet, sheet_name, workbook_name=xl_file)

    def verify_sheet_old(self, ws):
        # Table	Column

        schema_columns = [
            "SCHEMA",
            "OWNER",
        ]
        # TABLE_NAME

        table_name_columns = ["TABLE_NAME", "TABLE", "TABLE NAME"]
        column_name_columns = ["COLUMN_NAME", "COLUMN", "COLUMN NAME"]
        security_class = [
            "INFO SECURITY CLASS",
            "CLASSIFICATION",
            "Information Security Classification",
            "ISC",
        ]
        # Information Security Classification

        required_columns = {
            # "schema_columns": schema_columns,
            "table_name_columns": table_name_columns,
            "column_name_columns": column_name_columns,
            "security_class": security_class,
        }
        required_columns_exist = {
            # "schema_columns": False,
            "table_name_columns": False,
            "column_name_columns": False,
            "security_class": False,
        }
        # look in the first row of the worksheet for the required columns
        first_row = next(ws.iter_rows(max_row=1, values_only=True), None)
        logger.debug("First row: %s", first_row)
        for idx, cell_value in enumerate(first_row):
            if cell_value:
                cell_value = cell_value.upper()
                logger.debug("Cell value: %s", cell_value)
                for key, columns in required_columns.items():
                    logger.debug("key: %s", key)
                    logger.debug("columns: %s", columns)
                    if cell_value.upper() in [
                        column.upper() for column in columns
                    ]:
                        required_columns_exist[key] = True
                        logger.debug("found column %s in position %s", key, idx)

        for key in required_columns_exist.keys():
            if not required_columns_exist[key]:
                logger.error(
                    "Required column %s not found in worksheet %s",
                    key,
                    ws.title,
                )
                return False
        return True

    def extract_data(self, ws, sheet_name, workbook_name=None):
        """Extract data from the worksheet and store it in the structure."""
        logger.debug("Extracting data from sheet: %s", sheet_name)
        column_indicies = self.get_column_indexes(ws)
        logger.debug("Column indexes: %s", column_indicies)
        for row in ws.iter_rows(min_row=2, values_only=True):
            table_name = row[column_indicies["table_name"]]
            column_name = row[column_indicies["column_name"]]
            security_classification_value = row[
                column_indicies["security_classification"]
            ]
            logger.info("table name column: %s", table_name)
            logger.info("column name column: %s", column_name)
            logger.info(
                "security classification value in row: %s",
                security_classification_value,
            )

            if not table_name or not column_name:
                logger.warning(
                    "Skipping row with missing schema, table name, or column name: %s",
                    row,
                )
                continue

            # Convert to string for consistency
            table_name_str = str(table_name).strip()
            column_name_str = str(column_name).strip()
            logger.debug(
                "security_classification_name (column name): %s",
                security_classification_value,
            )
            self.dc.add_classification(
                table_name_str,
                column_name_str,
                security_classification_value,
            )

    def verify_sheet(self, ws):
        columns_idx = self.get_column_indexes(ws, no_raise=True)
        if len(columns_idx) != 3:
            logger.error(
                "Required columns not found in worksheet %s",
                ws.title,
            )
            return False
        for key in columns_idx.keys():
            if columns_idx[key] is None:
                logger.error(
                    "Required column %s not found in worksheet %s",
                    key,
                    ws.title,
                )
                return False
        return True

    def get_column_indexes(self, ws, no_raise=False):
        """Get the column indexes for the required columns."""
        # schema_columns = [
        #     "SCHEMA",
        #     "OWNER",
        # ]
        table_name_columns = ["TABLE_NAME", "TABLE", "TABLE NAME"]
        column_name_columns = ["COLUMN_NAME", "COLUMN", "COLUMN NAME"]
        security_class = [
            "INFO SECURITY CLASS",
            "CLASSIFICATION",
            "Information Security Classification",
            "ISC",
        ]

        column_indexes = {
            # "schema": None,
            "table_name": None,
            "column_name": None,
            "security_classification": None,
        }

        column_name_columns = [col.upper() for col in column_name_columns]
        security_class = [sc.upper() for sc in security_class]
        table_name_columns = [col.upper() for col in table_name_columns]

        for row in ws.iter_rows(max_row=1, values_only=True):
            for idx, cell_value in enumerate(row):
                if cell_value:
                    cell_value = cell_value.upper()
                    # if cell_value in schema_columns:
                    #     column_indexes["schema"] = idx
                    if cell_value in table_name_columns:
                        column_indexes["table_name"] = idx
                    elif cell_value in column_name_columns:
                        column_indexes["column_name"] = idx
                    elif cell_value in security_class:
                        column_indexes["security_classification"] = idx
                        logger.debug("sec class found at index %s", idx)

        logger.debug("Column indexes: %s", column_indexes)
        if (
            column_indexes["table_name"] is None
            or column_indexes["column_name"] is None
            or column_indexes["security_classification"] is None
        ):
            logger.error(
                "Required columns not found in worksheet %s",
                ws.title,
            )
            if not no_raise:
                raise ValueError(
                    f"Required columns not found in worksheet {ws.title}"
                )
        logger.debug("Final column indexes: %s", column_indexes)
        return column_indexes

    def has_data(self, ws):
        """Check if worksheet has data using openpyxl."""
        # Check if worksheet has any cells with values
        for row in ws.iter_rows():
            if row:
                # logger.debug("Row: %s %s", row[0], type(row[0]))
                if (
                    row[0].value
                    and row[1].value
                    and row[0].value.upper() == "CLASSIFICATIONS"
                    and row[1].value.upper() == "DEFINITIONS"
                ):
                    logger.debug(
                        "Skipping row with CLASSIFICATIONS and DEFINITIONS headers"
                    )
                    # Classifications	Definitions
                    # this is an indicator that the data classfication template sheet
                    # which contains metadata about the data classification is the
                    # current sheet
                    return False
                for cell in row:
                    if cell.value is not None and str(cell.value).strip():
                        return True
        return False


if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print(
    #         "Usage: python consolidate_data_classification.py <src_dir> <dest_doc>"
    #     )
    #     sys.exit(1)
    logging_config_file = pathlib.Path("__file__").parent / "logging.config"
    logging.config.fileConfig(logging_config_file)
    logger = logging.getLogger("main")

    src_dir = (
        pathlib.Path(__file__).parent
        / ".."
        / "project_specific"
        / "lexus"
        / "data_classifications"
    )
    dest_doc = (
        pathlib.Path(__file__).parent
        / ".."
        / "project_specific"
        / "lexus"
        / "data_classifications"
        / "consolidated_data_classification.json"
    )

    consolidator = Consoldate_DC(src_dir, dest_doc)
    consolidator.consolidate()
    # Here you would call methods to perform the consolidation
    # e.g., consolidator.consolidate()
