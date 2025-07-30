"""
API for reading and classification information from either a xl spreadsheet or a
json file.
"""

import json
import logging
import pathlib

import constants
import data_types

logger = logging.getLogger(__name__)

class DataClassification:
    """
    Data classification and data masking class.

    Retrieves and merges the data classification information in the data
    classification spreadsheet, and the classifications that are already defined
    in the constants.DATA_TO_MASK list.  Where data is defined in both locations
    the constants.DATA_TO_MASK will take precidence.
    """

    def __init__(self, data_class_doc: pathlib.Path, schema: str) -> None:
        """
        Construct instance of the DataClassification class.

        :param data_class_doc: path to the data classification document.
        :type data_class_doc: pathlib.Path
        :param schema: the schema that all the objects in the spreadsheet belong
            to.
        :type schema: str
        """
        # self.valid_sheets = ["ECAS", "GAS2", "CLIENT", "ISP", "ILCR", "GAS"]
        self.valid_sheets = []
        self.dc_struct: None | dict[str, dict[str, data_types.DataToMask]] = (
            None
        )
        self.schema = schema
        self.data_class_doc_path = data_class_doc
        self.data_class_doc_type = self.get_document_type()
        self.load()

    def set_valid_sheets(self, valid_sheets: list[str]) -> None:
        """
        Set the valid sheets for the data classification document.  This will
        only apply if the soource document is an excel spreadsheet.

        :param valid_sheets: list of valid sheet names
        :type valid_sheets: list[str]
        """
        if self.data_class_doc_type != constants.DataClassificationDocumentType.EXCEL:
            logger.warning(
                "set_valid_sheets called with a sheet list of %s on a "
                "non-excel document, ignoring"
            )
        self.valid_sheets = valid_sheets

    def get_mask_info(self, table_name: str, column_name: str) -> str:
        """
        Get the data classification for a given table and column.

        :param table_name: name of the table
        :type table_name: str
        :param column_name: name of the column
        :type column_name: str
        :return: the data classification for the table and column
        :rtype: str
        """
        # double check that the struct has been populated
        if self.dc_struct is None:
            self.load()
        # create short vars
        tab = table_name.upper()
        col = column_name.upper()

        # get_mask_info
        if (tab in self.dc_struct) and col in self.dc_struct[tab]:
            return self.dc_struct[tab][col]
        return None

    def get_mask_dummy_val(self, data_type: constants.ORACLE_TYPES) -> str:
        """
        Return the dummy value for the data type.

        Recieves an oracle type like VARCHAR2, NUMBER, etc. and returns a dummy
        value that will go into the cells for this column that will be used
        to indicate that a particular column has been masked.

        :param data_type: the data type of the column to be masked
        :type data_type: constants.ORACLE_TYPES
        :return: the dummy value for the data type
        :rtype: str
        """
        if data_type not in constants.OracleMaskValuesMap:
            msg = (f"data type {data_type} not in OracleMaskValuesMap",)
            raise ValueError(msg)

        return constants.OracleMaskValuesMap[data_type]

    def has_masking(self, table_name: str) -> bool:
        """
        Check if the table has any masking.

        :param table_name: name of the table
        :type table_name: str
        :return: True if the table has any masking, False otherwise
        :rtype: bool
        """
        return table_name.upper() in self.dc_struct

    def get_document_type(self) -> constants.DataClassificationDocumentType:

        if self.data_class_doc_path.suffix == ".xlsx":
            return constants.DataClassificationDocumentType.EXCEL
        elif self.data_class_doc_path.suffix == ".json":
            return constants.DataClassificationDocumentType.JSON
        else:
            raise ValueError(
                f"Unsupported document type: {self.data_class_doc_path.suffix}"
            )

    def load(self) -> None:
        """
        Load the data classification information from the document.

        This method will load the data classification information from the
        document specified in the constructor, and merge it with the
        constants.DATA_TO_MASK list.
        """
        if self.data_class_doc_type == constants.DataClassificationDocumentType.EXCEL:
            self.load_xlsx()
        elif self.data_class_doc_type == constants.DataClassificationDocumentType.JSON:
            self.load_json()
        # load overrides from config
        self.load_config_data_class_overrides()


    def load_json(self) -> None:
        """
        Load the data classification information from a JSON document.
        """
        logger.debug("Loading data classification from JSON document %s",
                     self.data_class_doc_path)
        with open(self.data_class_doc_path, "r") as f:
            json_struct = json.load(f)

        if not self.dc_struct:
            self.dc_struct = {}

        for table_name in json_struct:
            logger.debug("Processing table %s", table_name)
            for column_name in json_struct[table_name]:
                class_rec = data_types.DataToMask(
                    table_name=table_name,
                    schema=self.schema,
                    column_name=column_name,
                    faker_method=None,
                    percent_null=0,
                )
                if table_name not in self.dc_struct:
                    self.dc_struct[table_name] = {}
                self.dc_struct[table_name][column_name] = class_rec


    def load_config_data_class_overrides(self) -> None:
        """
        Load the data classification overrides from the config file.

        This method will load the data classification overrides from the
        config file and merge them with the data classification information.

        data classifications from config will take precedence over data
        classifications from the spreadsheet.
        """
        if not constants.DATA_TO_MASK:
            logger.warning(
                "No data classification overrides defined in the config file."
            )
            return

        for override in constants.DATA_TO_MASK:
            tab = override.table_name
            col = override.column_name
            if tab not in self.dc_struct:
                self.dc_struct[tab] = {}
            if (
                col in self.dc_struct[tab]
                and override.ignore
            ):
                # remove the column from the data classification
                logger.warning("override for %s %s", tab, col)
                del self.dc_struct[tab][col]
            else:
                self.dc_struct[tab][col] = override

    def load_xlsx(self) -> None:
        """
        Merge classifications in constants with classes defined in the ss.
        """
        self.dc_struct = {}
        if not self.valid_sheets:
            logger.warning(
                "No valid sheets defined for the data classification document, "
                "will not load any data from the document.  Call  "
                "set_valid_sheets() to define valid sheets."
            )
            return
        for sheet_name in self.valid_sheets:
            dfs = pd.read_excel(self.ss_path, sheet_name=sheet_name)
            #'TABLE_NAME' / 'COLUMN NAME' / 'INFO SECURITY CLASS'
            subset_df = dfs.loc[
                dfs["INFO SECURITY CLASS"].str.lower() != "public",
                ["TABLE NAME", "COLUMN NAME", "INFO SECURITY CLASS"],
            ]
            for _index, row in subset_df.iterrows():
                tab = row["TABLE NAME"].upper()
                col = row["COLUMN NAME"].upper()

                class_rec = data_types.DataToMask(
                    table_name=tab,
                    schema=self.schema,
                    column_name=col,
                    faker_method=None,
                    percent_null=0,
                )
                if tab not in self.dc_struct:
                    self.dc_struct[tab] = {}
                if col not in self.dc_struct[tab]:
                    self.dc_struct[tab][col] = class_rec
        # # load the data classification defined in the constants,
        # # remove any classifications that have been identified as ignore
        # for class_rec in constants.DATA_TO_MASK:
        #     tab = class_rec.table_name
        #     col = class_rec.column_name
        #     if tab not in self.dc_struct:
        #         self.dc_struct[tab] = {}
        #     if (
        #         col in self.dc_struct[tab]
        #         and hasattr(self.dc_struct[tab][col], "ignore")
        #         and class_rec.ignore
        #     ):
        #         # remove the column from the data classification
        #         LOGGER.warning("override for %s %s", tab, col)
        #         del self.dc_struct[tab][col]
        #     else:
        #         self.dc_struct[tab][col] = class_rec

