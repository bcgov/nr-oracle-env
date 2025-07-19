"""
Configuration for the data classification utility.
"""
import enum

EXPECTED_COLUMNS = [
    "TABLE_NAME",
    "COLUMN_NAME",
    "SECURITY_CLASSIFICATION",
]

# these are the column names that are used in the excel spreadsheets
# to identify the name of the table that a security classification
# applies to
TABLE_COLUMN_NAMES = ["TABLE_NAME", "TABLE", "TABLE NAME"]
# ditto as above for for the name of the column that a securityclassification
# applies to
COLUMN_COLUMN_NAMES = ["COLUMN_NAME", "COLUMN", "COLUMN NAME"]
# ditto as above for the security classification itself
SECURITY_CLASSIFICATION_COLUMN_NAMES = [
            "INFO SECURITY CLASS",
            "CLASSIFICATION",
            "Information Security Classification",
            "ISC",
]

class ColumnIndexKeys(enum.Enum):
    """
    Enum for column index keys.
    """

    TABLE_NAME = "table_name"
    COLUMN_NAME = "column_name"
    SECURITY_CLASSIFICATION = "security_classification"

