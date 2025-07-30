import logging
import os
import pathlib

import oradb_lib

LOGGER = logging.getLogger(__name__)


def test_data_classification(data_classification_obj):
    """
    Test the loading of data classification from an Excel file.
    """
    dc = data_classification_obj
    # Load the data classification
    dc.load()

    # Check if the data classification is loaded correctly
    assert dc.dc_struct
    LOGGER.debug("Data Classification Structure: %s", dc.dc_struct)
    # assert not data_classification_obj.data_classification_df.empty

    assert "CLIENT_LOCATION" in dc.dc_struct
