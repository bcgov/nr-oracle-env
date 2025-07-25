import logging
import os
import pathlib

import oradb_lib

LOGGER = logging.getLogger(__name__)


def test_dc_load(data_classification_obj):
    """
    Test the loading of data classification from an Excel file.
    """
    dc = data_classification_obj
    # Load the data classification
    dc.load()

    # Check if the data classification is loaded correctly
    assert dc.dc_struct
    #LOGGER.debug("Data Classification Structure: %s", dc.dc_struct)
    # assert not data_classification_obj.data_classification_df.empty

    assert "CLIENT_LOCATION" in dc.dc_struct

    LOGGER.debug("mailing city: %s", dc.dc_struct['MAILING_CITY'])
    mask = dc.has_masking("MAILING_CITY")
    LOGGER.debug("mailing city has masking?: %s", mask)
    mask_info = dc.get_mask_info('MAILING_CITY', 'COUNTRY_NAME')
    LOGGER.debug("mask info: %s", mask_info)
    LOGGER.debug("country name mask info: %s", dc.get_mask_info('MAILING_CITY', 'COUNTRY_NAME'))
    LOGGER.debug("province state name mask info: %s", dc.get_mask_info('MAILING_CITY', 'PROVINCE_STATE_NAME'))
    LOGGER.debug("CITY_NAME name mask info: %s", dc.get_mask_info('MAILING_CITY', 'CITY_NAME'))

