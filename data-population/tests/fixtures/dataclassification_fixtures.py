import logging
import os
import pathlib

import db_env_utils.oradb_lib as oradb_lib
import pytest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def data_classification_file():
    """
    Path to the data classification file.
    """
    # Get the path to the current file
    data_dir = pathlib.Path(__file__).parent / ".." / "test_data"
    class_ss = data_dir / "classification_ss.xlsx"
    # Return the absolute path of the Excel file

    yield class_ss


@pytest.fixture(scope="module")
def data_classification_obj(data_classification_file):
    dc = oradb_lib.DataClassification(
        data_class_ss_path=data_classification_file,
        schema="THE",
    )
    yield dc
