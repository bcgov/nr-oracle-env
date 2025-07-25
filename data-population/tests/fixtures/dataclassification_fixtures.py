import logging
import os
import pathlib

import data_classification
import pytest

import db_env_utils.oradb_lib as oradb_lib

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def data_classification_file():
    """
    Path to the data classification file.
    """
    # Get the path to the current file
    data_dir = pathlib.Path(__file__).parent / ".." / "test_data"
    class_json = data_dir / "fixed_data_classification.json"
    # Return the absolute path of the Excel file

    yield class_json


@pytest.fixture(scope="module")
def data_classification_obj(data_classification_file):
    dc = data_classification.DataClassification(
        data_class_doc=data_classification_file,
        schema="THE",
    )
    yield dc
