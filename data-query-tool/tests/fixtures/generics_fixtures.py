import logging
import pathlib

import pytest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def data_dir():
    mod_dir = pathlib.Path(__file__).parent
    data_dir = (mod_dir / "..").resolve()
    yield data_dir
