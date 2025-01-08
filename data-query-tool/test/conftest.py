import logging
import sys
import pathlib

import pytest

file_dir = pathlib.Path(__file__).parent

sys.path.append(file_dir / "..")
sys.path.append(file_dir / ".")


LOGGER = logging.getLogger(__name__)

pytest_plugins = [
    "fixtures.oracle_struct_fixtures",
]
testSession = None
