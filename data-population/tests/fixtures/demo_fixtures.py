"""
Demo fixtures, used to verify the test suite is working correctly.
"""

import pytest


@pytest.fixture(scope="module")
def demo_fixture():
    """
    Demonstration of a fixture.
    """
    yield "Demo Fixture"
