"""
Example of a test file.

Used to verify that the pytest configuration is setup correctly.
"""

import logging

import pytest

LOGGER = logging.getLogger(__name__)


def test_verify_test_framework_config():
    """
    Verify the test framework is configured correctly.
    """
    LOGGER.info("Demo Test is running")
    assert True


def test_verify_test_framework_fixtures_config(demo_fixture):
    """
    Verify the test framework is configured correctly.
    """
    LOGGER.info("Demo Test is running")
    assert demo_fixture == "Demo Fixture"
