import unittest.mock as mock

import pytest
from snowflake.cli.plugins.nativeapp.artifacts import BundleMap


@pytest.fixture()
def mock_bundle_map():
    yield mock.Mock(spec=BundleMap)
