import json
from unittest import mock

import pytest

from tests.testing_utils.fixtures import TEST_DIR


@pytest.fixture
def mock_available_packages_sql_result(mock_ctx, mock_cursor):
    with open(
        TEST_DIR / "test_data/packages_available_in_snowflake_sql_result_rows.json"
    ) as fh:
        result_rows = json.load(fh)
    ctx = mock_ctx(
        mock_cursor(
            columns=["package_name", "version"],
            rows=result_rows,
        )
    )
    with mock.patch(
        "snowflake.cli.app.snow_connector.connect_to_snowflake", return_value=ctx
    ):
        yield
