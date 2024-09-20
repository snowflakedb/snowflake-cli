import unittest.mock as mock
from typing import Dict, Any

import pytest


class _MockTelemetryUtils:
    """
    collection of shorthand utilities for mocked telemetry object
    """

    def __init__(self, mocked_telemetry: mock.MagicMock):
        self.mocked_telemetry = mocked_telemetry

    def extract_first_result_executing_command_telemetry_message(
        self,
    ) -> Dict[str, Any]:
        return next(
            args.args[0].to_dict()["message"]
            for args in self.mocked_telemetry.call_args_list
            if args.args[0].to_dict().get("message").get("type")
            == "result_executing_command"
        )


@pytest.fixture
def mock_telemetry():
    """
    fixture for mocking telemetry calls, providing
    useful utility functions to validate calls
    """
    with mock.patch(
        "snowflake.connector.telemetry.TelemetryClient.try_add_log_to_batch"
    ) as mocked_telemetry:
        yield _MockTelemetryUtils(mocked_telemetry)
