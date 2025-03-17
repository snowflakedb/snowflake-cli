import pytest
from datetime import datetime, timedelta

from snowflake.cli._plugins.logs.commands import DATETIME_FORMAT
from snowflake.cli._plugins.logs.manager import get_timestamps_from_log_messages


@pytest.mark.integration
def test_logs_with_from_and_to_date(runner):
    eight_hours_ago = datetime.now() - timedelta(hours=8)
    six_hours_ago = datetime.now() - timedelta(hours=6)
    result = runner.invoke_with_connection_json(
        [
            "logs",
            "compute_pool",
            "SNOWCLI_COMPUTE_POOL",
            "--from",
            eight_hours_ago.strftime(DATETIME_FORMAT),
            "--to",
            six_hours_ago.strftime(DATETIME_FORMAT),
        ]
    )
    dates_in_output = get_timestamps_from_log_messages(result.output)

    assert dates_in_output
    assert all(date > eight_hours_ago for date in dates_in_output)
    assert all(date < six_hours_ago for date in dates_in_output)
