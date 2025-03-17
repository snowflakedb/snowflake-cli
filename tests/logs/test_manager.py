from datetime import datetime, timedelta

import pytest
from click import ClickException
from snowflake.cli._plugins.logs.commands import (
    DATETIME_FORMAT,
    get_datetime_from_string,
)
from snowflake.cli._plugins.logs.manager import LogsManager


def test_correct_parsing_of_string_to_datetime():
    time_string = "2022-02-02 02:02:02"

    assert get_datetime_from_string(time_string) == datetime.strptime(
        time_string, DATETIME_FORMAT
    )


@pytest.mark.parametrize(
    "time_string",
    [
        "2022-22-22 12:00:00",
        "2024-11-03",
        "2024-11-03 12:00:00:00",
        "About one hour ago" "92348573948753202",
    ],
)
def test_if_error_is_raise_for_incorrect_time_string(time_string):
    with pytest.raises(ClickException) as e:
        get_datetime_from_string(time_string, "test_value")

    assert (
        str(e.value)
        == "Incorrect format for 'test_value'. Please use '%Y-%m-%d %H:%M:%S' format."
    )
    assert e.value.exit_code == 1


def test_if_passing_to_time_earlier_than_from_time_raiser_error():
    from_time = datetime.now()
    to_time = from_time - timedelta(hours=1)

    with pytest.raises(ClickException) as e:
        LogsManager()._get_timestamp_query(  # noqa
            from_time=from_time.strftime(DATETIME_FORMAT),
            to_time=to_time.strftime(DATETIME_FORMAT),
        )

    assert (
        str(e.value)
        == "From_time cannot be later than to_time. Please check the values"
    )
    assert e.value.exit_code == 1
