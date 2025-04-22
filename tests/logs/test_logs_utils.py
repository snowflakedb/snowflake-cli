from datetime import datetime, timedelta, timezone

import pytest
from click import ClickException
from snowflake.cli._plugins.logs.commands import (
    get_datetime_from_string,
)
from snowflake.cli._plugins.logs.utils import (
    get_log_levels,
    get_timestamp_query,
    parse_log_levels_for_query,
)

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


@pytest.mark.parametrize(
    "time_string",
    [  # Should cover all possible formats accepted by datetime.fromisoformat
        "2022-02-02T02:02:02",  # Basic date and time
        "2022-02-02 02:02:02",  # Basic date and time withouth T
        "2022-02-02",  # Date only
        "2022-02-02T02:02",  # Date and time with minutes
        "2022-02-02T02:02+07:00",  # Date and time with minutes and timezone
        "2022-02-02 02:02+07:00",  # Date and time with minutes and timezone without T
        "2022-02-02T02:02:02",  # Date and time with second
        "2022-02-02T02:02:02+07:00",  # Date and time with second and timezone
        "2022-02-02T02:02:02.123456",  # Date and time with microseconds
        "2022-02-02T02:02:02+00:00",  # Date and time with timezone
        "2022-02-02T02:02:02.123456+00:00",  # Date and time with microseconds and timezone
    ],
)
def test_correct_parsing_of_string_to_datetime(time_string):
    result = get_datetime_from_string(time_string)

    assert result.year == 2022
    assert result.month == 2
    assert result.day == 2


@pytest.mark.parametrize(
    "time_string",
    [
        "2022-22-22 12:00:00",
        "2024-11-03 12:00:00 UTC",
        "About one hour ago" "92348573948753202",
    ],
)
def test_if_error_is_raise_for_incorrect_time_string(time_string):
    with pytest.raises(ClickException) as e:
        get_datetime_from_string(time_string, "test_value")

    assert (
        str(e.value)
        == "Incorrect format for 'test_value'. Please use one of approved ISO formats."
    )
    assert e.value.exit_code == 1


def test_timezone_is_parsed_properly():
    time_string = "2024-11-03T12:00:00+02:00"
    result = get_datetime_from_string(time_string)
    assert result.year == 2024
    assert result.month == 11
    assert result.day == 3
    assert result.hour == 12
    assert result.minute == 0
    assert result.tzinfo is not None
    assert result.tzinfo.utcoffset(result) == timedelta(hours=2)
    assert (
        result.astimezone(timezone.utc).strftime("%Y-%m-%d %H-%M-%S")
        == "2024-11-03 10-00-00"
    )


def test_if_passing_to_time_earlier_than_from_time_raiser_error():
    from_time = datetime.now()
    to_time = from_time - timedelta(hours=1)

    with pytest.raises(ClickException) as e:
        get_timestamp_query(from_time=from_time, to_time=to_time)  # noqa

    assert (
        str(e.value)
        == "From_time cannot be later than to_time. Please check the values"
    )
    assert e.value.exit_code == 1


@pytest.mark.parametrize(
    "log_level,expected",
    [
        ("", ["INFO", "WARN", "ERROR", "FATAL"]),
        ("TRACE", ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"]),
        ("DEBUG", ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]),
        ("INFO", ["INFO", "WARN", "ERROR", "FATAL"]),
        ("WARN", ["WARN", "ERROR", "FATAL"]),
        ("ERROR", ["ERROR", "FATAL"]),
        ("FATAL", ["FATAL"]),
        ("fatal", ["FATAL"]),
        ("eRrOr", ["ERROR", "FATAL"]),
    ],
)
def test_if_log_levels_list_is_correctly_filtered(log_level, expected):
    result = get_log_levels(log_level)

    assert result == expected


@pytest.mark.parametrize(
    "level,expected",
    [
        ("", "'INFO', 'WARN', 'ERROR', 'FATAL'"),
        ("INFO", "'INFO', 'WARN', 'ERROR', 'FATAL'"),
        ("DEBUG", "'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'"),
        ("wArN", "'WARN', 'ERROR', 'FATAL'"),
    ],
)
def test_if_log_level_gives_correct_query(level, expected):
    result = parse_log_levels_for_query(level)

    assert result == expected
