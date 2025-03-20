from unittest import mock

import pytest


def test_providing_to_time_and_refresh_causes_error(runner):

    result = runner.invoke(
        [
            "logs",
            "table",
            "test_table",
            "--to",
            '"2022-02-02 02:02:02"',
            "--refresh",
            "5",
        ]
    )
    assert result.exit_code == 1
    assert "You cannot set both --refresh and --to parameters" in result.output


@mock.patch("snowflake.cli._plugins.logs.manager.LogsManager.logs_table")
def test_providing_to_time_earlier_than_from_time_causes_error(mock_table, runner):
    mock_table.return_value = "test_table"

    result = runner.invoke(
        [
            "logs",
            "table",
            "test_table",
            "--from",
            "2022-02-02 02:02:02",
            "--to",
            "2022-02-01 02:02:02",
        ]
    )
    assert result.exit_code == 1
    assert "From_time cannot be later than to_time" in result.output


@pytest.mark.parametrize("parameter", ["--from", "--to"])
@pytest.mark.parametrize(
    "time_string",
    [
        "2024-11-03 12:00:00 UTC",
        "2024.11.03 12 00",
        "About one hour ago",
        "92348573948753202",
    ],
)
def test_providing_time_in_incorrect_format_causes_error(
    time_string, parameter, runner, snapshot
):
    result = runner.invoke(["logs", "compute_pool", "foo", parameter, time_string])

    assert result.exit_code == 1
    assert result.output == snapshot


@mock.patch(
    "snowflake.cli._plugins.logs.manager.LogsManager.logs_table",
    new_callable=mock.PropertyMock,
)
def test_correct_query_is_constructed(
    mock_table, mock_connect, mock_ctx, runner, snapshot
):
    mock_table.return_value = "foo"
    ctx = mock_ctx()
    mock_connect.return_value = ctx

    _ = runner.invoke(
        [
            "logs",
            "compute_pool",
            "bar",
            "--from",
            "2022-02-02 02:02:02",
            "--to",
            "2022-02-03 02:02:02",
        ]
    )

    queries = ctx.get_queries()
    assert len(queries) == 1
    assert queries[0] == snapshot
