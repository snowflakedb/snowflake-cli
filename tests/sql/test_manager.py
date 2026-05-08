from unittest import mock

from snowflake.cli._plugins.sql.manager import SqlManager


@mock.patch("snowflake.cli.api.sql_execution.BaseSqlExecutor._execute_string")
def test_execute_passed_correct_query(mock_execute_string):
    sql_manager = SqlManager()

    # The execute method returns a generator. We must consume it to trigger the calls.
    _, result_generator = sql_manager.execute(
        query="select 1; select 2", files=None, std_in=False
    )
    list(result_generator)  # This consumes the generator

    # The query is split, so we expect two calls to _execute_string
    calls = [
        mock.call("select 1", cursor_class=mock.ANY),
        mock.call("select 2", cursor_class=mock.ANY),
    ]
    mock_execute_string.assert_has_calls(calls)
    assert mock_execute_string.call_count == 2 