from unittest import mock


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_repl_input_handling(mock_execute, runner, mock_cursor, snapshot):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))
    result = runner.invoke(["sql"])
    assert result.exit_code == 0
    snapshot.assert_match(result.output)
