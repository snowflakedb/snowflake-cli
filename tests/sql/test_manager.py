from unittest import mock

from snowflake.cli._plugins.sql.manager import SqlManager


@mock.patch("snowflake.cli.api.sql_execution.BaseSqlExecutor._execute_string")
def test_execute_passed_correct_query(mock_execute_string):
    sql_manager = SqlManager()

    _, result_generator = sql_manager.execute(
        query="select 1; select 2", files=None, std_in=False
    )
    list(result_generator)

    assert mock_execute_string.call_count == 2
    executed_queries = [call.args[0] for call in mock_execute_string.call_args_list]
    assert executed_queries == ["select 1;", "select 2"]
