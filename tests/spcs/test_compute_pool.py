from unittest.mock import Mock, patch

from snowflake.cli.plugins.spcs.compute_pool.manager import ComputePoolManager
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.cli.api.project.util import to_string_literal
import json


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_create(mock_execute_query):
    pool_name = "test_pool"
    min_nodes = 2
    max_nodes = 3
    instance_family = "test_family"
    auto_resume = True
    initially_suspended = False
    auto_suspend_secs = 7200
    comment = "'test comment'"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().create(
        pool_name=pool_name,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        instance_family=instance_family,
        auto_resume=auto_resume,
        initially_suspended=initially_suspended,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
    )
    expected_query = " ".join(
        [
            "CREATE COMPUTE POOL test_pool",
            "MIN_NODES = 2",
            "MAX_NODES = 3",
            "INSTANCE_FAMILY = test_family",
            "AUTO_RESUME = True",
            "INITIALLY_SUSPENDED = False",
            "AUTO_SUSPEND_SECS = 7200",
            "COMMENT = 'test comment'",
        ]
    )
    actual_query = " ".join(mock_execute_query.mock_calls[0].args[0].split())
    assert expected_query == actual_query
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.create")
def test_create_pool_cli_defaults(mock_create, runner):
    result = runner.invoke(
        [
            "spcs",
            "compute-pool",
            "create",
            "test_pool",
            "--family",
            "test_family",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_create.assert_called_once_with(
        pool_name="test_pool",
        min_nodes=1,
        max_nodes=1,
        instance_family="test_family",
        auto_resume=True,
        initially_suspended=False,
        auto_suspend_secs=3600,
        comment=None,
    )


@patch("snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.create")
def test_create_pool_cli(mock_create, runner):
    result = runner.invoke(
        [
            "spcs",
            "compute-pool",
            "create",
            "test_pool",
            "--min-nodes",
            "2",
            "--max-nodes",
            "3",
            "--family",
            "test_family",
            "--no-auto-resume",
            "--init-suspend",
            "--auto-suspend-secs",
            "7200",
            "--comment",
            "this is a test",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_create.assert_called_once_with(
        pool_name="test_pool",
        min_nodes=2,
        max_nodes=3,
        instance_family="test_family",
        auto_resume=False,
        initially_suspended=True,
        auto_suspend_secs=7200,
        comment=to_string_literal("this is a test"),
    )


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_stop(mock_execute_query):
    pool_name = "test_pool"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().stop(pool_name)
    expected_query = "alter compute pool test_pool stop all"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_suspend(mock_execute_query):
    pool_name = "test_pool"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().suspend(pool_name)
    expected_query = "alter compute pool test_pool suspend"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.suspend")
def test_suspend_cli(mock_suspend, mock_cursor, runner):
    pool_name = "test_pool"
    cursor = mock_cursor(
        rows=[["Statement executed successfully."]], columns=["status"]
    )
    mock_suspend.return_value = cursor
    result = runner.invoke(["spcs", "compute-pool", "suspend", pool_name])
    mock_suspend.assert_called_once_with(pool_name)
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output

    cursor_copy = mock_cursor(
        rows=[["Statement executed successfully."]], columns=["status"]
    )
    mock_suspend.return_value = cursor_copy
    result_json = runner.invoke(
        ["spcs", "compute-pool", "suspend", pool_name, "--format", "json"]
    )
    result_json_parsed = json.loads(result_json.output)
    assert isinstance(result_json_parsed, dict)
    assert result_json_parsed == {"status": "Statement executed successfully."}


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_resume(mock_execute_query):
    pool_name = "test_pool"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().resume(pool_name)
    expected_query = "alter compute pool test_pool resume"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.resume")
def test_resume_cli(mock_resume, mock_cursor, runner):
    pool_name = "test_pool"
    cursor = mock_cursor(
        rows=[["Statement executed successfully."]], columns=["status"]
    )
    mock_resume.return_value = cursor
    result = runner.invoke(["spcs", "compute-pool", "resume", pool_name])
    mock_resume.assert_called_once_with(pool_name)
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output

    cursor_copy = mock_cursor(
        rows=[["Statement executed successfully."]], columns=["status"]
    )
    mock_resume.return_value = cursor_copy
    result_json = runner.invoke(
        ["spcs", "compute-pool", "resume", pool_name, "--format", "json"]
    )
    result_json_parsed = json.loads(result_json.output)
    assert isinstance(result_json_parsed, dict)
    assert result_json_parsed == {"status": "Statement executed successfully."}
