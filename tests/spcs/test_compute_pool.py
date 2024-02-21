from unittest.mock import Mock, patch

from snowflake.cli.plugins.spcs.compute_pool.manager import ComputePoolManager
from snowflake.cli.plugins.spcs.compute_pool.commands import _compute_pool_name_callback
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.cli.api.project.util import to_string_literal
import pytest

from click import ClickException
from tests.spcs.test_common import SPCS_OBJECT_EXISTS_ERROR
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.plugins.spcs.common import (
    NoPropertiesProvidedError,
)
from tests_integration.testing_utils.assertions.test_result_assertions import assert_that_result_is_successful_and_executed_successfully
from tests.spcs.utils import assert_mock_execute_is_called_once_with_query


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
    expected_query = """
        create compute pool test_pool
        min_nodes = 2
        max_nodes = 3
        instance_family = test_family
        auto_resume = True
        initially_suspended = False
        auto_suspend_secs = 7200
        comment = 'test comment'
    """
    assert_mock_execute_is_called_once_with_query(mock_execute_query, expected_query)
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
@patch("snowflake.cli.plugins.spcs.compute_pool.manager.handle_object_already_exists")
def test_create_repository_already_exists(mock_handle, mock_execute):
    pool_name = "test_object"
    mock_execute.side_effect = SPCS_OBJECT_EXISTS_ERROR
    ComputePoolManager().create(
        pool_name=pool_name,
        min_nodes=1,
        max_nodes=1,
        instance_family="test_family",
        auto_resume=False,
        initially_suspended=True,
        auto_suspend_secs=7200,
        comment=to_string_literal("this is a test"),
    )
    mock_handle.assert_called_once_with(
        SPCS_OBJECT_EXISTS_ERROR, ObjectType.COMPUTE_POOL, pool_name
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
def test_suspend_cli(mock_suspend, mock_statement_success, runner):
    pool_name = "test_pool"
    mock_suspend.return_value = mock_statement_success()
    result = runner.invoke(["spcs", "compute-pool", "suspend", pool_name])
    mock_suspend.assert_called_once_with(pool_name)
    assert_that_result_is_successful_and_executed_successfully(result)


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
def test_resume_cli(mock_resume, mock_statement_success, runner):
    pool_name = "test_pool"
    mock_resume.return_value = mock_statement_success()
    result = runner.invoke(["spcs", "compute-pool", "resume", pool_name])
    mock_resume.assert_called_once_with(pool_name)
    assert_that_result_is_successful_and_executed_successfully(result)


@patch("snowflake.cli.plugins.spcs.compute_pool.commands.is_valid_object_name")
def test_compute_pool_name_callback(mock_is_valid):
    name = "test_pool"
    mock_is_valid.return_value = True
    assert _compute_pool_name_callback(name) == name


@patch("snowflake.cli.plugins.spcs.compute_pool.commands.is_valid_object_name")
def test_compute_pool_name_callback_invalid(mock_is_valid):
    name = "test_pool"
    mock_is_valid.return_value = False
    with pytest.raises(ClickException) as e:
        _compute_pool_name_callback(name)
    assert "is not a valid compute pool name." in e.value.message


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_set_property(mock_execute_query):
    pool_name = "test_pool"
    min_nodes = 2
    max_nodes = 3
    auto_resume = False
    auto_suspend_secs = 7200
    comment = to_string_literal("this is a test")
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().set_property(
        pool_name, min_nodes, max_nodes, auto_resume, auto_suspend_secs, comment
    )
    expected_query = f"""
        alter compute pool {pool_name} set
        min_nodes = {min_nodes}
        max_nodes = {max_nodes}
        auto_resume = {auto_resume}
        auto_suspend_secs = {auto_suspend_secs}
        comment = {comment}
    """
    assert_mock_execute_is_called_once_with_query(mock_execute_query, expected_query)
    assert result == cursor


def test_set_property_no_properties():
    pool_name = "test_pool"
    with pytest.raises(NoPropertiesProvidedError) as e:
        ComputePoolManager().set_property(pool_name, None, None, None, None, None)
    assert (
        e.value.message
        == ComputePoolManager.set_no_properties_message
    )


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.set_property"
)
def test_set_property_cli(mock_set, mock_statement_success, runner):
    mock_set.return_value = mock_statement_success()
    pool_name = "test_pool"
    min_nodes = 2
    max_nodes = 3
    auto_resume = False
    auto_suspend_secs = 7200
    comment = "this is a test"
    result = runner.invoke(
        [
            "spcs",
            "compute-pool",
            "set",
            pool_name,
            "--min-nodes",
            str(min_nodes),
            "--max-nodes",
            str(max_nodes),
            "--no-auto-resume",
            "--auto-suspend-secs",
            auto_suspend_secs,
            "--comment",
            comment,
        ]
    )
    mock_set.assert_called_once_with(
        pool_name=pool_name,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        auto_resume=auto_resume,
        auto_suspend_secs=auto_suspend_secs,
        comment=to_string_literal(comment),
    )
    assert_that_result_is_successful_and_executed_successfully(result)


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.set_property"
)
def test_set_property_no_properties_cli(mock_set, runner):
    pool_name = "test_pool"
    mock_set.side_effect = NoPropertiesProvidedError(
        ComputePoolManager.set_no_properties_message
    )
    result = runner.invoke(["spcs", "compute-pool", "set", pool_name])
    assert result.exit_code == 1, result.output
    assert "No properties specified" in result.output
    mock_set.assert_called_once_with(
        pool_name=pool_name,
        min_nodes=None,
        max_nodes=None,
        auto_resume=None,
        auto_suspend_secs=None,
        comment=None,
    )


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_unset_property(mock_execute_query):
    pool_name = "test_pool"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().unset_property(pool_name, True, True, True)
    expected_query = (
        "alter compute pool test_pool unset auto_resume,auto_suspend_secs,comment"
    )
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


def test_unset_property_no_properties():
    pool_name = "test_pool"
    with pytest.raises(NoPropertiesProvidedError) as e:
        ComputePoolManager().unset_property(pool_name, False, False, False)
    assert (
        e.value.message
        == ComputePoolManager.unset_no_properties_message
    )


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.unset_property"
)
def test_unset_property_cli(mock_unset, mock_statement_success, runner):
    mock_unset.return_value = mock_statement_success()
    pool_name = "test_pool"
    result = runner.invoke(
        [
            "spcs",
            "compute-pool",
            "unset",
            pool_name,
            "--auto-resume",
            "--auto-suspend-secs",
            "--comment",
        ]
    )
    mock_unset.assert_called_once_with(
        pool_name=pool_name, auto_resume=True, auto_suspend_secs=True, comment=True
    )
    assert_that_result_is_successful_and_executed_successfully(result)


@patch(
    "snowflake.cli.plugins.spcs.compute_pool.manager.ComputePoolManager.unset_property"
)
def test_unset_property_no_properties_cli(mock_unset, runner):
    pool_name = "test_pool"
    mock_unset.side_effect = NoPropertiesProvidedError(
        ComputePoolManager.unset_no_properties_message
    )
    result = runner.invoke(["spcs", "compute-pool", "unset", pool_name])
    assert result.exit_code == 1, result.output
    assert "No properties specified" in result.output
    mock_unset.assert_called_once_with(
        pool_name=pool_name, auto_resume=False, auto_suspend_secs=False, comment=False
    )


def test_unset_property_with_args(runner):
    pool_name = "test_pool"
    result = runner.invoke(
        ["spcs", "compute-pool", "unset", pool_name, "--auto-suspend-secs", "1"]
    )
    assert result.exit_code == 2, result.output
    assert "Got unexpected extra argument" in result.output
