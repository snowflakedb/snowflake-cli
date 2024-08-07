# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from unittest.mock import Mock, patch

import pytest
from click import ClickException
from snowflake.cli._plugins.spcs.common import (
    NoPropertiesProvidedError,
)
from snowflake.cli._plugins.spcs.compute_pool.commands import (
    _compute_pool_name_callback,
)
from snowflake.cli._plugins.spcs.compute_pool.manager import ComputePoolManager
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_string_literal
from snowflake.connector.cursor import SnowflakeCursor

from tests.spcs.test_common import SPCS_OBJECT_EXISTS_ERROR
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_executed_successfully,
)


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
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
        if_not_exists=False,
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


@patch("snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.create")
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
        if_not_exists=False,
    )


@patch("snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.create")
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
            "--if-not-exists",
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
        if_not_exists=True,
    )


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
@patch("snowflake.cli._plugins.spcs.compute_pool.manager.handle_object_already_exists")
def test_create_compute_pool_already_exists(mock_handle, mock_execute):
    pool_name = "test_pool"
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
        if_not_exists=False,
    )
    mock_handle.assert_called_once_with(
        SPCS_OBJECT_EXISTS_ERROR, ObjectType.COMPUTE_POOL, pool_name
    )


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_create_compute_pool_if_not_exists(mock_execute_query):
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().create(
        pool_name="test_pool",
        min_nodes=1,
        max_nodes=1,
        instance_family="test_family",
        auto_resume=True,
        initially_suspended=False,
        auto_suspend_secs=3600,
        comment=None,
        if_not_exists=True,
    )
    expected_query = " ".join(
        [
            "CREATE COMPUTE POOL IF NOT EXISTS test_pool",
            "MIN_NODES = 1",
            "MAX_NODES = 1",
            "INSTANCE_FAMILY = test_family",
            "AUTO_RESUME = True",
            "INITIALLY_SUSPENDED = False",
            "AUTO_SUSPEND_SECS = 3600",
        ]
    )
    actual_query = " ".join(mock_execute_query.mock_calls[0].args[0].split())
    assert expected_query == actual_query
    assert result == cursor


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
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
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_suspend(mock_execute_query):
    pool_name = "test_pool"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().suspend(pool_name)
    expected_query = "alter compute pool test_pool suspend"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.suspend")
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
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_resume(mock_execute_query):
    pool_name = "test_pool"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().resume(pool_name)
    expected_query = "alter compute pool test_pool resume"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.resume")
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


@patch("snowflake.cli._plugins.spcs.compute_pool.commands.is_valid_object_name")
def test_compute_pool_name_callback(mock_is_valid):
    name = "test_pool"
    mock_is_valid.return_value = True
    fqn = FQN.from_string(name)
    assert _compute_pool_name_callback(fqn) == fqn


@patch("snowflake.cli._plugins.spcs.compute_pool.commands.is_valid_object_name")
def test_compute_pool_name_callback_invalid(mock_is_valid):
    name = "test_pool"
    mock_is_valid.return_value = False
    with pytest.raises(ClickException) as e:
        _compute_pool_name_callback(FQN.from_string(name))
    assert "is not a valid compute pool name." in e.value.message


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
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
    expected_query = "\n".join(
        [
            f"alter compute pool {pool_name} set",
            f"min_nodes = {min_nodes}",
            f"max_nodes = {max_nodes}",
            f"auto_resume = {auto_resume}",
            f"auto_suspend_secs = {auto_suspend_secs}",
            f"comment = {comment}",
        ]
    )
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


def test_set_property_no_properties():
    pool_name = "test_pool"
    with pytest.raises(NoPropertiesProvidedError) as e:
        ComputePoolManager().set_property(pool_name, None, None, None, None, None)
    assert (
        e.value.message
        == f"No properties specified for compute pool '{pool_name}'. Please provide at least one property to set."
    )


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.set_property"
)
def test_set_property_cli(mock_set, mock_statement_success, runner):
    cursor = mock_statement_success()
    mock_set.return_value = cursor
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
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.set_property"
)
def test_set_property_no_properties_cli(mock_set, runner):
    pool_name = "test_pool"
    mock_set.side_effect = NoPropertiesProvidedError(
        f"No properties specified for compute pool '{pool_name}'. Please provide at least one property to set."
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
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
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
        == f"No properties specified for compute pool '{pool_name}'. Please provide at least one property to reset to its default value."
    )


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.unset_property"
)
def test_unset_property_cli(mock_unset, mock_statement_success, runner):
    cursor = mock_statement_success()
    mock_unset.return_value = cursor
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
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.unset_property"
)
def test_unset_property_no_properties_cli(mock_unset, runner):
    pool_name = "test_pool"
    mock_unset.side_effect = NoPropertiesProvidedError(
        f"No properties specified for compute pool '{pool_name}'. Please provide at least one property to reset to its default value."
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


@patch(
    "snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager._execute_query"
)
def test_status(mock_execute_query):
    pool_name = "test_pool"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ComputePoolManager().status(pool_name=pool_name)
    expected_query = f"call system$get_compute_pool_status('{pool_name}')"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.compute_pool.manager.ComputePoolManager.status")
def test_status_cli(mock_status, mock_statement_success, runner):
    pool_name = "test_pool"
    mock_status.return_value = mock_statement_success()
    result = runner.invoke(["spcs", "compute-pool", "status", pool_name])
    mock_status.assert_called_once_with(pool_name=pool_name)
    assert_that_result_is_successful_and_executed_successfully(result)


@patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "command, parameters",
    [
        ("list", []),
        ("list", ["--like", "PATTERN"]),
        ("describe", ["NAME"]),
        ("drop", ["NAME"]),
    ],
)
def test_command_aliases(mock_connector, runner, mock_ctx, command, parameters):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", command, "compute-pool", *parameters])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["spcs", "compute-pool", command, *parameters], catch_exceptions=False
    )
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert queries[0] == queries[1]
