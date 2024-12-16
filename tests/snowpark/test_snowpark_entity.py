from pathlib import Path
from unittest import mock

import pytest
import yaml
from markdown_it.rules_inline import entity

from snowflake.cli._plugins.snowpark.snowpark_entity import FunctionEntity
from snowflake.cli._plugins.snowpark.snowpark_entity_model import FunctionEntityModel
from snowflake.cli._plugins.workspace.context import WorkspaceContext, ActionContext
from testing_utils.mock_config import mock_config_key

CONNECTOR = "snowflake.connector.connect"
CONTEXT = ""
EXECUTE_QUERY = "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"


@pytest.fixture
def example_function_workspace(project_directory): #TODO: try to make a common fixture for all entities
    with mock_config_key("enable_native_app_children", True):
        with project_directory("snowpark_functions_v2") as pdir:
            with Path(pdir / "snowflake.yml").open() as definition_file:
                definition = yaml.safe_load(definition_file)
                model = FunctionEntityModel(
                    **definition.get("entities", {}).get("func1")
                )

                workspace_context = WorkspaceContext(
                    console=mock.MagicMock(),
                    project_root=pdir,
                    get_default_role=lambda: "test_role",
                    get_default_warehouse=lambda: "test_warehouse",
                )

                return (
                    FunctionEntity(
                        workspace_ctx=workspace_context, entity_model=model
                    ),
                    ActionContext(
                        get_entity=lambda *args: None,
                    ),
                )


@mock.patch(EXECUTE_QUERY)
def test_action_describe(mock_execute, example_function_workspace):
    entity, action_context = example_function_workspace
    result = entity.action_describe(action_context)

    mock_execute.assert_called_with("DESCRIBE IDENTIFIER('func1')")

@mock.patch(EXECUTE_QUERY)
def test_action_drop(mock_execute, example_function_workspace):
    entity, action_context = example_function_workspace
    result = entity.action_drop(action_context)

    mock_execute.assert_called_with("DROP IDENTIFIER('func1')")

@pytest.mark.parametrize("execution_arguments", [None, ["arg1", "arg2"], ["foo",42,"bar"]])
@mock.patch(EXECUTE_QUERY)
def test_action_execute(mock_execute, execution_arguments, example_function_workspace, snapshot):
    entity, action_context = example_function_workspace
    result = entity.action_execute(action_context, execution_arguments)

    mock_execute.assert_called_with(snapshot)

def test_describe_sql(example_function_workspace):
    entity, _ = example_function_workspace
    assert entity.get_describe_sql() == "DESCRIBE IDENTIFIER('func1')"

def test_drop_sql(example_function_workspace):
    entity, _ = example_function_workspace
    assert entity.get_drop_sql() == "DROP IDENTIFIER('func1')"

@pytest.mark.parametrize("execution_arguments", [None, ["arg1", "arg2"], ["foo",42,"bar"]])
def test_function_get_execute_sql(execution_arguments, example_function_workspace, snapshot):
    entity, _ = example_function_workspace
    assert entity.get_execute_sql(execution_arguments) == snapshot