from pathlib import Path
from unittest import mock

import pytest
import yaml

from snowflake.cli._plugins.snowpark.snowpark_entity import FunctionEntity
from snowflake.cli._plugins.snowpark.snowpark_entity_model import FunctionEntityModel
from snowflake.cli._plugins.workspace.context import WorkspaceContext, ActionContext
from testing_utils.mock_config import mock_config_key


@pytest.fixture
def example_function_workspace(project_directory):
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


def test_describe_sql(e):
