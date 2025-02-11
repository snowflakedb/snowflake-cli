from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.streamlit.streamlit_entity import (
    StreamlitEntity,
)
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext

from tests.testing_utils.mock_config import mock_config_key

STREAMLIT_NAME = "test_streamlit"
CONNECTOR = "snowflake.connector.connect"
CONTEXT = ""
EXECUTE_QUERY = "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"

GET_UI_PARAMETERS = "snowflake.cli._plugins.connection.util.get_ui_parameters"


@pytest.fixture
def example_streamlit_workspace(project_directory):
    with mock_config_key("enable_native_app_children", True):
        with project_directory("example_streamlit_v2") as pdir:
            with Path(pdir / "snowflake.yml").open() as definition_file:
                definition = yaml.safe_load(definition_file)
                model = StreamlitEntityModel(
                    **definition.get("entities", {}).get("test_streamlit")
                )

                workspace_context = WorkspaceContext(
                    console=mock.MagicMock(),
                    project_root=pdir,
                    get_default_role=lambda: "test_role",
                    get_default_warehouse=lambda: "test_warehouse",
                )

                yield (
                    StreamlitEntity(
                        workspace_ctx=workspace_context, entity_model=model
                    ),
                    ActionContext(
                        get_entity=lambda *args: None,
                    ),
                )


def test_cannot_instantiate_without_feature_flag():
    with pytest.raises(NotImplementedError) as err:
        StreamlitEntity()
    assert str(err.value) == "Streamlit entity is not implemented yet"


def test_nativeapp_children_interface(example_streamlit_workspace, snapshot):
    sl, action_context = example_streamlit_workspace

    sl.bundle()
    bundle_artifact = sl.root / "output" / "bundle" / "streamlit" / "streamlit_app.py"
    deploy_sql_str = sl.get_deploy_sql()
    grant_sql_str = sl.get_usage_grant_sql(app_role="app_role")

    assert bundle_artifact.exists()
    assert deploy_sql_str == snapshot
    assert (
        grant_sql_str == f"GRANT USAGE ON STREAMLIT None TO APPLICATION ROLE app_role;"
    )


def test_bundle(example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    entity.action_bundle(action_ctx)

    output = entity.root / "output" / "bundle" / "streamlit"  # noqa

    assert output.exists()
    assert (output / "streamlit_app.py").exists()
    assert (output / "environment.yml").exists()
    assert (output / "pages" / "my_page.py").exists()


@mock.patch(EXECUTE_QUERY)
def test_deploy(mock_execute, example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    entity.action_deploy(action_ctx)

    mock_execute.assert_called_with(
        f"CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')\nMAIN_FILE = 'streamlit_app.py'\nQUERY_WAREHOUSE = 'test_warehouse'\nTITLE = 'My Fancy Streamlit';"
    )


@mock.patch(EXECUTE_QUERY)
def test_drop(mock_execute, example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    entity.action_drop(action_ctx)

    mock_execute.assert_called_with(f"DROP STREAMLIT IDENTIFIER('{STREAMLIT_NAME}');")


@mock.patch(CONNECTOR)
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={"UI_SNOWSIGHT_ENABLE_REGIONLESS_REDIRECT": "false"},
)
@mock.patch("click.get_current_context")
def test_get_url(
    mock_get_ctx,
    mock_param,
    mock_connect,
    mock_cursor,
    example_streamlit_workspace,
    mock_ctx,
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"SYSTEM$RETURN_CURRENT_ORG_NAME()": "FOOBARBAZ"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connect.return_value = ctx
    mock_get_ctx.return_value = ctx

    entity, action_ctx = example_streamlit_workspace
    result = entity.action_get_url(action_ctx)

    mock_connect.assert_called()


@mock.patch(EXECUTE_QUERY)
def test_share(mock_connect, example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    entity.action_share(action_ctx, to_role="test_role")

    mock_connect.assert_called_with(
        "GRANT USAGE ON STREAMLIT IDENTIFIER('test_streamlit') TO ROLE test_role;"
    )


@mock.patch(EXECUTE_QUERY)
def test_execute(mock_execute, example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    entity.action_execute(action_ctx)

    mock_execute.assert_called_with(f"EXECUTE STREAMLIT {STREAMLIT_NAME}();")


def test_get_execute_sql(example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    execute_sql = entity.get_execute_sql()

    assert execute_sql == f"EXECUTE STREAMLIT {STREAMLIT_NAME}();"


def test_get_drop_sql(example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    drop_sql = entity.get_drop_sql()

    assert drop_sql == f"DROP STREAMLIT IDENTIFIER('{STREAMLIT_NAME}');"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"replace": True},
        {"if_not_exists": True},
        {"from_stage_name": "test_stage"},
        {"from_stage_name": "test_stage", "replace": True},
        {"from_stage_name": "test_stage", "if_not_exists": True},
    ],
)
def test_get_deploy_sql(kwargs, example_streamlit_workspace, snapshot):
    entity, action_ctx = example_streamlit_workspace
    deploy_sql = entity.get_deploy_sql(**kwargs)

    assert deploy_sql == snapshot
