from pathlib import Path
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.streamlit.streamlit_entity_model import StreamlitEntityModel
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext

CONNECTOR = "snowflake.connector.connect"
CONTEXT = ""
EXECUTE_QUERY = "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"

GET_UI_PARAMETERS = "snowflake.cli._plugins.connection.util.get_ui_parameters"


@pytest.fixture
def example_streamlit_workspace(project_directory):
    with project_directory("example_streamlit_v2") as pdir:
        with Path(pdir / "snowflake.yml").open() as definition_file:
            definition = yaml.safe_load(definition_file)
            print(definition)
            model = StreamlitEntityModel(
                **definition.get("entities", {}).get("test_streamlit")
            )

            workspace_context = WorkspaceContext(
                console=mock.MagicMock(),
                project_root=pdir,
                get_default_role=lambda: "test_role",
                get_default_warehouse=lambda: "test_warehouse",
            )

            return (
                StreamlitEntity(workspace_ctx=workspace_context, entity_model=model),
                ActionContext(
                    get_entity=lambda *args: None,
                ),
            )


def test_bundle(example_streamlit_workspace):

    entity, action_ctx = example_streamlit_workspace
    entity.action_bundle(action_ctx)

    output = entity.root / "output" / entity._entity_model.stage  # noqa
    assert output.exists()
    assert (output / "streamlit_app.py").exists()
    assert (output / "environment.yml").exists()
    assert (output / "pages" / "my_page.py").exists()


@mock.patch(EXECUTE_QUERY)
def test_drop(mock_execute, example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    entity.action_drop(action_ctx)

    mock_execute.assert_called_with("DROP STREAMLIT test_streamlit_deploy_snowcli")


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
def test_execute(mock_execute, example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    entity.action_execute(action_ctx)

    mock_execute.assert_called_with("EXECUTE STREAMLIT test_streamlit_deploy_snowcli()")


def test_get_execute_sql(example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    execute_sql = entity.action_get_execute_sql(action_ctx)

    assert execute_sql == "EXECUTE STREAMLIT test_streamlit_deploy_snowcli()"


def test_get_drop_sql(example_streamlit_workspace):
    entity, action_ctx = example_streamlit_workspace
    drop_sql = entity.action_get_drop_sql(action_ctx)

    assert drop_sql == "DROP STREAMLIT test_streamlit_deploy_snowcli"
