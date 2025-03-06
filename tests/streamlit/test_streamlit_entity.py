from __future__ import annotations

from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
import yaml
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.streamlit.streamlit_entity_model import StreamlitEntityModel
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.core.stage import StageResource

from tests.testing_utils.mock_config import mock_config_key

STREAMLIT_NAME = "test_streamlit"
CONNECTOR = "snowflake.connector.connect"
CONTEXT = ""
EXECUTE_QUERY = "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"

GET_UI_PARAMETERS = "snowflake.cli._plugins.connection.util.get_ui_parameters"


class TestStreamlitEntity:
    @pytest.fixture(autouse=True)
    def setup(self, project_directory):
        with mock_config_key("enable_native_app_children", True):
            with project_directory("example_streamlit_v2") as pdir:
                with Path(pdir / "snowflake.yml").open() as definition_file:
                    definition = yaml.safe_load(definition_file)
                    model = StreamlitEntityModel(
                        **definition.get("entities", {}).get("test_streamlit")
                    )
                    model.set_entity_id("test_streamlit")

                    workspace_context = WorkspaceContext(
                        console=mock.MagicMock(),
                        project_root=pdir,
                        get_default_role=lambda: "test_role",
                        get_default_warehouse=lambda: "test_warehouse",
                    )

                    self.entity = StreamlitEntity(
                        workspace_ctx=workspace_context, entity_model=model
                    )
                    self.action_ctx = ActionContext(
                        get_entity=lambda *args: None,
                    )

        self.mock_conn = mock.patch(
            "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._conn"
        ).start()
        self.mock_conn.schema = None
        self.mock_conn.database = None

        self.mock_execute = mock.patch(EXECUTE_QUERY).start()

        mock_stage_resource = StageResource(
            name="stage_resource_mock", collection=MagicMock()
        )
        self.mock_create_stage = mock.patch(
            "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._create_stage_if_not_exists",
            return_value=mock_stage_resource,
        ).start()

        # self.mock_make_url = mock.patch("snowflake.cli._plugins.connection.util.make_snowsight_url").start()

    def test_nativeapp_children_interface(self, snapshot):
        self.entity.bundle()
        bundle_artifact = (
            self.entity.root
            / "output"
            / "bundle"
            / "streamlit"
            / STREAMLIT_NAME
            / "streamlit_app.py"
        )
        deploy_sql_str = self.entity.get_deploy_sql()
        grant_sql_str = self.entity.get_usage_grant_sql(app_role="app_role")

        assert bundle_artifact.exists()
        assert deploy_sql_str == snapshot
        assert (
            grant_sql_str
            == f"GRANT USAGE ON STREAMLIT {STREAMLIT_NAME} TO APPLICATION ROLE app_role;"
        )

    def test_bundle(self):
        self.entity.action_bundle(self.action_ctx)
        output = (
            self.entity.root / "output" / "bundle" / "streamlit" / STREAMLIT_NAME
        )  # noqa

        assert output.exists()
        assert (output / "streamlit_app.py").exists()
        assert (output / "environment.yml").exists()
        assert (output / "pages" / "my_page.py").exists()

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.action_get_url"
    )
    def test_deploy(self, mock_get_url, mock_describe):
        mock_describe.return_value = False
        mock_get_url.return_value = "https://snowflake.com"
        self.entity.action_deploy(self.action_ctx, _open=False, replace=False)

        self.mock_execute.assert_called_with(
            f"CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')\nROOT_LOCATION = '@streamlit/test_streamlit'\nMAIN_FILE = 'streamlit_app.py'\nQUERY_WAREHOUSE = test_warehouse\nTITLE = 'My Fancy Streamlit';"
        )

    def test_drop(self):
        self.entity.action_drop(self.action_ctx)

        self.mock_execute.assert_called_with(
            f"DROP STREAMLIT IDENTIFIER('{STREAMLIT_NAME}');"
        )

    def test_share(self):
        self.entity.action_share(self.action_ctx, to_role="test_role")

        self.mock_execute.assert_called_with(
            "GRANT USAGE ON STREAMLIT IDENTIFIER('test_streamlit') TO ROLE test_role;"
        )

    def test_execute(self):
        self.entity.action_execute(self.action_ctx)

        self.mock_execute.assert_called_with(
            f"EXECUTE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')();"
        )

    def test_get_execute_sql(self):
        execute_sql = self.entity.get_execute_sql()

        assert execute_sql == f"EXECUTE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')();"

    def test_get_drop_sql(self):
        drop_sql = self.entity.get_drop_sql()

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
    def test_get_deploy_sql(self, kwargs, snapshot):
        deploy_sql = self.entity.get_deploy_sql(**kwargs)

        assert deploy_sql == snapshot
