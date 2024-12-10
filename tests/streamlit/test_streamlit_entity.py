from __future__ import annotations

from pathlib import Path

import pytest
from snowflake.cli._plugins.streamlit.streamlit_entity import (
    StreamlitEntity,
)
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import WorkspaceContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.testing_utils.mock_config import mock_config_key


def test_cannot_instantiate_without_feature_flag():
    with pytest.raises(NotImplementedError) as err:
        StreamlitEntity()
    assert str(err.value) == "Streamlit entity is not implemented yet"


def test_nativeapp_children_interface(temp_dir):
    with mock_config_key("enable_native_app_children", True):
        dm = DefinitionManager()
        ctx = WorkspaceContext(
            console=cc,
            project_root=dm.project_root,
            get_default_role=lambda: "mock_role",
            get_default_warehouse=lambda: "mock_warehouse",
        )
        main_file = "main.py"
        (Path(temp_dir) / main_file).touch()
        model = StreamlitEntityModel(
            type="streamlit",
            main_file=main_file,
            artifacts=[main_file],
        )
        sl = StreamlitEntity(model, ctx)

        sl.bundle()
        bundle_artifact = Path(temp_dir) / "output" / "deploy" / main_file
        deploy_sql_str = sl.get_deploy_sql()
        grant_sql_str = sl.get_usage_grant_sql(app_role="app_role")

        assert bundle_artifact.exists()
        assert deploy_sql_str == "CREATE OR REPLACE STREAMLIT None MAIN_FILE='main.py';"
        assert (
            grant_sql_str
            == "GRANT USAGE ON STREAMLIT None TO APPLICATION ROLE app_role;"
        )
