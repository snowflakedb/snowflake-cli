from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV20,
)

from tests.testing_utils.mock_config import mock_config_key


def _get_app_pkg_entity(project_directory):
    with project_directory("napp_children") as project_root:
        with Path(project_root / "snowflake.yml").open() as definition_file_path:
            project_definition = DefinitionV20(**yaml.safe_load(definition_file_path))
            wm = WorkspaceManager(
                project_definition=project_definition,
                project_root=project_root,
            )
            pkg_entity = wm.get_entity("pkg")
            streamlit_entity = wm.get_entity("my_streamlit")
            action_ctx = ActionContext(
                get_entity=lambda entity_id: streamlit_entity,
            )
            return (
                pkg_entity,
                action_ctx,
            )


def test_children_feature_flag_is_disabled():
    assert FeatureFlag.ENABLE_NATIVE_APP_CHILDREN.is_enabled() == False
    with pytest.raises(AttributeError) as err:
        ApplicationPackageEntityModel(
            **{"type": "application package", "children": [{"target": "some_child"}]}
        )
    assert str(err.value) == "Application package children are not supported yet"


def test_invalid_children_type():
    with mock_config_key("enable_native_app_children", True):
        definition_input = {
            "definition_version": "2",
            "entities": {
                "pkg": {
                    "type": "application package",
                    "artifacts": [],
                    "children": [
                        {
                            # packages cannot contain other packages as children
                            "target": "pkg2"
                        }
                    ],
                },
                "pkg2": {
                    "type": "application package",
                    "artifacts": [],
                },
            },
        }
        with pytest.raises(SchemaValidationError) as err:
            DefinitionV20(**definition_input)
        assert "Target type mismatch" in str(err.value)


def test_invalid_children_target():
    with mock_config_key("enable_native_app_children", True):
        definition_input = {
            "definition_version": "2",
            "entities": {
                "pkg": {
                    "type": "application package",
                    "artifacts": [],
                    "children": [
                        {
                            # no such entity
                            "target": "sl"
                        }
                    ],
                },
            },
        }
        with pytest.raises(SchemaValidationError) as err:
            DefinitionV20(**definition_input)
        assert "No such target: sl" in str(err.value)


def test_valid_children():
    with mock_config_key("enable_native_app_children", True):
        definition_input = {
            "definition_version": "2",
            "entities": {
                "pkg": {
                    "type": "application package",
                    "artifacts": [],
                    "children": [{"target": "sl"}],
                },
                "sl": {"type": "streamlit", "identifier": "my_streamlit"},
            },
        }
        project_definition = DefinitionV20(**definition_input)
        wm = WorkspaceManager(
            project_definition=project_definition,
            project_root="",
        )
        child_entity_id = project_definition.entities["pkg"].children[0]
        child_entity = wm.get_entity(child_entity_id.target)
        assert child_entity.__class__ == StreamlitEntity


@mock.patch(
    "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._get_identifier",
    return_value="IDENTIFIER('v_schema.my_streamlit')",
)
def test_children_bundle_with_custom_dir(mock_id, project_directory):
    with mock_config_key("enable_native_app_children", True):
        app_pkg, action_ctx = _get_app_pkg_entity(project_directory)
        bundle_result = app_pkg.action_bundle(action_ctx)
        deploy_root = bundle_result.deploy_root()

        # Application package artifacts
        assert (deploy_root / "README.md").exists()
        assert (deploy_root / "manifest.yml").exists()
        assert (deploy_root / "setup_script.sql").exists()

        # Child artifacts
        assert (
            deploy_root / "_entities" / "my_streamlit" / "streamlit_app.py"
        ).exists()

        # Generated setup script section
        with open(deploy_root / "setup_script.sql", "r") as f:
            setup_script_content = f.read()
            custom_dir_path = Path("_entities", "my_streamlit")
            assert setup_script_content.endswith(
                dedent(
                    f"""
                    -- AUTO GENERATED CHILDREN SECTION
                    CREATE OR REPLACE STREAMLIT IDENTIFIER('v_schema.my_streamlit')
                    FROM '{custom_dir_path}'
                    MAIN_FILE = 'streamlit_app.py'
                    QUERY_WAREHOUSE = 'streamlit';
                    CREATE APPLICATION ROLE IF NOT EXISTS my_app_role;
                    GRANT USAGE ON SCHEMA v_schema TO APPLICATION ROLE my_app_role;
                    GRANT USAGE ON STREAMLIT v_schema.my_streamlit TO APPLICATION ROLE my_app_role;
"""
                )
            )
