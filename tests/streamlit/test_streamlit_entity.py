from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    SPCS_RUNTIME_V2_NAME,
    StreamlitEntityModel,
)
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.exceptions import CliError

from tests.streamlit.streamlit_test_class import STREAMLIT_NAME, StreamlitTestClass

CONNECTOR = "snowflake.connector.connect"


class TestStreamlitEntity(StreamlitTestClass):
    def test_nativeapp_children_interface(self, example_entity, snapshot):
        example_entity.bundle()
        bundle_artifact = (
            example_entity.root
            / "output"
            / "bundle"
            / "streamlit"
            / STREAMLIT_NAME
            / "streamlit_app.py"
        )
        deploy_sql_str = example_entity.get_deploy_sql()
        grant_sql_str = example_entity.get_usage_grant_sql(app_role="app_role")

        assert bundle_artifact.exists()
        assert deploy_sql_str == snapshot
        assert (
            grant_sql_str
            == f"GRANT USAGE ON STREAMLIT {STREAMLIT_NAME} TO APPLICATION ROLE app_role;"
        )

    def test_bundle(self, example_entity, action_context):
        example_entity.action_bundle(action_context)
        output = (
            example_entity.root / "output" / "bundle" / "streamlit" / STREAMLIT_NAME
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
    def test_deploy(self, mock_get_url, mock_describe, example_entity, action_context):
        mock_describe.return_value = False
        mock_get_url.return_value = "https://snowflake.com"

        # Test legacy deployment behavior
        example_entity.action_deploy(
            action_context, _open=False, replace=False, legacy=True
        )

        self.mock_execute.assert_called_with(
            f"CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')\nROOT_LOCATION = '@streamlit/test_streamlit'\nMAIN_FILE = 'streamlit_app.py'\nQUERY_WAREHOUSE = test_warehouse\nTITLE = 'My Fancy Streamlit';"
        )

    def test_drop(self, example_entity, action_context):
        example_entity.action_drop(action_context)
        self.mock_execute.assert_called_with(
            f"DROP STREAMLIT IDENTIFIER('{STREAMLIT_NAME}');"
        )

    def test_share(self, example_entity, action_context):
        example_entity.action_share(action_context, "test_role")
        self.mock_execute.assert_called_with(
            f"GRANT USAGE ON STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') TO ROLE test_role;"
        )

    def test_execute(self, example_entity, action_context):
        example_entity.action_execute(action_context)
        self.mock_execute.assert_called_with(
            f"EXECUTE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')();"
        )

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
    def test_get_deploy_sql(self, example_entity, snapshot, kwargs):
        sql = example_entity.get_deploy_sql()
        assert sql == snapshot

    def test_get_drop_sql(self, example_entity):
        sql = example_entity.get_drop_sql()
        assert sql == "DROP STREAMLIT IDENTIFIER('test_streamlit');"

    def test_get_execute_sql(self, example_entity):
        sql = example_entity.get_execute_sql()
        assert sql == "EXECUTE STREAMLIT IDENTIFIER('test_streamlit')();"

    def test_if_schema_and_database_are_passed_from_connection(
        self, example_entity, snapshot
    ):
        self.mock_conn.schema = "test_schema"
        self.mock_conn.database = "test_database"

        result = example_entity.get_deploy_sql()

        assert result == snapshot

    @pytest.mark.parametrize("attribute", ["schema", "database"])
    def test_if_attribute_is_not_set_correct_error_is_raised(
        self, example_entity, attribute
    ):
        with pytest.raises(ValueError) as e:
            result = getattr(example_entity, attribute)
        assert str(e.value) == f"Could not determine {attribute} for {STREAMLIT_NAME}"

    def test_spcs_runtime_v2_model_fields(self, workspace_context):
        """Test that StreamlitEntityModel accepts runtime_name and compute_pool fields"""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        assert model.runtime_name == SPCS_RUNTIME_V2_NAME
        assert model.compute_pool == "MYPOOL"

    def test_get_deploy_sql_with_spcs_runtime_v2(self, workspace_context):
        """Test that get_deploy_sql includes RUNTIME_NAME and COMPUTE_POOL when experimental is True"""

        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        # Test with FROM syntax (artifacts_dir provided) - versioned deployment
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert f"RUNTIME_NAME = '{SPCS_RUNTIME_V2_NAME}'" in sql
        assert "COMPUTE_POOL = 'MYPOOL'" in sql

    def test_get_deploy_sql_spcs_runtime_v2_with_stage(self, workspace_context):
        """Test that SPCS runtime v2 clauses are NOT added with stage-based deployment (old-style streamlits)"""

        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        # Test with stage-based deployment (ROOT_LOCATION) - should NOT include SPCS runtime fields
        # as stage-based deployments are old-style
        sql = entity.get_deploy_sql(from_stage_name="@stage/path", legacy=False)

        assert "ROOT_LOCATION = '@stage/path'" in sql
        assert "RUNTIME_NAME" not in sql
        assert "COMPUTE_POOL" not in sql

    def test_get_deploy_sql_without_spcs_runtime_v2(self, workspace_context):
        """Test that get_deploy_sql works normally when legacy is True"""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        # Test with legacy flag - should not add SPCS runtime v2 fields
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=True)

        assert "RUNTIME_NAME" not in sql
        assert "COMPUTE_POOL" not in sql

    def test_spcs_runtime_v2_requires_correct_runtime_name(self, workspace_context):
        """Test that SPCS runtime v2 requires correct runtime name to be enabled"""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        # Test with versioned deployment (default, legacy=False) and correct runtime_name
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)
        assert f"RUNTIME_NAME = '{SPCS_RUNTIME_V2_NAME}'" in sql
        assert "COMPUTE_POOL = 'MYPOOL'" in sql

        # Test with legacy=True, should not add SPCS fields
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=True)
        assert "RUNTIME_NAME" not in sql
        assert "COMPUTE_POOL" not in sql

        # Test with wrong runtime_name
        model.runtime_name = "SOME_OTHER_RUNTIME"
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)
        assert "RUNTIME_NAME" not in sql
        assert "COMPUTE_POOL" not in sql

    def test_spcs_runtime_v2_requires_runtime_and_pool(self, workspace_context):
        """Test that SPCS runtime v2 SQL generation works with valid models"""

        # Test with valid container runtime and compute_pool
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)
        assert f"RUNTIME_NAME = '{SPCS_RUNTIME_V2_NAME}'" in sql
        assert "COMPUTE_POOL = 'MYPOOL'" in sql

        # Test with warehouse runtime (no compute_pool needed)
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name="SYSTEM$WAREHOUSE_RUNTIME",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)
        # Warehouse runtime should not trigger SPCS runtime v2 mode
        assert "RUNTIME_NAME" not in sql
        assert "COMPUTE_POOL" not in sql

    def test_spcs_runtime_validation(self, workspace_context):
        """Test validation for SPCS runtime configuration"""

        # Test: SYSTEM$ST_CONTAINER_RUNTIME_PY3_11 requires compute_pool
        escaped_runtime_name = SPCS_RUNTIME_V2_NAME.replace("$", r"\$")
        with pytest.raises(
            ValueError,
            match=rf"compute_pool is required when using {escaped_runtime_name}",
        ):
            StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                runtime_name=SPCS_RUNTIME_V2_NAME,
                main_file="streamlit_app.py",
                artifacts=["streamlit_app.py"],
            )

        # Test: compute_pool without runtime_name is invalid
        with pytest.raises(
            ValueError, match="compute_pool is specified without runtime_name"
        ):
            StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                compute_pool="MYPOOL",
                main_file="streamlit_app.py",
                artifacts=["streamlit_app.py"],
            )

        # Test: warehouse runtime without compute_pool is valid
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name="SYSTEM$WAREHOUSE_RUNTIME",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        assert model.runtime_name == "SYSTEM$WAREHOUSE_RUNTIME"
        assert model.compute_pool is None

        # Test: container runtime with compute_pool is valid
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        assert model.runtime_name == SPCS_RUNTIME_V2_NAME
        assert model.compute_pool == "MYPOOL"

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_with_spcs_runtime_v2_and_legacy_flag_raises_error(
        self, mock_bundle, mock_object_exists, workspace_context, action_context
    ):
        """Test that deploying with SPCS runtime v2 and --legacy flag raises a clear error"""
        mock_object_exists.return_value = False
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )

        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        entity = StreamlitEntity(
            workspace_ctx=workspace_context,
            entity_model=model,
        )

        with pytest.raises(
            CliError,
            match="runtime_name and compute_pool are not compatible with --legacy flag",
        ):
            entity.action_deploy(
                action_context, _open=False, replace=False, legacy=True
            )

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._deploy_versioned"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_replace_legacy_with_versioned_shows_warning(
        self,
        mock_bundle,
        mock_deploy_versioned,
        mock_is_legacy,
        mock_object_exists,
        workspace_context,
        action_context,
    ):
        """Test that replacing a legacy deployment with versioned shows a warning"""
        mock_object_exists.return_value = True
        mock_is_legacy.return_value = True  # Existing is legacy
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )

        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        entity = StreamlitEntity(
            workspace_ctx=workspace_context,
            entity_model=model,
        )

        entity.action_deploy(action_context, _open=False, replace=True, legacy=False)

        # Verify warning was shown
        assert any(
            "Replacing legacy ROOT_LOCATION deployment with versioned deployment"
            in str(call)
            for call in workspace_context.console.warning.call_args_list
        )

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._deploy_legacy"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_replace_versioned_with_legacy_shows_warning(
        self,
        mock_bundle,
        mock_deploy_legacy,
        mock_is_legacy,
        mock_object_exists,
        workspace_context,
        action_context,
    ):
        """Test that replacing a versioned deployment with legacy shows a warning"""
        mock_object_exists.return_value = True
        mock_is_legacy.return_value = False  # Existing is versioned
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )

        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")

        entity = StreamlitEntity(
            workspace_ctx=workspace_context,
            entity_model=model,
        )

        entity.action_deploy(action_context, _open=False, replace=True, legacy=True)

        # Verify warning was shown
        assert any(
            "Deployment style is changing from versioned to legacy" in str(call)
            for call in workspace_context.console.warning.call_args_list
        )
