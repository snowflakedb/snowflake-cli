from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    PREVIEW_DATABASE,
    PREVIEW_SCHEMA,
    PREVIEW_WORKSPACE_STAGE_URI,
    SPCS_RUNTIME_V2_NAME,
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import WorkspaceContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.console.abc import AbstractConsole
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

    def test_bundle_auto_includes_main_file(self, project_directory):
        """Test that main_file is automatically included even if not in artifacts."""

        with project_directory("example_streamlit_v2"):
            # Create workspace context inside the context manager so project_root
            # points to the temporary directory
            workspace_ctx = WorkspaceContext(
                console=mock.MagicMock(spec=AbstractConsole),
                project_root=Path().resolve(),
                get_default_role=lambda: "mock_role",
                get_default_warehouse=lambda: "mock_warehouse",
            )
            model = StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                main_file="streamlit_app.py",
                artifacts=["environment.yml"],  # main_file NOT included
            )
            model.set_entity_id("test_streamlit")
            entity = StreamlitEntity(workspace_ctx=workspace_ctx, entity_model=model)

            entity.bundle()
            output = entity.root / "output" / "bundle" / "streamlit" / "test_streamlit"

            assert (output / "streamlit_app.py").exists()  # auto-included
            assert (output / "environment.yml").exists()

    def test_bundle_deduplicates_pages_directory_and_glob(self, project_directory):
        with project_directory("example_streamlit_v2"):
            workspace_ctx = WorkspaceContext(
                console=mock.MagicMock(spec=AbstractConsole),
                project_root=Path().resolve(),
                get_default_role=lambda: "mock_role",
                get_default_warehouse=lambda: "mock_warehouse",
            )
            model = StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                main_file="streamlit_app.py",
                artifacts=["streamlit_app.py", "pages/", "pages/*.py"],
            )
            model.set_entity_id("test_streamlit")
            entity = StreamlitEntity(workspace_ctx=workspace_ctx, entity_model=model)

            entity.bundle()
            output = entity.root / "output" / "bundle" / "streamlit" / "test_streamlit"

            assert (output / "streamlit_app.py").exists()
            assert (output / "pages" / "my_page.py").exists()

    def test_bundle_deduplicates_pages_glob_and_directory(self, project_directory):
        with project_directory("example_streamlit_v2"):
            workspace_ctx = WorkspaceContext(
                console=mock.MagicMock(spec=AbstractConsole),
                project_root=Path().resolve(),
                get_default_role=lambda: "mock_role",
                get_default_warehouse=lambda: "mock_warehouse",
            )
            model = StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                main_file="streamlit_app.py",
                artifacts=["streamlit_app.py", "pages/*.py", "pages/"],
            )
            model.set_entity_id("test_streamlit")
            entity = StreamlitEntity(workspace_ctx=workspace_ctx, entity_model=model)

            entity.bundle()
            output = entity.root / "output" / "bundle" / "streamlit" / "test_streamlit"

            assert (output / "streamlit_app.py").exists()
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

    # ---- Personal-preview deploy (`--preview`) ----

    def _build_preview_entity(
        self,
        workspace_context,
        *,
        entity_id: str = "streamlittestapp",
        identifier_name: str = "testapp_vnext_private",
        main_file: str = "streamlit_app.py",
        query_warehouse: str = "REGRESS",
        runtime_name: str | None = SPCS_RUNTIME_V2_NAME,
        compute_pool: str | None = "MYPOOL",
        title: str | None = None,
        comment: str | None = None,
    ) -> StreamlitEntity:
        kwargs = {
            "type": "streamlit",
            "identifier": identifier_name,
            "main_file": main_file,
            "artifacts": [main_file],
            "query_warehouse": query_warehouse,
        }
        if runtime_name is not None:
            kwargs["runtime_name"] = runtime_name
        if compute_pool is not None:
            kwargs["compute_pool"] = compute_pool
        if title is not None:
            kwargs["title"] = title
        if comment is not None:
            kwargs["comment"] = comment
        model = StreamlitEntityModel(**kwargs)
        model.set_entity_id(entity_id)
        return StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

    def test_get_preview_deploy_sql_with_spcs_runtime(self, workspace_context):
        """SPCS-runtime preview deploy must match the user's example SQL exactly."""
        entity = self._build_preview_entity(workspace_context)

        sql = entity.get_preview_deploy_sql(replace=True)

        assert sql == (
            "CREATE OR REPLACE STREAMLIT user$.public.testapp_vnext_private\n"
            "FROM 'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live'\n"
            "MAIN_FILE = 'streamlittestapp/streamlit_app.py'\n"
            "QUERY_WAREHOUSE = REGRESS\n"
            "CREATE_CODE_STAGE = FALSE\n"
            "RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'\n"
            "COMPUTE_POOL = 'MYPOOL';"
        )

    def test_get_preview_deploy_sql_without_spcs_runtime(self, workspace_context):
        """Warehouse-runtime preview omits SPCS clauses but keeps CREATE_CODE_STAGE = FALSE."""
        entity = self._build_preview_entity(
            workspace_context,
            runtime_name=None,
            compute_pool=None,
        )

        sql = entity.get_preview_deploy_sql(replace=True)

        assert "RUNTIME_NAME" not in sql
        assert "COMPUTE_POOL" not in sql
        assert "CREATE_CODE_STAGE = FALSE" in sql
        assert sql.startswith(
            "CREATE OR REPLACE STREAMLIT user$.public.testapp_vnext_private"
        )

    def test_get_preview_deploy_sql_replace_false(self, workspace_context):
        entity = self._build_preview_entity(workspace_context)
        sql = entity.get_preview_deploy_sql(replace=False)
        assert sql.startswith("CREATE STREAMLIT user$.public.testapp_vnext_private")
        assert "CREATE OR REPLACE" not in sql

    def test_get_preview_deploy_sql_main_file_prefix(self, workspace_context):
        """MAIN_FILE is auto-prefixed with the entity_id regardless of yaml main_file."""
        entity = self._build_preview_entity(
            workspace_context,
            entity_id="my_subdir",
            main_file="nested/app.py",
        )
        sql = entity.get_preview_deploy_sql(replace=False)
        assert "MAIN_FILE = 'my_subdir/nested/app.py'" in sql

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._preview_object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._get_preview_url"
    )
    def test_deploy_with_preview_uploads_to_workspace_subfolder(
        self,
        mock_url,
        mock_bundle,
        mock_exists,
        mock_sync,
        workspace_context,
        action_context,
    ):
        """sync_deploy_root_with_stage receives the per-entity workspace subfolder."""
        mock_exists.return_value = False
        mock_url.return_value = "https://snowflake.com/preview"
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        entity = self._build_preview_entity(workspace_context)

        entity.action_deploy(action_context, _open=False, replace=True, preview=True)

        assert mock_sync.call_count == 1
        kwargs = mock_sync.call_args.kwargs
        # Stage-path-parts should be derived from
        # snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live/streamlittestapp
        assert "streamlittestapp" in kwargs["stage_path_parts"].full_path
        assert PREVIEW_WORKSPACE_STAGE_URI in kwargs["stage_path_parts"].full_path
        # Pruning is forced off because workspace HEAD is shared.
        assert kwargs["prune"] is False

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._preview_object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._get_preview_url"
    )
    def test_deploy_with_preview_does_not_call_add_live_version(
        self,
        mock_url,
        mock_bundle,
        mock_exists,
        mock_sync,
        workspace_context,
        action_context,
    ):
        mock_exists.return_value = False
        mock_url.return_value = "https://snowflake.com/preview"
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        entity = self._build_preview_entity(workspace_context)

        entity.action_deploy(action_context, _open=False, replace=True, preview=True)

        executed_queries = [c.args[0] for c in self.mock_execute.call_args_list]
        assert not any("ADD LIVE VERSION" in q for q in executed_queries), (
            f"ADD LIVE VERSION should never be issued in preview mode. "
            f"Queries: {executed_queries}"
        )
        # The preview CREATE STREAMLIT must be the executed query.
        assert any(
            q.startswith(
                "CREATE OR REPLACE STREAMLIT user$.public.testapp_vnext_private"
            )
            for q in executed_queries
        )

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._preview_object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_with_preview_object_exists_no_replace_errors(
        self,
        mock_bundle,
        mock_exists,
        mock_sync,
        workspace_context,
        action_context,
    ):
        from click import ClickException

        mock_exists.return_value = True
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        entity = self._build_preview_entity(workspace_context)

        with pytest.raises(ClickException, match="already exists"):
            entity.action_deploy(
                action_context, _open=False, replace=False, preview=True
            )
        # Upload must not have happened.
        mock_sync.assert_not_called()

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_with_preview_and_legacy_raises_error(
        self, mock_bundle, workspace_context, action_context
    ):
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        entity = self._build_preview_entity(workspace_context)

        with pytest.raises(CliError, match="--preview is not compatible with --legacy"):
            entity.action_deploy(
                action_context,
                _open=False,
                replace=True,
                preview=True,
                legacy=True,
            )

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_with_preview_and_prune_raises_error(
        self, mock_bundle, workspace_context, action_context
    ):
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        entity = self._build_preview_entity(workspace_context)

        with pytest.raises(CliError, match="--prune is not supported with --preview"):
            entity.action_deploy(
                action_context,
                _open=False,
                replace=True,
                preview=True,
                prune=True,
            )

    def test_preview_constants_match_user_example(self):
        assert PREVIEW_DATABASE == "user$"
        assert PREVIEW_SCHEMA == "public"
        assert (
            PREVIEW_WORKSPACE_STAGE_URI
            == "snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live"
        )

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._preview_object_exists",
        return_value=False,
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._get_preview_url",
        return_value="https://snowflake.com/preview",
    )
    def test_deploy_with_preview_skips_grants_when_yaml_has_grants(
        self,
        _mock_url,
        mock_bundle,
        _mock_exists,
        _mock_sync,
        mock_streamlit_manager,
        workspace_context,
        action_context,
    ):
        """`grants:` from snowflake.yml target the YAML FQN, not the preview FQN.

        Applying them in preview mode would either fail or silently grant on
        the wrong object, so `_deploy_preview` must skip grants and warn.
        """
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        from snowflake.cli.api.project.schemas.entities.common import Grant

        entity = self._build_preview_entity(workspace_context)
        entity.model.grants = [Grant(privilege="USAGE", role="SOME_ROLE")]

        entity.action_deploy(action_context, _open=False, replace=True, preview=True)

        mock_streamlit_manager.return_value.grant_privileges.assert_not_called()
        assert any(
            "Skipping `grants:` in --preview mode" in str(call)
            for call in workspace_context.console.warning.call_args_list
        )
