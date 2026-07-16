from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity, _TagRef
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    SPCS_RUNTIME_V2_NAME,
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import WorkspaceContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.project.schemas.entities.common import PathMapping

from tests.conftest import MockCursor
from tests.streamlit.streamlit_test_class import STREAMLIT_NAME, StreamlitTestClass

CONNECTOR = "snowflake.connector.connect"


class TestStreamlitEntity(StreamlitTestClass):
    @staticmethod
    def _create_entity(project_root: Path, main_file: str, artifacts):
        workspace_ctx = WorkspaceContext(
            console=mock.MagicMock(spec=AbstractConsole),
            project_root=project_root,
            get_default_role=lambda: "mock_role",
            get_default_warehouse=lambda: "mock_warehouse",
        )
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file=main_file,
            artifacts=artifacts,
        )
        model.set_entity_id("test_streamlit")
        return StreamlitEntity(workspace_ctx=workspace_ctx, entity_model=model)

    @staticmethod
    def _write_file(path: Path, content: str = ""):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    @staticmethod
    def _bundle_output(project_root: Path) -> Path:
        return project_root / "output" / "bundle" / "streamlit" / "test_streamlit"

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
            entity = self._create_entity(
                project_root=Path().resolve(),
                main_file="streamlit_app.py",
                artifacts=["environment.yml"],  # main_file NOT included
            )

            entity.bundle()
            output = entity.root / "output" / "bundle" / "streamlit" / "test_streamlit"

            assert (output / "streamlit_app.py").exists()  # auto-included
            assert (output / "environment.yml").exists()

    def test_bundle_auto_inserts_main_file_when_no_artifacts(self, tmp_path):
        self._write_file(tmp_path / "app.py")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="app.py",
            artifacts=[],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [Path("app.py")]
        assert (output / "app.py").exists()

    def test_bundle_skips_auto_insert_when_main_file_is_explicit_artifact(
        self, tmp_path
    ):
        self._write_file(tmp_path / "app.py")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="app.py",
            artifacts=["app.py"],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [Path("app.py")]
        assert list(bundle_map.to_deploy_paths(Path("app.py"))) == [Path("app.py")]
        assert (output / "app.py").exists()

    @pytest.mark.parametrize(
        "main_file, artifact_dir, expected_source, sibling_file",
        [
            (
                "apps/my_app/app.py",
                "apps/my_app/",
                Path("apps/my_app"),
                "apps/my_app/environment.yml",
            ),
            (
                "src/apps/my_app/app.py",
                "src/",
                Path("src"),
                "src/shared.py",
            ),
        ],
    )
    def test_bundle_skips_auto_insert_when_main_file_inside_directory_artifact(
        self, tmp_path, main_file, artifact_dir, expected_source, sibling_file
    ):
        self._write_file(tmp_path / main_file)
        self._write_file(tmp_path / sibling_file)
        entity = self._create_entity(
            project_root=tmp_path,
            main_file=main_file,
            artifacts=[artifact_dir],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [expected_source]
        assert list(bundle_map.to_deploy_paths(Path(main_file))) == [Path(main_file)]
        assert (output / main_file).exists()
        assert (output / sibling_file).exists()

    def test_bundle_auto_inserts_when_main_file_outside_artifact_directory(
        self, tmp_path
    ):
        self._write_file(tmp_path / "app.py")
        self._write_file(tmp_path / "other_dir" / "helper.py")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="app.py",
            artifacts=["other_dir/"],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [Path("app.py"), Path("other_dir")]
        assert (output / "app.py").exists()
        assert (output / "other_dir" / "helper.py").exists()

    def test_bundle_auto_inserts_when_artifact_has_different_dest(self, tmp_path):
        self._write_file(tmp_path / "apps" / "my_app" / "app.py")
        self._write_file(tmp_path / "apps" / "my_app" / "helper.py")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="apps/my_app/app.py",
            artifacts=[PathMapping(src="apps/my_app/", dest="elsewhere/")],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [
            Path("apps/my_app/app.py"),
            Path("apps/my_app"),
        ]
        assert list(bundle_map.to_deploy_paths(Path("apps/my_app/app.py"))) == [
            Path("apps/my_app/app.py"),
            Path("elsewhere/my_app/app.py"),
        ]
        assert (output / "apps" / "my_app" / "app.py").exists()
        assert (output / "elsewhere" / "my_app" / "app.py").exists()
        assert (output / "elsewhere" / "my_app" / "helper.py").exists()

    def test_bundle_skips_auto_insert_when_artifact_dest_equals_src(self, tmp_path):
        """Self-referential dest (no trailing slash, equal to src) deploys to the
        same canonical path as auto-insert, so the auto-insert is skipped."""
        self._write_file(tmp_path / "apps" / "my_app" / "app.py")
        self._write_file(tmp_path / "apps" / "my_app" / "env.yml")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="apps/my_app/app.py",
            artifacts=[PathMapping(src="apps/my_app/", dest="apps/my_app")],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [Path("apps/my_app")]
        assert (output / "apps" / "my_app" / "app.py").exists()
        assert (output / "apps" / "my_app" / "env.yml").exists()

    def test_bundle_auto_inserts_when_artifact_dest_is_deploy_root(self, tmp_path):
        """``dest='./'`` lands the directory's children at the deploy root, which
        is a different canonical path than auto-insert produces, so the
        auto-insert still fires (no collision)."""
        self._write_file(tmp_path / "apps" / "my_app" / "app.py")
        self._write_file(tmp_path / "apps" / "my_app" / "env.yml")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="apps/my_app/app.py",
            artifacts=[PathMapping(src="apps/my_app/", dest="./")],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [
            Path("apps/my_app/app.py"),
            Path("apps/my_app"),
        ]
        # auto-inserted main_file lands at apps/my_app/app.py
        assert (output / "apps" / "my_app" / "app.py").exists()
        # dir-walk lands children at ./my_app/...
        assert (output / "my_app" / "app.py").exists()
        assert (output / "my_app" / "env.yml").exists()

    def test_bundle_skips_auto_insert_when_dest_root_artifact_covers_root_main_file(
        self, tmp_path
    ):
        """An artifact with ``src=app.py, dest='./'`` deploys ``app.py`` to the
        deploy root, the same canonical path the auto-insert would produce,
        so the auto-insert is skipped."""
        self._write_file(tmp_path / "app.py")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="app.py",
            artifacts=[PathMapping(src="app.py", dest="./")],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert list(bundle_map.all_sources()) == [Path("app.py")]
        assert list(bundle_map.to_deploy_paths(Path("app.py"))) == [Path("app.py")]
        assert (output / "app.py").exists()

    def test_bundle_handles_glob_overlap_with_main_file(self, tmp_path):
        """Glob-style src (``pages/*.py``) fails the helper's ``relative_to``
        check, so the auto-insert fires. Downstream file-level dedup in
        ``_ArtifactPathMap.put()`` handles the overlap without raising."""
        self._write_file(tmp_path / "pages" / "main.py")
        self._write_file(tmp_path / "pages" / "page.py")
        entity = self._create_entity(
            project_root=tmp_path,
            main_file="pages/main.py",
            artifacts=["pages/*.py"],
        )

        bundle_map = entity.bundle()
        output = self._bundle_output(tmp_path)

        assert sorted(p.as_posix() for p in bundle_map.all_sources()) == [
            "pages/main.py",
            "pages/page.py",
        ]
        assert (output / "pages" / "main.py").exists()
        assert (output / "pages" / "page.py").exists()

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

    @pytest.mark.parametrize(
        "field,payload",
        [
            ("title", "evil'; DROP TABLE users; --"),
            ("comment", "it's a trap'); DROP TABLE"),
            ("main_file", "app.py'; DROP TABLE"),
            ("compute_pool", "POOL'; DROP TABLE"),
        ],
    )
    def test_get_deploy_sql_escapes_string_literals(
        self, workspace_context, field, payload
    ):
        """Regression for SNOW-3417292: values from snowflake.yml must be
        escaped before being interpolated into CREATE STREAMLIT SQL so a
        single quote in any of them cannot break out of the SQL literal."""
        kwargs = dict(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        if field == "compute_pool":
            # compute_pool requires a matching runtime_name; pin runtime_name
            # to the SPCS constant so only compute_pool carries the payload.
            kwargs["runtime_name"] = SPCS_RUNTIME_V2_NAME
            kwargs["compute_pool"] = payload
        else:
            kwargs[field] = payload

        model = StreamlitEntityModel(**kwargs)
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        # The raw unescaped payload must not appear as a simple quoted value
        # in the SQL. to_string_literal uses standard SQL quote-doubling ('')
        # so if escaping works, the raw `= '<payload>'` pattern cannot match.
        assert f"= '{payload}'" not in sql
        # Confirm quote-doubling is present (each ' in payload becomes '')
        assert "''" in sql

    def test_get_deploy_sql_with_tags(self, workspace_context):
        """Test that get_deploy_sql includes WITH TAG (...) when tags are set."""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[Tag("cost_center", "engineering"), Tag("owner", "team_a")],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert "WITH TAG (cost_center='engineering',owner='team_a')" in sql

    def test_get_deploy_sql_tags_escape_single_quotes(self, workspace_context):
        """Tag values containing single quotes must be properly escaped."""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[Tag("env", "it's prod")],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert "WITH TAG (env='it''s prod')" in sql
        assert "WITH TAG (env='it's prod')" not in sql

    def test_get_set_tag_sql_with_tags(self, workspace_context):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[Tag("cost_center", "engineering"), Tag("owner", "team_a")],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        sql = entity.get_set_tag_sql()

        assert (
            sql
            == "ALTER STREAMLIT IDENTIFIER('test_streamlit') SET TAG cost_center='engineering',owner='team_a';"
        )

    def test_get_set_tag_sql_no_tags(self, workspace_context):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        assert entity.get_set_tag_sql() is None

    def test_get_set_tag_sql_escapes_single_quotes(self, workspace_context):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[Tag("env", "it's prod")],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        sql = entity.get_set_tag_sql()

        assert "SET TAG env='it''s prod'" in sql
        assert "SET TAG env='it's prod'" not in sql

    def test_get_unset_tag_sql(self, workspace_context):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        sql = entity.get_unset_tag_sql(["MYDB.PUBLIC.COST_CENTER", "MYDB.PUBLIC.OWNER"])

        assert (
            sql
            == "ALTER STREAMLIT IDENTIFIER('test_streamlit') UNSET TAG MYDB.PUBLIC.COST_CENTER,MYDB.PUBLIC.OWNER;"
        )

    def test_sync_tags_unsets_removed_and_sets_desired(self, workspace_context):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[Tag("new_tag", "v")],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        with (
            mock.patch.object(
                entity,
                "_get_current_tags",
                return_value=[_TagRef("OLD_TAG", "MYDB.MYSCHEMA.OLD_TAG")],
            ),
            mock.patch.object(entity, "_execute_query") as mock_exec,
        ):
            entity._sync_tags()  # noqa: SLF001

        calls = [str(c.args[0]) for c in mock_exec.call_args_list]
        assert any("UNSET TAG MYDB.MYSCHEMA.OLD_TAG" in c for c in calls)
        assert any("SET TAG new_tag='v'" in c for c in calls)

    def test_sync_tags_unsets_all_when_no_desired_tags(self, workspace_context):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        with (
            mock.patch.object(
                entity,
                "_get_current_tags",
                return_value=[
                    _TagRef("TAG_A", "MYDB.MYSCHEMA.TAG_A"),
                    _TagRef("TAG_B", "MYDB.MYSCHEMA.TAG_B"),
                ],
            ),
            mock.patch.object(entity, "_execute_query") as mock_exec,
        ):
            entity._sync_tags()  # noqa: SLF001

        assert mock_exec.call_count == 1
        unset_sql = mock_exec.call_args.args[0]
        assert "UNSET TAG" in unset_sql
        assert " SET TAG " not in unset_sql

    def test_sync_tags_skips_when_tags_property_absent(self, workspace_context):
        """When tags is not set in snowflake.yml (None), _sync_tags must be a no-op.
        Neither _get_current_tags nor any query should be issued."""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        with (
            mock.patch.object(entity, "_get_current_tags") as mock_get,
            mock.patch.object(entity, "_execute_query") as mock_exec,
        ):
            entity._sync_tags()  # noqa: SLF001

        mock_get.assert_not_called()
        mock_exec.assert_not_called()

    def test_sync_tags_unsets_all_when_tags_explicitly_empty(self, workspace_context):
        """tags: [] explicitly means 'manage tags and remove all' — unset everything currently set."""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        with (
            mock.patch.object(entity, "_get_current_tags", return_value=[]),
            mock.patch.object(entity, "_execute_query") as mock_exec,
        ):
            entity._sync_tags()  # noqa: SLF001

        mock_exec.assert_not_called()

    def test_get_current_tags_qualifies_information_schema_with_database(
        self, workspace_context
    ):
        """information_schema must be qualified with the resolved database so the query
        works when no default database is active in the session."""
        self.mock_conn.database = "MYDB"
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        self.mock_execute.return_value = MockCursor.from_input(
            [("MYDB", "MYSCHEMA", "MY_TAG")], ["TAG_DATABASE", "TAG_SCHEMA", "TAG_NAME"]
        )

        result = entity._get_current_tags()  # noqa: SLF001

        issued_sql = self.mock_execute.call_args.args[0]
        assert "MYDB.information_schema.tag_references" in issued_sql
        assert "WHERE LEVEL = 'STREAMLIT'" in issued_sql
        assert result == [_TagRef("MY_TAG", "MYDB.MYSCHEMA.MY_TAG")]

    def test_get_current_tags_falls_back_to_unqualified_when_no_database(
        self, workspace_context
    ):
        """When neither the model nor the connection has a database, information_schema
        is left unqualified (matching the pre-existing behaviour)."""
        self.mock_conn.database = None
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        self.mock_execute.return_value = MockCursor.from_input([], [])

        entity._get_current_tags()  # noqa: SLF001

        issued_sql = self.mock_execute.call_args.args[0]
        assert issued_sql.startswith(
            "SELECT TAG_DATABASE, TAG_SCHEMA, TAG_NAME FROM TABLE(information_schema"
        )

    def test_sync_tags_uses_fqn_for_unset(self, workspace_context):
        """UNSET TAG must use FQN so it resolves correctly outside the tag's schema."""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            tags=[],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        with (
            mock.patch.object(
                entity,
                "_get_current_tags",
                return_value=[_TagRef("GOV_TAG", "GOV_DB.GOV_SCHEMA.GOV_TAG")],
            ),
            mock.patch.object(entity, "_execute_query") as mock_exec,
        ):
            entity._sync_tags()  # noqa: SLF001

        unset_sql = mock_exec.call_args.args[0]
        assert "GOV_DB.GOV_SCHEMA.GOV_TAG" in unset_sql
        assert "UNSET TAG GOV_DB.GOV_SCHEMA.GOV_TAG" in unset_sql
