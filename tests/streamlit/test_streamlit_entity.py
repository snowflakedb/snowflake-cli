import re
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli._plugins.streamlit.streamlit_entity import (
    StreamlitEntity,
    _describe_row_is_spcs_v2,
    _is_live_version_already_exists_error,
    _TagRef,
)
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    SPCS_RUNTIME_V2_NAME,
    WAREHOUSE_RUNTIME_NAME,
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import WorkspaceContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.errno import INSUFFICIENT_PRIVILEGES, SQL_COMPILATION_ERROR
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.project.schemas.updatable_model import context
from snowflake.connector.errors import ProgrammingError

from tests.conftest import MockCursor
from tests.streamlit.streamlit_test_class import (
    RESTART_PARAMS,
    RESTART_SQL,
    STREAMLIT_NAME,
    StreamlitTestClass,
    restart_calls,
    stage_diff_unchanged,
    stage_diff_with_changes,
)

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

    @staticmethod
    def _runtime_entity(
        workspace_context, runtime_name=WAREHOUSE_RUNTIME_NAME, compute_pool=None
    ):
        """Build an entity on a given runtime, defaulting to the warehouse runtime."""
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=runtime_name,
            compute_pool=compute_pool,
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        return StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

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

    def test_compute_pool_alone_emits_both_runtime_fields(self, workspace_context):
        """A compute_pool with no runtime_name reaches the DDL, with the runtime named.

        The pool is only meaningful to the container runtime, so naming it explicitly
        avoids emitting COMPUTE_POOL against whatever runtime the account defaults to.
        """
        entity = self._runtime_entity(
            workspace_context, runtime_name=None, compute_pool="MYPOOL"
        )

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert f"RUNTIME_NAME = '{SPCS_RUNTIME_V2_NAME}'" in sql
        assert "COMPUTE_POOL = 'MYPOOL'" in sql

    def test_container_runtime_without_pool_emits_runtime_name_alone(
        self, workspace_context
    ):
        """The container runtime needs no compute_pool; Snowflake supplies a default.

        Requiring one used to reject this config outright, so RUNTIME_NAME never
        reached the DDL for an app that only named its runtime.
        """
        entity = self._runtime_entity(
            workspace_context, runtime_name=SPCS_RUNTIME_V2_NAME
        )

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert f"RUNTIME_NAME = '{SPCS_RUNTIME_V2_NAME}'" in sql
        assert "COMPUTE_POOL" not in sql

    def test_compute_pool_omitted_and_warned_for_warehouse_runtime(
        self, workspace_context
    ):
        """Snowflake ignores COMPUTE_POOL on the warehouse runtime, so it is not sent.

        The config is accepted rather than rejected, so that migrating an app from the
        container runtime only needs runtime_name changed, but the pool doing nothing
        is worth saying out loud.
        """
        entity = self._runtime_entity(
            workspace_context,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            compute_pool="MYPOOL",
        )

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert f"RUNTIME_NAME = '{WAREHOUSE_RUNTIME_NAME}'" in sql
        assert "COMPUTE_POOL" not in sql

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._deploy_versioned"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_warns_that_compute_pool_is_ignored_for_warehouse_runtime(
        self,
        mock_bundle,
        mock_deploy_versioned,
        mock_object_exists,
        workspace_context,
        action_context,
    ):
        """The ignored pool is surfaced at deploy time, where there is a console."""
        mock_object_exists.return_value = False
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        entity = self._runtime_entity(
            workspace_context,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            compute_pool="MYPOOL",
        )

        entity.action_deploy(action_context, _open=False, replace=False, legacy=False)

        warnings = [
            str(call) for call in workspace_context.console.warning.call_args_list
        ]
        assert any("MYPOOL" in w and "is ignored because" in w for w in warnings)

    def test_compute_pool_whitespace_is_trimmed(self, workspace_context):
        """Stray whitespace around a compute_pool never reaches the DDL.

        A pool name is an identifier, so padding is never meaningful. Untrimmed it
        would be emitted verbatim, the same way a padded runtime_name used to be.
        """
        entity = self._runtime_entity(
            workspace_context,
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="  MYPOOL  ",
        )

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert "COMPUTE_POOL = 'MYPOOL'" in sql

    def test_compute_pool_not_emitted_for_legacy_deployment(self, workspace_context):
        """--legacy deploys carry neither runtime field, compute_pool included."""
        entity = self._runtime_entity(
            workspace_context, runtime_name=None, compute_pool="MYPOOL"
        )

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=True)

        assert "COMPUTE_POOL" not in sql
        assert "RUNTIME_NAME" not in sql

    @pytest.mark.parametrize("field", ["runtime_name", "compute_pool"])
    def test_templated_runtime_fields_survive_the_pre_render_pass(self, field):
        """A templated value is not a runtime name yet, so validation must not judge it.

        The allowlist and the blank check live in field validators specifically so they
        inherit UpdatableModel's template-skip wrap. A model validator runs outside any
        field's validator chain and would reject `<% ... %>` before rendering, which
        broke `snow streamlit deploy` for templated project files.
        """
        template = f"<% ctx.env.{field.upper()} %>"

        with context({"skip_validation_on_templates": True}):
            model = StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                main_file="streamlit_app.py",
                artifacts=["streamlit_app.py"],
                **{field: template},
            )

        assert getattr(model, field) == template

    def test_runtime_name_with_quote_is_rejected_before_sql_generation(self):
        """A quote-bearing runtime_name never reaches the SQL escaping path.

        The allowlist is the control that keeps RUNTIME_NAME out of the SNOW-3417292
        escaping test above, so pin that it rejects rather than escapes.
        """
        with pytest.raises(ValueError, match="Unknown runtime_name"):
            StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                runtime_name=f"{WAREHOUSE_RUNTIME_NAME}'; DROP STREAMLIT x; --",
                main_file="streamlit_app.py",
                artifacts=["streamlit_app.py"],
            )

    def test_unknown_runtime_name_is_rejected_on_assignment(self):
        """An unrecognized runtime_name is rejected when assigned, not just at construction.

        UpdatableModel sets validate_assignment=True, so the validator re-runs on
        assignment. The emission behaviour for a valid runtime is covered by
        test_get_deploy_sql_with_spcs_runtime_v2 and its legacy counterpart.
        """
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )

        with pytest.raises(
            ValueError, match="Unknown runtime_name 'SOME_OTHER_RUNTIME'"
        ):
            model.runtime_name = "SOME_OTHER_RUNTIME"

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
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)
        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)
        # Warehouse runtime is requested explicitly, so it must reach the DDL. It
        # takes no compute pool, so COMPUTE_POOL stays out.
        assert f"RUNTIME_NAME = '{WAREHOUSE_RUNTIME_NAME}'" in sql
        assert "COMPUTE_POOL" not in sql

    @pytest.mark.parametrize(
        "runtime_name, compute_pool, expected_error",
        [
            pytest.param(
                SPCS_RUNTIME_V2_NAME, None, None, id="container-runtime-without-pool"
            ),
            pytest.param(None, "MYPOOL", None, id="pool-without-runtime-is-inferred"),
            pytest.param(
                WAREHOUSE_RUNTIME_NAME, "MYPOOL", None, id="warehouse-runtime-with-pool"
            ),
            pytest.param(
                "SYSTEM$SOMETHING_NEW",
                None,
                "Unknown runtime_name 'SYSTEM$SOMETHING_NEW'",
                id="unknown-runtime",
            ),
            pytest.param(
                "   ",
                None,
                "Unknown runtime_name '   '",
                id="whitespace-only-runtime",
            ),
            pytest.param(
                "   ",
                "MYPOOL",
                "Unknown runtime_name '   '",
                id="whitespace-only-runtime-with-pool",
            ),
            pytest.param(
                "",
                None,
                "Unknown runtime_name ''",
                id="empty-runtime",
            ),
            pytest.param(
                None, "   ", "compute_pool must not be empty", id="blank-pool"
            ),
            pytest.param(None, "", "compute_pool must not be empty", id="empty-pool"),
            pytest.param(
                SPCS_RUNTIME_V2_NAME,
                "   ",
                "compute_pool must not be empty",
                id="blank-pool-with-container-runtime",
            ),
            pytest.param(WAREHOUSE_RUNTIME_NAME, None, None, id="warehouse-valid"),
            pytest.param(SPCS_RUNTIME_V2_NAME, "MYPOOL", None, id="container-valid"),
            pytest.param(None, None, None, id="runtime-absent"),
        ],
    )
    def test_runtime_and_compute_pool_pairing(
        self, runtime_name, compute_pool, expected_error
    ):
        """The validator is a table over (runtime_name x compute_pool); test it as one.

        The whitespace-only rows matter: such a value is raw-truthy, so before it was
        rejected it slipped past every rule here and reached the DDL.
        """

        def build():
            return StreamlitEntityModel(
                type="streamlit",
                identifier="test_streamlit",
                runtime_name=runtime_name,
                compute_pool=compute_pool,
                main_file="streamlit_app.py",
                artifacts=["streamlit_app.py"],
            )

        if expected_error:
            with pytest.raises(ValueError, match=re.escape(expected_error)):
                build()
        else:
            model = build()
            # A compute pool with no runtime_name infers the container runtime.
            expected_runtime = runtime_name or (
                SPCS_RUNTIME_V2_NAME if compute_pool else None
            )
            assert model.runtime_name == expected_runtime
            assert model.compute_pool == compute_pool

    @pytest.mark.parametrize(
        "supplied",
        [
            WAREHOUSE_RUNTIME_NAME.lower(),
            f"  {WAREHOUSE_RUNTIME_NAME}  ",
            f"{WAREHOUSE_RUNTIME_NAME}\n",
            # U+017F LATIN SMALL LETTER LONG S upper-cases to plain "S"
            WAREHOUSE_RUNTIME_NAME.lower().replace("s", "\u017f", 1),
            # A non-breaking space is the realistic version of this: it survives a
            # copy-paste out of rendered docs and str.strip() treats it as whitespace.
            f"{WAREHOUSE_RUNTIME_NAME}\xa0",
        ],
        ids=[
            "lowercase",
            "padded",
            "trailing-newline",
            "homoglyph",
            "trailing-nbsp",
        ],
    )
    def test_recognized_runtime_name_is_canonicalized(
        self, workspace_context, supplied
    ):
        """A recognized runtime_name reaches the DDL as its canonical constant.

        Matching tolerates casing and stray whitespace (a yaml block scalar keeps a
        trailing newline), but the DDL must carry one exact spelling. Emitting the
        value verbatim is what previously let padded and homoglyph spellings through.
        """
        entity = self._runtime_entity(workspace_context, supplied)

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=False)

        assert f"RUNTIME_NAME = '{WAREHOUSE_RUNTIME_NAME}'" in sql
        assert "COMPUTE_POOL" not in sql

    def test_warehouse_runtime_not_emitted_for_legacy_deployment(
        self, workspace_context
    ):
        """--legacy deploys emit no RUNTIME_NAME, warehouse runtime included."""
        entity = self._runtime_entity(workspace_context)

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/artifacts"), legacy=True)

        assert "RUNTIME_NAME" not in sql

    def test_warehouse_runtime_not_emitted_for_root_location_deployment(
        self, workspace_context
    ):
        """ROOT_LOCATION deploys emit no RUNTIME_NAME, warehouse runtime included."""
        entity = self._runtime_entity(workspace_context)

        sql = entity.get_deploy_sql(from_stage_name="@stage/path", legacy=False)

        assert "ROOT_LOCATION = '@stage/path'" in sql
        assert "RUNTIME_NAME" not in sql

    def test_get_alter_sql_sets_warehouse_runtime_name(self, workspace_context):
        """Redeploying onto warehouse runtime alters an app that is on another runtime."""
        entity = self._runtime_entity(workspace_context)

        alter_sql = entity.get_alter_sql(
            current={
                "runtime_name": SPCS_RUNTIME_V2_NAME,
                "compute_pool": "MYPOOL",
                "query_warehouse": "test_warehouse",
            }
        )

        assert alter_sql is not None
        assert f"RUNTIME_NAME = '{WAREHOUSE_RUNTIME_NAME}'" in alter_sql
        assert "COMPUTE_POOL" not in alter_sql

    def test_get_alter_sql_omits_unchanged_warehouse_runtime_name(
        self, workspace_context
    ):
        """An app already on warehouse runtime gets no redundant RUNTIME_NAME clause."""
        entity = self._runtime_entity(workspace_context)

        alter_sql = entity.get_alter_sql(
            current={
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "query_warehouse": "test_warehouse",
            }
        )

        assert alter_sql is not None
        assert "RUNTIME_NAME" not in alter_sql

    def test_get_alter_sql_warns_when_moving_a_live_app_between_runtimes(
        self, workspace_context
    ):
        """Moving a running app's runtime is announced when it happens.

        A release note is not in front of the user at deploy time, so the warning names
        both the runtime the app is on and the one it is moving to.
        """
        entity = self._runtime_entity(workspace_context)

        entity.get_alter_sql(
            current={
                "runtime_name": SPCS_RUNTIME_V2_NAME,
                "query_warehouse": "test_warehouse",
            }
        )

        warnings = [
            str(call) for call in workspace_context.console.warning.call_args_list
        ]
        assert any(
            SPCS_RUNTIME_V2_NAME in w and WAREHOUSE_RUNTIME_NAME in w for w in warnings
        )

    def test_get_alter_sql_does_not_warn_when_runtime_is_unchanged(
        self, workspace_context
    ):
        """No migration, no warning: an unchanged runtime emits no clause to announce."""
        entity = self._runtime_entity(workspace_context)

        entity.get_alter_sql(
            current={
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "query_warehouse": "test_warehouse",
            }
        )

        warnings = [
            str(call) for call in workspace_context.console.warning.call_args_list
        ]
        assert not any("from runtime" in w for w in warnings)

    def test_get_alter_sql_adds_compute_pool_moving_onto_container_runtime(
        self, workspace_context
    ):
        """Moving an app from the warehouse runtime onto the container runtime adds the pool."""
        entity = self._runtime_entity(
            workspace_context,
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="MYPOOL",
        )

        alter_sql = entity.get_alter_sql(
            current={
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "query_warehouse": "test_warehouse",
            }
        )

        assert alter_sql is not None
        assert f"RUNTIME_NAME = '{SPCS_RUNTIME_V2_NAME}'" in alter_sql
        assert "COMPUTE_POOL = 'MYPOOL'" in alter_sql

    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._deploy_legacy"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_warns_when_legacy_discards_warehouse_runtime(
        self,
        mock_bundle,
        mock_deploy_legacy,
        mock_object_exists,
        workspace_context,
        action_context,
    ):
        """--legacy drops RUNTIME_NAME, so say so instead of discarding it silently.

        Driven through deploy() rather than get_deploy_sql, because the guard being
        exercised lives in deploy() and the SQL-level tests bypass it entirely.
        """
        mock_object_exists.return_value = False
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        entity = self._runtime_entity(workspace_context)

        entity.action_deploy(action_context, _open=False, replace=False, legacy=True)

        warnings = [
            str(call) for call in workspace_context.console.warning.call_args_list
        ]
        assert any(
            WAREHOUSE_RUNTIME_NAME in w and "is ignored for --legacy" in w
            for w in warnings
        )

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
            match="is not compatible with the --legacy flag",
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

    def _setup_versioned_replace_mocks(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        workspace_context,
        *,
        existing_is_legacy: bool,
        live_uri: str | None = None,
        describe_side_effect=None,
        describe_return=None,
    ):
        """Shared wiring for versioned --replace deploy tests.

        Returns ``(entity, mock_stage_manager, live_uri)``.
        """
        live_uri = live_uri or (
            f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        )
        mock_object_exists.return_value = True
        mock_is_legacy.return_value = existing_is_legacy
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        if describe_side_effect is not None:
            self.mock_describe.side_effect = describe_side_effect
        elif describe_return is not None:
            self.mock_describe.return_value = describe_return

        mock_stage_manager = mock_stage_manager_cls.return_value
        mock_stage_manager.stage_path_parts_from_str.return_value = mock.Mock()

        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            query_warehouse="test_warehouse",
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(
            workspace_ctx=workspace_context,
            entity_model=model,
        )
        return entity, mock_stage_manager, live_uri

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StageManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_versioned_replace_legacy_recreates_as_versioned(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        mock_sync,
        mock_streamlit_manager_cls,
        workspace_context,
        action_context,
    ):
        """Replacing a legacy ROOT_LOCATION app recreates it as versioned.

        CREATE OR REPLACE (no ROOT_LOCATION) then ADD LIVE VERSION, rather than
        ALTER + ADD LIVE VERSION on an object that still has ROOT_LOCATION.
        """
        live_uri = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        legacy_describe = mock.Mock()
        legacy_describe.fetchone.return_value = {
            "live_version_location_uri": None,
            "query_warehouse": "test_warehouse",
        }
        versioned_describe = mock.Mock()
        versioned_describe.fetchone.return_value = {
            "live_version_location_uri": live_uri,
        }
        # call 1: pre-conversion DESCRIBE (legacy, no URI);
        # call 2: post-ADD-LIVE DESCRIBE inside _ensure_live_version_location_uri
        entity, mock_stage_manager, live_uri = self._setup_versioned_replace_mocks(
            mock_bundle,
            mock_is_legacy,
            mock_object_exists,
            mock_stage_manager_cls,
            workspace_context,
            existing_is_legacy=True,
            live_uri=live_uri,
            describe_side_effect=[legacy_describe, versioned_describe],
        )

        entity.action_deploy(action_context, _open=False, replace=True, legacy=False)

        create_or_replace_calls = [
            c
            for c in self.mock_execute.call_args_list
            if "CREATE OR REPLACE STREAMLIT" in str(c)
        ]
        assert len(create_or_replace_calls) == 1
        assert "ROOT_LOCATION" not in str(create_or_replace_calls[0])
        alter_calls = [
            c for c in self.mock_execute.call_args_list if "ALTER STREAMLIT" in str(c)
        ]
        # Only ADD LIVE VERSION ALTER — no property ALTER on the legacy object
        assert all("ADD LIVE VERSION" in str(c) for c in alter_calls)
        add_live_calls = [
            c for c in self.mock_execute.call_args_list if "ADD LIVE VERSION" in str(c)
        ]
        assert len(add_live_calls) == 1
        mock_stage_manager.stage_path_parts_from_str.assert_called_once_with(live_uri)
        mock_sync.assert_called_once()
        assert mock_sync.call_args.kwargs["force_overwrite"] is True
        assert restart_calls(self.mock_execute_with_params) == []

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StageManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_versioned_replace_existing_versioned_skips_add_live_version(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        mock_sync,
        mock_streamlit_manager_cls,
        workspace_context,
        action_context,
    ):
        """Existing versioned apps already have a live version; do not re-add."""
        live_uri = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = {
            "live_version_location_uri": live_uri,
            "query_warehouse": "test_warehouse",
        }
        entity, mock_stage_manager, live_uri = self._setup_versioned_replace_mocks(
            mock_bundle,
            mock_is_legacy,
            mock_object_exists,
            mock_stage_manager_cls,
            workspace_context,
            existing_is_legacy=False,
            live_uri=live_uri,
            describe_return=mock_cursor,
        )

        entity.action_deploy(action_context, _open=False, replace=True, legacy=False)

        add_live_calls = [
            c for c in self.mock_execute.call_args_list if "ADD LIVE VERSION" in str(c)
        ]
        assert add_live_calls == []
        mock_stage_manager.stage_path_parts_from_str.assert_called_once_with(live_uri)
        # Warehouse DESCRIBE has no SPCS runtime_name — do not CALL restart.
        assert restart_calls(self.mock_execute_with_params) == []

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StageManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_versioned_content_only_replace_restarts_without_alter(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        mock_sync,
        mock_streamlit_manager_cls,
        workspace_context,
        action_context,
    ):
        """SNOW-4014804: content-only --replace (no property ALTER) still restarts.

        yml omits runtime_name; DESCRIBE says the live object is SPCS v2.
        """
        live_uri = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = {
            "live_version_location_uri": live_uri,
            "query_warehouse": "test_warehouse",
            "runtime_name": SPCS_RUNTIME_V2_NAME,
        }
        entity, _, _ = self._setup_versioned_replace_mocks(
            mock_bundle,
            mock_is_legacy,
            mock_object_exists,
            mock_stage_manager_cls,
            workspace_context,
            existing_is_legacy=False,
            live_uri=live_uri,
            describe_return=mock_cursor,
        )
        mock_sync.return_value = stage_diff_with_changes()

        entity.action_deploy(action_context, _open=False, replace=True, legacy=False)

        alter_set_calls = [
            c
            for c in self.mock_execute.call_args_list
            if "ALTER STREAMLIT" in str(c) and "SET" in str(c)
        ]
        assert alter_set_calls == []
        self.mock_execute_with_params.assert_any_call(RESTART_SQL, RESTART_PARAMS)

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StageManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_versioned_spcs_noop_diff_does_not_restart(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        mock_sync,
        mock_streamlit_manager_cls,
        workspace_context,
        action_context,
    ):
        """Unchanged artifacts must not bounce the SPCS service."""
        live_uri = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = {
            "live_version_location_uri": live_uri,
            "query_warehouse": "test_warehouse",
            "runtime_name": SPCS_RUNTIME_V2_NAME,
        }
        entity, _, _ = self._setup_versioned_replace_mocks(
            mock_bundle,
            mock_is_legacy,
            mock_object_exists,
            mock_stage_manager_cls,
            workspace_context,
            existing_is_legacy=False,
            live_uri=live_uri,
            describe_return=mock_cursor,
        )
        mock_sync.return_value = stage_diff_unchanged()

        entity.action_deploy(action_context, _open=False, replace=True, legacy=False)

        assert restart_calls(self.mock_execute_with_params) == []

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StageManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_versioned_first_create_does_not_restart(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        mock_sync,
        mock_streamlit_manager_cls,
        workspace_context,
        action_context,
    ):
        """A new object starts a new process; do not pay a restart cold start."""
        mock_object_exists.return_value = False
        mock_is_legacy.return_value = False
        mock_bundle.return_value = BundleMap(
            project_root=workspace_context.project_root,
            deploy_root=workspace_context.project_root / "output",
        )
        mock_stage_manager_cls.return_value.stage_path_parts_from_str.return_value = (
            mock.Mock()
        )
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            query_warehouse="test_warehouse",
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(
            workspace_ctx=workspace_context,
            entity_model=model,
        )

        entity.action_deploy(action_context, _open=False, replace=False, legacy=False)

        assert restart_calls(self.mock_execute_with_params) == []

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StageManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_versioned_replace_existing_restart_error_fails(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        mock_sync,
        mock_streamlit_manager_cls,
        workspace_context,
        action_context,
    ):
        """A failed restart must fail the deploy: the process was not bounced."""
        live_uri = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = {
            "live_version_location_uri": live_uri,
            "query_warehouse": "test_warehouse",
            "runtime_name": SPCS_RUNTIME_V2_NAME,
        }
        entity, _, _ = self._setup_versioned_replace_mocks(
            mock_bundle,
            mock_is_legacy,
            mock_object_exists,
            mock_stage_manager_cls,
            workspace_context,
            existing_is_legacy=False,
            live_uri=live_uri,
            describe_return=mock_cursor,
        )
        mock_sync.return_value = stage_diff_with_changes()
        unknown_fn = "Unknown function SYSTEM$RESTART_STREAMLIT"

        def _execute_with_params(sql, params=None, **kwargs):
            if "SYSTEM$RESTART_STREAMLIT" in sql:
                raise ProgrammingError(errno=SQL_COMPILATION_ERROR, msg=unknown_fn)
            return mock.Mock()

        self.mock_execute_with_params.side_effect = _execute_with_params

        with pytest.raises(ProgrammingError) as exc_info:
            entity.action_deploy(
                action_context, _open=False, replace=True, legacy=False
            )

        assert exc_info.value.errno == SQL_COMPILATION_ERROR
        mock_sync.assert_called_once()

    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
    )
    @mock.patch("snowflake.cli._plugins.streamlit.streamlit_entity.StageManager")
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._object_exists"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity._is_legacy_deployment"
    )
    @mock.patch(
        "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitEntity.bundle"
    )
    def test_deploy_versioned_replace_existing_restart_privilege_error_fails(
        self,
        mock_bundle,
        mock_is_legacy,
        mock_object_exists,
        mock_stage_manager_cls,
        mock_sync,
        mock_streamlit_manager_cls,
        workspace_context,
        action_context,
    ):
        """Insufficient privilege means the process was not bounced; fail deploy."""
        live_uri = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = {
            "live_version_location_uri": live_uri,
            "query_warehouse": "test_warehouse",
            "runtime_name": SPCS_RUNTIME_V2_NAME,
        }
        entity, _, _ = self._setup_versioned_replace_mocks(
            mock_bundle,
            mock_is_legacy,
            mock_object_exists,
            mock_stage_manager_cls,
            workspace_context,
            existing_is_legacy=False,
            live_uri=live_uri,
            describe_return=mock_cursor,
        )
        mock_sync.return_value = stage_diff_with_changes()

        def _execute_with_params(sql, params=None, **kwargs):
            if "SYSTEM$RESTART_STREAMLIT" in sql:
                raise ProgrammingError(
                    errno=INSUFFICIENT_PRIVILEGES, msg="Insufficient privileges"
                )
            return mock.Mock()

        self.mock_execute_with_params.side_effect = _execute_with_params

        with pytest.raises(ProgrammingError) as exc_info:
            entity.action_deploy(
                action_context, _open=False, replace=True, legacy=False
            )

        assert exc_info.value.errno == INSUFFICIENT_PRIVILEGES
        mock_sync.assert_called_once()

    def test_describe_row_is_spcs_v2(self):
        assert _describe_row_is_spcs_v2({"runtime_name": SPCS_RUNTIME_V2_NAME}) is True
        assert (
            _describe_row_is_spcs_v2({"runtime_name": SPCS_RUNTIME_V2_NAME.lower()})
            is True
        )
        assert (
            _describe_row_is_spcs_v2(
                {"runtime_name": "SYSTEM$ST_CONTAINER_RUNTIME_PY3_12"}
            )
            is True
        )
        assert (
            _describe_row_is_spcs_v2({"runtime_name": "SYSTEM$WAREHOUSE_RUNTIME"})
            is False
        )
        assert _describe_row_is_spcs_v2({}) is False
        assert _describe_row_is_spcs_v2({"runtime_name": None}) is False

    def test_get_restart_sql(self, workspace_context):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)
        assert entity.get_restart_sql() == RESTART_SQL

    def test_restart_passes_identifier_as_bind_parameter(self, workspace_context):
        """A hostile identifier reaches the connector as a bind value, not as SQL text.

        Replaces an earlier test that asserted the identifier was quote-escaped into
        the statement. Binding is the stronger property: there is no escaping left to
        get wrong, and it matches SYSTEM$GET_APPLICATION_SERVICE_LOGS in
        apps/manager.py and SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN in log_streaming.
        """
        hostile = "app'; DROP TABLE users; --"
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        with mock.patch.object(entity, "_get_identifier", return_value=hostile):
            entity._restart_running_app()  # noqa: SLF001

        self.mock_execute_with_params.assert_called_once_with(
            "CALL SYSTEM$RESTART_STREAMLIT(?);", (hostile,)
        )
        # The payload must not appear anywhere in the statement text.
        sent_sql = self.mock_execute_with_params.call_args[0][0]
        assert "DROP TABLE" not in sent_sql

    def test_restart_step_sanitizes_identifier_for_terminal(self, workspace_context):
        """A quoted identifier carrying control characters must not reach the terminal.

        `console.step` does not sanitize (only `styled_message` does), so escape
        sequences in an identifier would otherwise be written out verbatim.
        """
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        with mock.patch.object(
            entity, "_get_identifier", return_value="app\x1b[31mRED\x1b[0m"
        ), mock.patch.object(entity, "_execute_query"):
            entity._restart_running_app()  # noqa: SLF001

        steps = [str(call) for call in workspace_context.console.step.call_args_list]
        assert any("appRED" in s for s in steps)
        assert not any("\x1b" in s for s in steps)

    def test_is_live_version_already_exists_error_matches_errno(self):
        errno_exc = ProgrammingError(errno=99106, msg="duplicate")
        assert _is_live_version_already_exists_error(errno_exc) is True

        sqlstate_exc = ProgrammingError(msg="099106 ... 42710")
        assert _is_live_version_already_exists_error(sqlstate_exc) is True

        message_exc = ProgrammingError(msg="There is already a live version")
        assert _is_live_version_already_exists_error(message_exc) is True

        other = ProgrammingError(errno=1234, msg="something else")
        assert _is_live_version_already_exists_error(other) is False

    def test_ensure_live_version_raises_when_describe_returns_no_row(
        self, workspace_context
    ):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier="test_streamlit",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("test_streamlit")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)
        empty = mock.Mock()
        empty.fetchone.return_value = None
        self.mock_describe.return_value = empty

        with pytest.raises(CliError) as e:
            entity._ensure_live_version_location_uri()  # noqa: SLF001

        message = str(e.value)
        assert "did not report a versioned stage location" in message
        # Keep the error actionable: name a workaround and the field to check.
        assert "--legacy" in message
        assert "live_version_location_uri" in message

    # runtime_name is deliberately absent: it also flows into to_string_literal, but
    # the allowlist rejects a quote-bearing value before any SQL is built, so there is
    # no escaped output to assert on. See
    # test_runtime_name_with_quote_is_rejected_before_sql_generation.
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
