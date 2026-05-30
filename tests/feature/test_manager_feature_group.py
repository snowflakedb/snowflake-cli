"""Phase 5 RED tests — CLI manager wires FeatureGroup state through.

Pin three contracts on the CLI manager:

1. ``init`` scaffolds ``<project_root>/sources/feature_groups/`` with a
   ``.gitkeep`` so the directory survives ``git add`` on a fresh project.
2. ``init``'s export pipeline forwards FG rows from
   :func:`decl_api.fetch_feature_group_rows` into
   :func:`decl_api.export_specs` via the ``feature_group_rows`` kwarg.
3. ``plan`` / ``apply`` / ``list`` paths call
   :func:`decl_api.fetch_feature_group_rows` and forward the result into
   :func:`decl_api.fetch_applied_state`.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest import mock

import pytest

_DEFAULT_MANIFEST_YAML = textwrap.dedent(
    """\
    manifest_version: 1
    type: feature_store
    default_target: DEFAULT
    targets:
      DEFAULT:
        account_identifier: TEST_ORG-TEST_ACCT
        database: TEST_DB
        schema: TEST_SCHEMA
        role: TEST_ROLE
    """
)


def _write_manifest(project_root: Path) -> Path:
    project_root.mkdir(parents=True, exist_ok=True)
    p = project_root / "manifest.yml"
    p.write_text(_DEFAULT_MANIFEST_YAML)
    return p


# ---------------------------------------------------------------------------
# Shared fixtures (mirror the structure of test_manager.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_execute_query():
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


@pytest.fixture
def mock_decl():
    with mock.patch("snowflake.cli._plugins.feature.manager.decl_api") as m:
        m.fetch_applied_state.return_value = mock.MagicMock(name="state")
        m.validate_specs.return_value = []
        m.generate_plan.return_value = mock.MagicMock(name="plan", ops=[], warnings=[])
        m.serialize_plan.return_value = json.dumps(
            {
                "version": "1",
                "created_at": "2026-05-11T00:00:00+00:00",
                "target_database": "TEST_DB",
                "target_schema": "TEST_SCHEMA",
                "target_name": "DEFAULT",
                "source_files": [],
                "plan": {"ops": [], "warnings": []},
                "summary": {},
            }
        )
        m.state_queries.return_value = {
            "show_ofts": "SHOW",
            "show_tables": "SHOW",
            "describe_specification_template": 'DESCRIBE "{name}"',
        }
        m.list_state_queries.return_value = {
            "show_ofts": "SHOW",
            "describe_specification_template": 'DESCRIBE "{name}"',
        }
        m.fetch_entity_rows.return_value = []
        m.fetch_feature_view_rows.return_value = []
        m.fetch_feature_group_rows.return_value = []
        m.parse_specification_rows.return_value = None
        m.enrich_list_results.return_value = []
        exec_result = mock.MagicMock()
        exec_result.status = "applied"
        exec_result.ops = []
        exec_result.warnings = []
        exec_result.errors = []
        m.execute_plan.return_value = exec_result
        m.export_specs.return_value = {
            "status": "exported",
            "directory": "",
            "files": [],
        }
        m.assert_feature_store_initialized = mock.MagicMock(
            name="assert_feature_store_initialized",
            return_value=mock.MagicMock(name="FeatureStore"),
        )
        yield m


@pytest.fixture(autouse=True)
def mock_cli_context():
    with mock.patch("snowflake.cli._plugins.feature.manager.get_cli_context") as m:
        ctx = mock.MagicMock()
        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"
        ctx.connection.warehouse = "TEST_WH"
        ctx.connection.role = "TEST_ROLE"
        ctx.connection.account = "TEST_ORG-TEST_ACCT"
        m.return_value = ctx
        yield m


@pytest.fixture(autouse=True)
def mock_account_identifier():
    from snowflake.cli.api.identifiers import AccountIdentifier

    with mock.patch(
        "snowflake.cli._plugins.feature.manager.get_account_identifier",
        return_value=AccountIdentifier("TEST_ORG", "TEST_ACCT"),
    ) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_build_session():
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._build_session",
        return_value=mock.MagicMock(name="session"),
    ):
        yield


def _patch_feature_store():
    fs_patch = mock.patch("snowflake.ml.feature_store.feature_store.FeatureStore")
    cm_patch = mock.patch("snowflake.ml.feature_store.feature_store.CreationMode")
    return fs_patch, cm_patch


# ---------------------------------------------------------------------------
# init scaffolding
# ---------------------------------------------------------------------------


class TestInitScaffoldsFeatureGroups:
    def test_init_creates_sources_feature_groups_dir(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = _patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        fg_dir = tmp_path / "sources" / "feature_groups"
        assert fg_dir.is_dir(), "sources/feature_groups/ must be scaffolded by init"

    def test_init_writes_feature_groups_gitkeep(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = _patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        gitkeep = tmp_path / "sources" / "feature_groups" / ".gitkeep"
        assert gitkeep.is_file(), (
            "sources/feature_groups/.gitkeep must be written so the empty "
            "directory survives `git add` on a fresh project"
        )


# ---------------------------------------------------------------------------
# init export pipeline forwards FG rows
# ---------------------------------------------------------------------------


class TestInitExportForwardsFGRows:
    def test_init_forwards_feature_group_rows_to_export_specs(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        # Drive a non-empty FG row set so the kwarg shape is non-trivial.
        fg_rows = [
            {
                "name": "USER_FRAUD_FG",
                "version": "V1",
                "desc": "",
                "owner": "ROLE",
                "auto_prefix": True,
                "sources": [{"fv_name": "FV_A", "fv_version": "V1"}],
                "output_columns": None,
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
            }
        ]
        mock_decl.fetch_feature_group_rows.return_value = fg_rows

        fs_patch, cm_patch = _patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        # The exporter is called once during init's _export_into_sources.
        mock_decl.export_specs.assert_called_once()
        kwargs = mock_decl.export_specs.call_args.kwargs
        assert kwargs.get("feature_group_rows") == fg_rows


# ---------------------------------------------------------------------------
# plan / apply / list forward FG rows into fetch_applied_state
# ---------------------------------------------------------------------------


class TestPlanWiresFGRows:
    def test_plan_forwards_feature_group_rows_to_fetch_applied_state(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        fg_rows = [
            {
                "name": "FG_A",
                "version": "V1",
                "desc": "",
                "owner": "ROLE",
                "auto_prefix": True,
                "sources": [],
                "output_columns": None,
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
            }
        ]
        mock_decl.fetch_feature_group_rows.return_value = fg_rows

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        mock_decl.fetch_applied_state.assert_called()
        kwargs = mock_decl.fetch_applied_state.call_args.kwargs
        assert kwargs.get("feature_group_rows") == fg_rows


class TestListWiresFGRows:
    def test_list_specs_forwards_feature_group_rows_to_enrich(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        fg_rows = [
            {
                "name": "FG_A",
                "version": "V1",
                "desc": "",
                "owner": "ROLE",
                "auto_prefix": True,
                "sources": [],
                "output_columns": None,
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
            }
        ]
        mock_decl.fetch_feature_group_rows.return_value = fg_rows

        FeatureManager().list_specs(from_dir=tmp_path, target_name=None)

        mock_decl.enrich_list_results.assert_called()
        kwargs = mock_decl.enrich_list_results.call_args.kwargs
        assert kwargs.get("feature_group_rows") == fg_rows
