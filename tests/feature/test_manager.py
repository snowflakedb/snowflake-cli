# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for FeatureManager — new manifest-driven shape (Phase 3+4).

The CLI surface is DCM-strict (D3): every Snowflake-bound command takes
``from_dir=<dir>`` (default cwd) and ``target_name=<name>`` (default =
manifest's ``default_target``). The manager resolves the on-disk project
via :func:`decl_api.discover_project` / :func:`decl_api.load_manifest`
/ :func:`decl_api.resolve_target`, asserts the active connection's
account matches the target's ``account_identifier`` (D4), and then
delegates state SQL / plan generation / execution to ``decl_api``.

Tests cover:

* :class:`TestResolveProject` — the private resolver (D4 account match,
  manifest discovery, default-target resolution).
* :class:`TestFeatureManagerInit` — manifest scaffolding + ``init-exist``
  fail-fast (D6).
* :class:`TestFeatureManagerPlan` — read-only validate + plan against
  the manifest target (no SQL strings in the manager).
* :class:`TestWritePlan` — plan persistence under
  ``<project_root>/out/plan/`` (D8, relocated).
* :class:`TestApplyCommand` — L1–L7 plan-file lifecycle (preserved,
  relocated to ``out/plan/``).  L6 widened to cover both account and
  ``target_name`` mismatch (D4 + D4-ext).
* :class:`TestFeatureManagerListSpecs` / :class:`TestFeatureManagerDescribe`
  / :class:`TestFeatureManagerExportSpecs` — every Snowflake-bound
  command runs through the manifest resolver.
* :class:`TestFeatureManagerIngest` / :class:`TestFeatureManagerQuery`
  — preserved snowml-core delegation contract (unchanged by Phase 3+4).
* :class:`TestSurfaceDeletions` — the deleted helpers
  (``_expand_with_datasources``, ``_is_full_sync``) MUST stay gone.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest import mock

import pytest

# ---------------------------------------------------------------------------
# Manifest helpers — every Snowflake-bound test layouts a minimal project
# under ``tmp_path`` and points the manager at it via ``from_dir=tmp_path``.
# ---------------------------------------------------------------------------


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


def _write_manifest(
    project_root: Path,
    *,
    yaml_text: str = _DEFAULT_MANIFEST_YAML,
) -> Path:
    """Write *yaml_text* to ``<project_root>/manifest.yml`` and return the path."""
    project_root.mkdir(parents=True, exist_ok=True)
    manifest = project_root / "manifest.yml"
    manifest.write_text(yaml_text)
    return manifest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_execute_query():
    """Patch ``FeatureManager.execute_query`` so tests don't need a real connection."""
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


@pytest.fixture
def mock_decl():
    """Patch the ``decl_api`` module the manager imports.

    Most facades resolve to MagicMocks with sensible defaults; tests
    that need a non-default behaviour overwrite the relevant attribute.
    """
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
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }
        m.list_state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_entities": (
                "SHOW TAGS LIKE 'SNOWML_FEATURE_STORE_ENTITY_%' "
                "IN SCHEMA TEST_DB.TEST_SCHEMA"
            ),
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }
        m.fetch_entity_rows.return_value = []
        m.parse_specification_rows.return_value = None
        m.enrich_list_results.return_value = []
        m.list_query.return_value = (
            "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
        )
        m.describe_query.return_value = (
            "SHOW ONLINE FEATURE TABLES LIKE 'test' IN SCHEMA TEST_DB.TEST_SCHEMA"
        )
        m.drop_queries.return_value = [
            'DROP ONLINE FEATURE TABLE IF EXISTS "TEST_DB"."TEST_SCHEMA"."test"'
        ]
        exec_result = mock.MagicMock()
        exec_result.status = "applied"
        exec_result.ops = []
        exec_result.warnings = []
        exec_result.errors = []
        m.execute_plan.return_value = exec_result

        # decl_api.export_specs is a regular function in the real module;
        # in the mock we hand back a sensible result dict by default.
        m.export_specs.return_value = {
            "status": "exported",
            "directory": "",
            "files": [],
        }

        # ``assert_feature_store_initialized`` is the init-first guard
        # (docs/DEVELOPMENT_STANDARDS.md rule #11).  ``MagicMock``
        # special-cases any attribute that starts with ``assert_``
        # (treats it as one of mock's built-in assertion helpers,
        # which raises ``AttributeError`` when called), so we must
        # assign a regular ``MagicMock`` to that name explicitly to
        # override the auto-attribute magic.  Default behaviour:
        # no-op (return a fake FeatureStore), i.e. "the schema is
        # already initialised, proceed with the test".  Tests that
        # need to drive the negative path overwrite this directly:
        # ``mock_decl.assert_feature_store_initialized.side_effect =
        #   decl_api.FeatureStoreNotInitializedError(...)``.
        m.assert_feature_store_initialized = mock.MagicMock(
            name="assert_feature_store_initialized",
            return_value=mock.MagicMock(name="FeatureStore"),
        )
        yield m


@pytest.fixture(autouse=True)
def mock_cli_context():
    """Patch ``get_cli_context`` for every manager test."""
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
    """Stub ``get_account_identifier`` to match the default manifest's account.

    The autouse default makes the L6 account check pass for every test
    that doesn't override it; the account-mismatch tests overwrite the
    return value to drive the resolver into the failure branch.
    """
    from snowflake.cli.api.identifiers import AccountIdentifier

    with mock.patch(
        "snowflake.cli._plugins.feature.manager.get_account_identifier",
        return_value=AccountIdentifier("TEST_ORG", "TEST_ACCT"),
    ) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_build_session():
    """Patch ``_build_session`` so tests don't construct a real Snowpark Session."""
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._build_session",
        return_value=mock.MagicMock(name="session"),
    ):
        yield


def _executed_sqls(mock_execute_query):
    sqls = []
    for call in mock_execute_query.call_args_list:
        if call.args:
            sqls.append(str(call.args[0]))
    return sqls


def _make_plan_json(
    *,
    target_database: str = "TEST_DB",
    target_schema: str = "TEST_SCHEMA",
    target_name: str = "DEFAULT",
) -> str:
    """Return a minimal valid PlanFile JSON envelope."""
    return json.dumps(
        {
            "version": "1",
            "created_at": "2026-05-11T00:00:00+00:00",
            "target_database": target_database,
            "target_schema": target_schema,
            "target_name": target_name,
            "source_files": ["fv.yaml"],
            "plan": {"ops": [], "warnings": []},
            "summary": {},
        }
    )


def _make_plans_dir(project_root: Path) -> Path:
    """Create ``<project_root>/out/plan/`` and return the Path (D8 relocated)."""
    plans_dir = project_root / "out" / "plan"
    plans_dir.mkdir(parents=True, exist_ok=True)
    return plans_dir


# ===========================================================================
# _resolve_project — the new private helper
# ===========================================================================


class TestResolveProject:
    """``FeatureManager._resolve_project(from_dir, target_name)`` walks
    up from ``from_dir`` to find ``manifest.yml``, loads it, resolves
    the named target (or ``default_target``), and asserts the active
    connection's account_identifier matches the target's (D4 /
    L6-extension)."""

    def test_resolve_project_returns_paths_manifest_target_triple(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Happy path: returns ``(FSProjectPaths, FSManifest, FSTarget)``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.manifest import FSManifest, FSTarget
        from snowflake.ml.feature_store.decl.project_paths import FSProjectPaths

        _write_manifest(tmp_path)

        paths, manifest, target = FeatureManager()._resolve_project(  # noqa: SLF001
            from_dir=tmp_path, target_name=None
        )

        assert isinstance(paths, FSProjectPaths)
        assert isinstance(manifest, FSManifest)
        assert isinstance(target, FSTarget)
        assert paths.project_root == tmp_path.resolve()

    def test_resolve_project_uses_default_target_when_target_name_none(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``target_name=None`` resolves to the manifest's ``default_target``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        _, _, target = FeatureManager()._resolve_project(  # noqa: SLF001
            from_dir=tmp_path, target_name=None
        )
        assert target.name == "DEFAULT"

    def test_resolve_project_target_lookup_is_case_insensitive(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Target names normalise via ``upper()`` (mirrors DCM behavior)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(
            tmp_path,
            yaml_text=textwrap.dedent(
                """\
                manifest_version: 1
                type: feature_store
                default_target: PROD
                targets:
                  DEV:
                    account_identifier: TEST_ORG-TEST_ACCT
                    database: TEST_DB
                    schema: TEST_SCHEMA
                  PROD:
                    account_identifier: TEST_ORG-TEST_ACCT
                    database: TEST_DB
                    schema: TEST_SCHEMA
                """
            ),
        )
        _, _, target = FeatureManager()._resolve_project(  # noqa: SLF001
            from_dir=tmp_path, target_name="dev"
        )
        assert target.name == "DEV"

    def test_resolve_project_missing_manifest_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Absent ``manifest.yml`` → ``CliError`` naming the start path."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError) as excinfo:
            FeatureManager()._resolve_project(  # noqa: SLF001
                from_dir=tmp_path, target_name=None
            )
        msg = str(excinfo.value)
        assert "manifest.yml" in msg or "manifest" in msg.lower()

    def test_resolve_project_account_mismatch_raises_cli_error(
        self,
        mock_execute_query,
        mock_decl,
        mock_account_identifier,
        tmp_path,
    ):
        """L6-extension: account mismatch → ``CliError`` naming both sides."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError
        from snowflake.cli.api.identifiers import AccountIdentifier

        _write_manifest(tmp_path)
        # Connection reports a different account than the manifest target.
        mock_account_identifier.return_value = AccountIdentifier(
            "OTHER_ORG", "OTHER_ACCT"
        )

        with pytest.raises(CliError) as excinfo:
            FeatureManager()._resolve_project(  # noqa: SLF001
                from_dir=tmp_path, target_name=None
            )
        msg = str(excinfo.value)
        # The error must name both account identifiers so operators can
        # see exactly which side is wrong.
        assert "OTHER_ORG" in msg or "OTHER_ACCT" in msg
        assert "TEST_ORG" in msg or "TEST_ACCT" in msg

    def test_resolve_project_unknown_target_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Asking for a target the manifest does not declare → ``CliError``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        _write_manifest(tmp_path)
        with pytest.raises(CliError):
            FeatureManager()._resolve_project(  # noqa: SLF001
                from_dir=tmp_path, target_name="MISSING"
            )

    def test_resolve_project_walks_up_to_find_manifest(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``discover_project`` walks up from ``from_dir`` to find the manifest."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        nested = tmp_path / "sub" / "deep"
        nested.mkdir(parents=True)
        paths, _, _ = FeatureManager()._resolve_project(  # noqa: SLF001
            from_dir=nested, target_name=None
        )
        assert paths.project_root == tmp_path.resolve()


# ===========================================================================
# init — manifest scaffolding (D6 + init-exist)
# ===========================================================================


class TestFeatureManagerInit:
    """``FeatureManager.init`` is the single bootstrap entry point.

    The init-subsumes-export contract is:

    1. Always operate in CWD (no ``from_dir``).  Callers pass a project
       root via ``project_root``; the new Typer command always resolves
       it to ``Path.cwd()``.
    2. ``--no-scaffold`` is gone.  Init is idempotent end-to-end: the
       manifest is written only when absent, but the FS bootstrap and
       the export-into-``sources/`` always re-run.
    3. ``--target NAME`` names the manifest target on a fresh init
       (default ``DEFAULT``); on a re-init it picks which existing
       manifest target to export from.
    4. ``--database`` / ``--schema`` override the active connection's
       defaults when a fresh manifest is being scaffolded.  They are
       ignored on a re-init (the manifest is the source of truth).
    5. After scaffolding, the export pipeline lands its YAMLs under
       ``<project_root>/sources/{entities,datasources,feature_views}/``
       via ``decl_api.export_specs(..., layout="sources")``.
    """

    def _patch_feature_store(self):
        """Convenience: patch the imperative ``FeatureStore`` + ``CreationMode``."""
        return (
            mock.patch("snowflake.ml.feature_store.feature_store.FeatureStore"),
            mock.patch("snowflake.ml.feature_store.feature_store.CreationMode"),
        )

    # ------------------------------------------------------------------
    # Fresh init — manifest creation
    # ------------------------------------------------------------------

    def test_init_writes_manifest_yml(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Happy path: a fresh ``init`` writes a parseable ``manifest.yml``."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        manifest_path = tmp_path / "manifest.yml"
        assert manifest_path.is_file()
        parsed = yaml.safe_load(manifest_path.read_text())
        assert parsed["manifest_version"] == 1
        assert parsed["type"] == "feature_store"
        assert parsed["default_target"] == "DEFAULT"
        assert "DEFAULT" in parsed["targets"]

    def test_init_populates_manifest_from_active_connection(
        self,
        mock_execute_query,
        mock_decl,
        mock_cli_context,
        mock_account_identifier,
        tmp_path,
    ):
        """db / schema / role come from the connection;
        ``account_identifier`` comes from
        :func:`get_account_identifier` (canonical ``<ORG>-<ACCOUNT>``).
        """
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.identifiers import AccountIdentifier

        mock_cli_context.return_value.connection.account = "my_acct"
        mock_cli_context.return_value.connection.database = "MY_DB"
        mock_cli_context.return_value.connection.schema = "MY_SCHEMA"
        mock_cli_context.return_value.connection.role = "MY_ROLE"
        mock_account_identifier.return_value = AccountIdentifier("MY_ORG", "MY_ACCT")

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        parsed = yaml.safe_load((tmp_path / "manifest.yml").read_text())
        target = parsed["targets"]["DEFAULT"]
        assert target["account_identifier"] == "MY_ORG-MY_ACCT"
        assert target["database"] == "MY_DB"
        assert target["schema"] == "MY_SCHEMA"
        assert target["role"] == "MY_ROLE"

    def test_init_account_identifier_falls_back_when_query_fails(
        self,
        mock_execute_query,
        mock_decl,
        mock_cli_context,
        mock_account_identifier,
        tmp_path,
    ):
        """If :func:`get_account_identifier` raises, init still writes a
        manifest using the connection's ``account`` so the operator can
        edit it.
        """
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_cli_context.return_value.connection.account = "fallback_acct"
        mock_account_identifier.side_effect = RuntimeError("simulated session failure")

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        parsed = yaml.safe_load((tmp_path / "manifest.yml").read_text())
        assert parsed["targets"]["DEFAULT"]["account_identifier"] == "fallback_acct"

    def test_init_does_not_write_warehouse_field(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """D2: ``warehouse`` MUST NOT appear in the generated manifest."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        text = (tmp_path / "manifest.yml").read_text()
        assert "warehouse" not in text.lower()

    # ------------------------------------------------------------------
    # Fresh init — --target / --database / --schema overrides
    # ------------------------------------------------------------------

    def test_init_target_name_overrides_default_in_fresh_manifest(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``--target STAGING`` names the only target ``STAGING`` (not
        ``DEFAULT``) and sets it as ``default_target`` on a brand-new
        manifest."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path, target_name="STAGING")

        parsed = yaml.safe_load((tmp_path / "manifest.yml").read_text())
        assert parsed["default_target"] == "STAGING"
        assert "STAGING" in parsed["targets"]
        assert "DEFAULT" not in parsed["targets"]

    def test_init_database_and_schema_overrides_supersede_connection(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``--database`` / ``--schema`` win over the active connection's
        defaults when scaffolding a brand-new manifest."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_cli_context.return_value.connection.database = "CONN_DB"
        mock_cli_context.return_value.connection.schema = "CONN_SCHEMA"

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(
                project_root=tmp_path,
                database="OVERRIDE_DB",
                schema="OVERRIDE_SCHEMA",
            )

        parsed = yaml.safe_load((tmp_path / "manifest.yml").read_text())
        target = parsed["targets"][parsed["default_target"]]
        assert target["database"] == "OVERRIDE_DB"
        assert target["schema"] == "OVERRIDE_SCHEMA"

    def test_init_overrides_drive_feature_store_and_export(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Overrides flow into both the FS bootstrap AND the export call."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_cli_context.return_value.connection.database = "CONN_DB"
        mock_cli_context.return_value.connection.schema = "CONN_SCHEMA"

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch as mock_fs_cls, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(
                project_root=tmp_path,
                database="OVERRIDE_DB",
                schema="OVERRIDE_SCHEMA",
            )

        # FeatureStore is built against the override db/schema.
        positional = mock_fs_cls.call_args.args
        assert "OVERRIDE_DB" in positional
        assert "OVERRIDE_SCHEMA" in positional

        # Export is called against the override db/schema, in sources layout.
        export_call = mock_decl.export_specs.call_args
        assert export_call is not None, "init must run the export pipeline"
        # signature: (show_rows, describe, output_dir, db, schema, **kwargs)
        assert export_call.args[3] == "OVERRIDE_DB"
        assert export_call.args[4] == "OVERRIDE_SCHEMA"
        assert export_call.kwargs.get("layout") == "sources"

    # ------------------------------------------------------------------
    # Scaffold side effects (always-on; no --no-scaffold escape).
    # ------------------------------------------------------------------

    def test_init_scaffolds_sources_subdirs(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``sources/{entities,datasources,feature_views}/`` exist after init."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        for sub in ("entities", "datasources", "feature_views"):
            assert (
                tmp_path / "sources" / sub
            ).is_dir(), f"Expected sources/{sub}/ to exist after init scaffold"

    def test_init_writes_out_plan_gitkeep(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``out/plan/.gitkeep`` is written so plan-discovery is tracked."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        gitkeep = tmp_path / "out" / "plan" / ".gitkeep"
        assert gitkeep.is_file()

    def test_init_calls_feature_store_with_create_if_not_exist(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Snowflake-side init runs ``FeatureStore(..., CREATE_IF_NOT_EXIST)``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch as mock_fs_cls, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        mock_fs_cls.assert_called_once()
        kwargs = mock_fs_cls.call_args[1]
        assert kwargs["creation_mode"] == mock_cm.CREATE_IF_NOT_EXIST

    def test_init_runs_export_into_sources_layout(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Init pulls deployed artifacts into ``<project_root>/sources/``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        mock_decl.export_specs.assert_called_once()
        call = mock_decl.export_specs.call_args
        # The output dir handed to decl_api.export_specs is the project
        # root (NOT a per-DB subdir): the exporter then writes into
        # <project_root>/sources/{...}/ when layout="sources".
        output_dir = call.args[2]
        assert Path(output_dir) == tmp_path.resolve()
        assert call.kwargs.get("layout") == "sources"

    def test_init_returns_status_initialized(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """init() returns ``{status, project_root, manifest_path, target, export}``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = FeatureManager().init(project_root=tmp_path)

        assert result["status"] == "initialized"
        assert Path(result["project_root"]) == tmp_path.resolve()
        assert Path(result["manifest_path"]) == (tmp_path / "manifest.yml").resolve()
        assert result["target"] == "DEFAULT"
        # Export envelope is surfaced so the operator sees what landed.
        assert "export" in result
        assert result["export"]["status"] == "exported"

    # ------------------------------------------------------------------
    # Idempotent re-init — manifest preserved, FS + export re-run.
    # ------------------------------------------------------------------

    def test_init_with_existing_manifest_does_not_overwrite_it(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """A re-init must NEVER overwrite an existing ``manifest.yml``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        original = _DEFAULT_MANIFEST_YAML
        _write_manifest(tmp_path, yaml_text=original)

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        # Bytes-identical preservation.
        assert (tmp_path / "manifest.yml").read_text() == original

    def test_init_with_existing_manifest_returns_skipped_manifest_status(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """The result envelope flags that the manifest write was skipped."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = FeatureManager().init(project_root=tmp_path)

        # Status reports the idempotent re-init shape.
        assert result["status"] == "initialized"
        assert result["manifest_written"] is False

    def test_init_with_existing_manifest_still_runs_feature_store_bootstrap(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """FS bootstrap must re-run on a re-init (idempotent CREATE_IF_NOT_EXIST)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch as mock_fs_cls, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        mock_fs_cls.assert_called_once()
        kwargs = mock_fs_cls.call_args[1]
        assert kwargs["creation_mode"] == mock_cm.CREATE_IF_NOT_EXIST

    def test_init_with_existing_manifest_still_runs_export(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Export must re-run on a re-init so artifacts stay fresh."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        mock_decl.export_specs.assert_called_once()
        assert mock_decl.export_specs.call_args.kwargs.get("layout") == "sources"

    def test_init_with_existing_manifest_rejects_mismatched_database_override(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Re-init with ``--database`` that differs from the resolved
        manifest target raises ``CliError`` instead of silently
        ignoring the override.

        Locks in the reject-on-conflict policy: the manifest is the
        source of truth on re-init, so a non-matching override is a
        user error the CLI must surface (with an actionable directive
        to edit ``manifest.yml`` or pick a different ``--target``).
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        _write_manifest(tmp_path)  # manifest target = TEST_DB / TEST_SCHEMA

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch as mock_fs_cls, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            with pytest.raises(CliError, match="manifest"):
                FeatureManager().init(
                    project_root=tmp_path,
                    database="OTHER_DB",
                )

        # No side-effects on rejection: FS bootstrap and export must
        # not run.
        mock_fs_cls.assert_not_called()
        mock_decl.export_specs.assert_not_called()

    def test_init_with_existing_manifest_rejects_mismatched_schema_override(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Re-init with ``--schema`` that differs from the resolved
        manifest target raises ``CliError`` (mirror of the
        ``--database`` mismatch test).
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        _write_manifest(tmp_path)  # manifest target = TEST_DB / TEST_SCHEMA

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch as mock_fs_cls, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            with pytest.raises(CliError, match="manifest"):
                FeatureManager().init(
                    project_root=tmp_path,
                    schema="OTHER_SCHEMA",
                )

        mock_fs_cls.assert_not_called()
        mock_decl.export_specs.assert_not_called()

    def test_init_with_existing_manifest_matching_overrides_is_noop(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Re-init with ``--database`` / ``--schema`` values that match
        the resolved manifest target is accepted (no ``CliError``).

        Matching overrides are operationally a no-op — the FS bootstrap
        and export still run against the manifest target's values, the
        manifest file is left untouched.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)  # manifest target = TEST_DB / TEST_SCHEMA
        manifest_before = (tmp_path / "manifest.yml").read_bytes()

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch as mock_fs_cls, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(
                project_root=tmp_path,
                database="TEST_DB",
                schema="TEST_SCHEMA",
            )

        # FS bootstrap + export run against the manifest target.
        export_call = mock_decl.export_specs.call_args
        assert export_call.args[3] == "TEST_DB"
        assert export_call.args[4] == "TEST_SCHEMA"
        positional = mock_fs_cls.call_args.args
        assert "TEST_DB" in positional
        assert "TEST_SCHEMA" in positional

        # Manifest is preserved bytes-identical.
        assert (tmp_path / "manifest.yml").read_bytes() == manifest_before

    def test_init_with_existing_manifest_resolves_named_target(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``--target NAME`` on a re-init picks the matching manifest target."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        multi_target_yaml = textwrap.dedent(
            """\
            manifest_version: 1
            type: feature_store
            default_target: DEFAULT
            targets:
              DEFAULT:
                account_identifier: TEST_ORG-TEST_ACCT
                database: TEST_DB
                schema: TEST_SCHEMA
              STAGING:
                account_identifier: TEST_ORG-TEST_ACCT
                database: STAGING_DB
                schema: STAGING_SCHEMA
            """
        )
        _write_manifest(tmp_path, yaml_text=multi_target_yaml)

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path, target_name="STAGING")

        export_call = mock_decl.export_specs.call_args
        assert export_call.args[3] == "STAGING_DB"
        assert export_call.args[4] == "STAGING_SCHEMA"

    def test_init_is_idempotent_on_repeated_calls(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """Two consecutive ``init`` calls produce the same on-disk shape."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)
            first_text = (tmp_path / "manifest.yml").read_text()
            FeatureManager().init(project_root=tmp_path)
            second_text = (tmp_path / "manifest.yml").read_text()

        assert first_text == second_text
        for sub in ("entities", "datasources", "feature_views"):
            assert (tmp_path / "sources" / sub).is_dir()

    # ------------------------------------------------------------------
    # Surface deletions — old kwargs are gone.
    # ------------------------------------------------------------------

    def test_init_no_longer_accepts_no_scaffold_kwarg(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """The ``--no-scaffold`` escape hatch is removed."""
        import inspect

        from snowflake.cli._plugins.feature.manager import FeatureManager

        sig = inspect.signature(FeatureManager.init)
        assert "no_scaffold" not in sig.parameters

    def test_init_no_longer_accepts_from_dir_kwarg(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """The ``--from`` / ``from_dir`` arg is removed; init runs in CWD."""
        import inspect

        from snowflake.cli._plugins.feature.manager import FeatureManager

        sig = inspect.signature(FeatureManager.init)
        assert "from_dir" not in sig.parameters


# ===========================================================================
# plan — read-only validate + plan against the manifest target
# ===========================================================================


class TestFeatureManagerPlan:
    """``FeatureManager.plan`` runs ``decl_api.load_project`` (manifest-driven
    spec load) → ``validate_specs`` → ``generate_plan``.  No SQL strings.
    """

    def test_plan_returns_status_ready_when_no_validation_errors(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.enums import OpKind

        _write_manifest(tmp_path)
        op = mock.MagicMock()
        op.kind = OpKind.NO_CHANGE
        op.name = "USER"
        op.reason = ""
        op.destructive = False
        plan_obj = mock.MagicMock(name="plan", ops=[op], warnings=[])
        mock_decl.generate_plan.return_value = plan_obj

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "ready"
        assert result["errors"] == []
        assert len(result["ops"]) == 1

    def test_plan_returns_validation_failed_when_validate_specs_returns_errors(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        err = mock.MagicMock()
        err.severity = "ERROR"
        err.code = "VERSION_CONFLICT"
        err.message = "Version conflict on FV X"
        err.__str__ = lambda self: "VERSION_CONFLICT: Version conflict on FV X"
        mock_decl.validate_specs.return_value = [err]

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["status"] == "validation_failed"
        assert result["errors"]
        mock_decl.generate_plan.assert_not_called()

    def test_plan_loads_project_via_decl_api(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``plan`` MUST go through ``decl_api.load_project`` (manifest-aware)
        rather than the legacy ``load_specs(input_files, ...)`` path."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        mock_decl.load_project.assert_called_once()

    def test_plan_dev_mode_threads_into_validate_specs(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``--dev`` MUST forward into ``decl_api.validate_specs`` so
        relaxed version validation actually applies. A regression here
        surfaces as ``MISSING_VERSION`` errors on a freshly-authored
        feature view despite ``--dev``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=True,
            allow_recreate=False,
        )

        kwargs = mock_decl.validate_specs.call_args.kwargs
        assert kwargs.get("dev_mode") is True, (
            "manager.plan must forward dev_mode=True to "
            f"decl_api.validate_specs; got kwargs={kwargs!r}"
        )

    def test_plan_dev_mode_false_threads_into_validate_specs(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """The strict-mode path must explicitly pass ``dev_mode=False``
        rather than relying on the api's default, so the contract is
        exercised in both directions.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        kwargs = mock_decl.validate_specs.call_args.kwargs
        assert kwargs.get("dev_mode") is False, (
            "manager.plan must forward dev_mode=False explicitly to "
            f"decl_api.validate_specs; got kwargs={kwargs!r}"
        )

    def test_plan_runtime_variables_flow_through_to_load_project(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``--variable key=value`` → parsed dict → ``load_project(runtime_vars=...)``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=["env_suffix=_DEV", "tenant=acme"],
            dev_mode=False,
            allow_recreate=False,
        )

        call_kwargs = mock_decl.load_project.call_args.kwargs
        runtime_vars = call_kwargs.get("runtime_vars") or {}
        assert runtime_vars.get("env_suffix") == "_DEV"
        assert runtime_vars.get("tenant") == "acme"

    def test_plan_default_target_used_when_target_name_omitted(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Omitting ``target_name`` selects the manifest's ``default_target``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        # The result envelope carries the resolved target name so the
        # CLI header can render it.
        assert result.get("target_name") == "DEFAULT"

    def test_plan_target_info_uses_manifest_db_schema_and_connection_warehouse(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """D2 + D4: db/schema from manifest target; warehouse from connection."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(
            tmp_path,
            yaml_text=textwrap.dedent(
                """\
                manifest_version: 1
                type: feature_store
                default_target: DEFAULT
                targets:
                  DEFAULT:
                    account_identifier: TEST_ORG-TEST_ACCT
                    database: MANIFEST_DB
                    schema: MANIFEST_SCHEMA
                """
            ),
        )

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["target_database"] == "MANIFEST_DB"
        assert result["target_schema"] == "MANIFEST_SCHEMA"
        assert result["target_warehouse"] == "TEST_WH"

    def test_plan_missing_manifest_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError):
            FeatureManager().plan(
                from_dir=tmp_path,
                target_name=None,
                variables=[],
                dev_mode=False,
                allow_recreate=False,
            )

    def test_plan_account_mismatch_raises_cli_error(
        self,
        mock_execute_query,
        mock_decl,
        mock_account_identifier,
        tmp_path,
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError
        from snowflake.cli.api.identifiers import AccountIdentifier

        _write_manifest(tmp_path)
        mock_account_identifier.return_value = AccountIdentifier(
            "OTHER_ORG", "OTHER_ACCT"
        )

        with pytest.raises(CliError):
            FeatureManager().plan(
                from_dir=tmp_path,
                target_name=None,
                variables=[],
                dev_mode=False,
                allow_recreate=False,
            )

    # ------------------------------------------------------------------
    # W-G.4 — manager fetches SHOW DYNAMIC TABLES and threads the resulting
    # ``dt_text_map`` into ``fetch_applied_state`` so that the planner can
    # recover the offline source-table binding for BatchFVs that lose it
    # in the ``DESCRIBE … TYPE = SPECIFICATION`` round-trip.  See
    # docs/BATCH_FV_BUG_BASH.md §6–§8 and the W-G(A) plan.
    # ------------------------------------------------------------------

    def test_plan_executes_show_dynamic_tables_query(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``plan`` MUST run the ``show_dynamic_tables`` SQL exposed by
        :func:`decl_api.state_queries` so the offline DT DDL can later be
        threaded into ``fetch_applied_state``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        mock_decl.state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_dynamic_tables": (
                "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
            ),
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        executed = _executed_sqls(mock_execute_query)
        assert any(
            "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA" in s for s in executed
        ), (
            "plan() must execute the show_dynamic_tables SQL from "
            "decl_api.state_queries so the offline DT DDL is fetched"
        )

    def test_plan_threads_dt_text_map_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """The DT DDL rows MUST be normalised into a ``{name: text}`` dict
        and passed to ``fetch_applied_state(..., dt_text_map=...)`` so the
        BatchFV source-table binding can be recovered."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        mock_decl.state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_dynamic_tables": (
                "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
            ),
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }

        dt_row = {
            "name": "MY_BATCH_FV$V1",
            "text": (
                "CREATE DYNAMIC TABLE MY_BATCH_FV$V1 TARGET_LAG = '1 minute' "
                "AS SELECT * FROM TEST_DB.TEST_SCHEMA.RAW_EVENTS"
            ),
        }

        def fake_execute_query(sql, *args, **kwargs):
            if "SHOW DYNAMIC TABLES" in str(sql):
                return iter([dt_row])
            return iter([])

        mock_execute_query.side_effect = fake_execute_query

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        dt_text_map = call.kwargs.get("dt_text_map")
        assert isinstance(dt_text_map, dict) and dt_text_map, (
            "fetch_applied_state must receive a non-empty dt_text_map "
            "keyed by DT name; got %r" % dt_text_map
        )
        assert "MY_BATCH_FV$V1" in dt_text_map
        assert "RAW_EVENTS" in dt_text_map["MY_BATCH_FV$V1"]

    def test_plan_threads_datasources_by_table_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``plan`` MUST build a ``datasources_by_table`` lookup from the
        locally-loaded ``BatchSource`` specs and pass it to
        :func:`decl_api.fetch_applied_state` — without this the decl-side
        BatchFV source-name recovery falls back to using the recovered
        physical table name as ``sources[0].name``, breaking
        ``MISSING_SOURCE`` validation on every re-plan after a
        ``snow feature init`` round-trip.

        Plan ref: .cursor/plans/bfv_source-name_recovery_82070393.plan.md
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        mock_decl.state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_dynamic_tables": (
                "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
            ),
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }
        sentinel_lookup = {"RAW_EVENTS_FG_DECL": "EVENTS_FG_DECL"}
        mock_decl.build_datasources_by_table.return_value = sentinel_lookup

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        # The helper must be invoked on the LOCAL batch (the loader
        # output) so the lookup is built from the on-disk YAMLs the
        # operator authored, then threaded into the runtime-state
        # reconstruction.
        mock_decl.build_datasources_by_table.assert_called_once()
        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        forwarded = call.kwargs.get("datasources_by_table")
        assert forwarded is sentinel_lookup, (
            "plan() must thread the datasources_by_table lookup built "
            "from the local BatchSource specs into fetch_applied_state "
            f"so BFV source-name recovery prefers logical names; got {forwarded!r}"
        )

    def test_write_plan_threads_dt_text_map_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``write_plan`` MUST also feed the DT DDL into
        ``fetch_applied_state`` — both code paths share the BatchFV
        source-recovery requirement."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        mock_decl.state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_dynamic_tables": (
                "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
            ),
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }
        dt_row = {
            "name": "MY_BATCH_FV$V1",
            "text": (
                "CREATE DYNAMIC TABLE MY_BATCH_FV$V1 TARGET_LAG = '1 minute' "
                "AS SELECT * FROM TEST_DB.TEST_SCHEMA.RAW_EVENTS"
            ),
        }

        def fake_execute_query(sql, *args, **kwargs):
            if "SHOW DYNAMIC TABLES" in str(sql):
                return iter([dt_row])
            return iter([])

        mock_execute_query.side_effect = fake_execute_query

        FeatureManager().write_plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            out_path=str(tmp_path / "plan.json"),
        )

        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        dt_text_map = call.kwargs.get("dt_text_map")
        assert isinstance(dt_text_map, dict) and dt_text_map
        assert "MY_BATCH_FV$V1" in dt_text_map

    def test_write_plan_threads_datasources_by_table_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Mirror of
        ``test_plan_threads_datasources_by_table_into_fetch_applied_state``
        for ``write_plan`` — both code paths share the BatchFV
        source-name recovery requirement."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        mock_decl.state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_dynamic_tables": (
                "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
            ),
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }
        sentinel_lookup = {"RAW_EVENTS_FG_DECL": "EVENTS_FG_DECL"}
        mock_decl.build_datasources_by_table.return_value = sentinel_lookup

        FeatureManager().write_plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            out_path=str(tmp_path / "plan.json"),
        )

        mock_decl.build_datasources_by_table.assert_called_once()
        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        forwarded = call.kwargs.get("datasources_by_table")
        assert forwarded is sentinel_lookup, (
            "write_plan() must thread the datasources_by_table lookup "
            "built from the local BatchSource specs into "
            f"fetch_applied_state; got {forwarded!r}"
        )


# ===========================================================================
# init export — applied-state unification (Bug A wide-scope fix).
#
# ``snow feature init`` previously fed the raw ``DESCRIBE … TYPE =
# SPECIFICATION`` JSON straight through ``decl_api.export_specs``.
# That JSON always returns ``spec.sources: []`` for BatchFeatureView
# (snowml-core's FROM SPECIFICATION serializer encodes the source
# binding into the offline Dynamic Table's ``SELECT … FROM …`` body,
# not the spec payload).  The exported YAML therefore drifted from
# the deployed runtime and a follow-up ``snow feature plan`` spuriously
# emitted ``RECREATE_FV`` — apply then crashed with "no resolvable
# source".  The fix routes the init export through the same
# ``fetch_applied_state`` path the plan / write_plan codepaths use
# (so BatchFV ``sources``, advanced BFV fields, offline-only BFVs,
# and FG source mappings all round-trip cleanly).  See
# .cursor/plans/init_export_applied-state_unification_*.plan.md.
# ===========================================================================


class TestFeatureManagerInitDtTextRecovery:
    """``init`` MUST mirror ``plan`` / ``write_plan`` on the
    applied-state surface: fetch ``SHOW DYNAMIC TABLES``, build the
    ``dt_text_map`` + ``feature_view_rows`` bundle, hand them to
    :func:`decl_api.fetch_applied_state`, and forward the recovered
    state to :func:`decl_api.export_specs` via the new
    ``applied_state=`` kwarg.

    Each test below pins one rung of that contract — together they
    are the RED gate for the wide-scope BatchFV export fix.
    """

    def _patch_feature_store(self):
        return (
            mock.patch("snowflake.ml.feature_store.feature_store.FeatureStore"),
            mock.patch("snowflake.ml.feature_store.feature_store.CreationMode"),
        )

    def _set_state_queries(self, mock_decl):
        """Wire the ``state_queries`` shape ``plan`` / ``init`` both
        consume (must include ``show_dynamic_tables`` so the DT-text
        recovery rung is exercisable end-to-end)."""
        mock_decl.state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_dynamic_tables": (
                "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
            ),
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }

    def test_init_executes_show_dynamic_tables_query(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``init`` MUST run the ``show_dynamic_tables`` SQL exposed by
        :func:`decl_api.state_queries` — same as ``plan`` — so the
        offline DT DDL is available for BatchFV source recovery during
        the export pass.

        Pre-fix init called :func:`decl_api.export_queries` (which has
        no DT-text query) and never issued this SQL.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._set_state_queries(mock_decl)

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        executed = _executed_sqls(mock_execute_query)
        assert any(
            "SHOW DYNAMIC TABLES IN SCHEMA TEST_DB.TEST_SCHEMA" in s for s in executed
        ), (
            "init() must execute the show_dynamic_tables SQL from "
            "decl_api.state_queries so the offline DT DDL is fetched "
            "for BatchFV source recovery during the export pass; "
            f"executed SQLs: {executed!r}"
        )

    def test_init_threads_dt_text_map_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``init`` MUST call :func:`decl_api.fetch_applied_state`
        with the ``dt_text_map`` recovered from ``SHOW DYNAMIC TABLES``
        — same shape as the plan-path contract pinned in
        ``test_plan_threads_dt_text_map_into_fetch_applied_state``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._set_state_queries(mock_decl)

        dt_row = {
            "name": "USER_CLICKS_FG_DECL$V1",
            "text": (
                "CREATE DYNAMIC TABLE USER_CLICKS_FG_DECL$V1 TARGET_LAG = '300 seconds' "
                "AS SELECT * FROM JKEW_DB.JKEW_SCHEMA.RAW_EVENTS"
            ),
        }

        def fake_execute_query(sql, *args, **kwargs):
            if "SHOW DYNAMIC TABLES" in str(sql):
                return iter([dt_row])
            return iter([])

        mock_execute_query.side_effect = fake_execute_query

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        dt_text_map = call.kwargs.get("dt_text_map")
        assert isinstance(dt_text_map, dict) and dt_text_map, (
            "init() must thread a non-empty dt_text_map into "
            "fetch_applied_state so the BatchFV source binding is "
            f"recovered before the exporter runs; got {dt_text_map!r}"
        )
        assert "USER_CLICKS_FG_DECL$V1" in dt_text_map
        assert "RAW_EVENTS" in dt_text_map["USER_CLICKS_FG_DECL$V1"]

    def test_init_threads_datasources_by_table_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``init``'s export pass MUST also thread the locally-loaded
        ``datasources_by_table`` lookup into
        :func:`decl_api.fetch_applied_state` so the exported FV YAMLs
        carry the operator-authored logical source names rather than
        the recovered physical table names.

        On a fresh ``init`` (no local datasources yet) the helper
        returns an empty map; the lookup is still threaded so the
        contract stays uniform across plan / write_plan / init.

        Plan ref: .cursor/plans/bfv_source-name_recovery_82070393.plan.md
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._set_state_queries(mock_decl)
        sentinel_lookup = {"RAW_EVENTS_FG_DECL": "EVENTS_FG_DECL"}
        mock_decl.build_datasources_by_table.return_value = sentinel_lookup

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        # The init export pass must invoke the helper at least once
        # so the lookup is built from whatever datasources happen to
        # exist on disk at init time (zero on a brand-new project,
        # populated on a re-init that already has YAMLs).  Pinning
        # the helper call here ensures the manager wires the helper
        # in identically across plan / write_plan / init.
        assert mock_decl.build_datasources_by_table.called, (
            "init() must call decl_api.build_datasources_by_table so "
            "the lookup is built from the (possibly empty) local "
            "BatchSource specs before applied-state recovery runs"
        )
        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        forwarded = call.kwargs.get("datasources_by_table")
        assert forwarded is sentinel_lookup, (
            "init() must thread the datasources_by_table lookup built "
            "by build_datasources_by_table into fetch_applied_state; "
            f"got {forwarded!r}"
        )

    def test_init_passes_applied_state_to_export_specs(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """The recovered :class:`AppliedState` MUST be forwarded to
        :func:`decl_api.export_specs` via the ``applied_state=`` kwarg
        so the exporter prefers the recovered BatchFV ``sources`` over
        the lossy raw ``specification_map``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._set_state_queries(mock_decl)
        sentinel_state = mock.MagicMock(name="recovered_applied_state")
        mock_decl.fetch_applied_state.return_value = sentinel_state

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        mock_decl.export_specs.assert_called_once()
        kwargs = mock_decl.export_specs.call_args.kwargs
        assert kwargs.get("applied_state") is sentinel_state, (
            "init() must forward the applied_state returned by "
            "fetch_applied_state into export_specs via the "
            f"applied_state= kwarg; got kwargs={list(kwargs.keys())!r}"
        )

    def test_init_fetches_feature_view_rows_for_offline_only_recovery(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path
    ):
        """``init`` MUST surface offline-only BatchFVs (visible only via
        ``list_feature_views``, not ``SHOW ONLINE FEATURE TABLES``) by
        forwarding ``feature_view_rows`` into
        :func:`decl_api.fetch_applied_state`.

        Without this, ``init`` re-exports only the OFT-visible subset
        and offline-only BFVs silently disappear from the local tree —
        a regression of the offline-BFV idempotency contract pinned in
        ``test_replan_offline_bfv_idempotency.py``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._set_state_queries(mock_decl)
        fv_rows = [
            {
                "name": "OFFLINE_BFV",
                "version": "V1",
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
                "kind": "BATCH",
                "entities": ["USER_ID"],
                "physical_dt_name": "OFFLINE_BFV$V1",
                "refresh_freq": "60 seconds",
            }
        ]
        mock_decl.fetch_feature_view_rows.return_value = fv_rows

        fs_patch, cm_patch = self._patch_feature_store()
        with fs_patch, cm_patch as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(project_root=tmp_path)

        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        forwarded = call.kwargs.get("feature_view_rows")
        assert forwarded == fv_rows, (
            "init() must forward the imperative-side feature_view_rows "
            "into fetch_applied_state so offline-only BFVs surface in "
            f"the exported tree; got {forwarded!r}"
        )


# ===========================================================================
# write_plan — relocated to <project_root>/out/plan/
# ===========================================================================


class TestWritePlan:
    """``write_plan`` persists a plan JSON under
    ``<project_root>/out/plan/feature_plan_<UTC ts>.json`` (D8 relocated)
    with ``target_name`` round-tripped (D4-ext)."""

    def test_write_plan_default_path_under_out_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """When ``out_path`` is omitted, the plan lands under
        ``<project_root>/out/plan/feature_plan_<ts>.json`` (D8)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        result_path = FeatureManager().write_plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            out_path=None,
        )

        result = Path(result_path)
        # Plan must land under <project_root>/out/plan/, not under
        # <cwd>/.snowflake/plans/ (D1 hard-break).
        assert result.parent == (tmp_path / "out" / "plan").resolve()
        assert result.name.startswith("feature_plan_")
        assert result.name.endswith(".json")
        assert result.exists()

    def test_write_plan_explicit_out_path_honoured(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        out = tmp_path / "custom" / "plan.json"
        result_path = FeatureManager().write_plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            out_path=str(out),
        )
        assert result_path == str(out)
        assert out.exists()

    def test_write_plan_writes_target_name_into_envelope(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """D4-ext: serialise_plan must receive ``target_name`` so apply
        can later reject mismatched plans."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().write_plan(
            from_dir=tmp_path,
            target_name="DEFAULT",
            variables=[],
            dev_mode=False,
            out_path=None,
        )

        mock_decl.serialize_plan.assert_called_once()
        kwargs = mock_decl.serialize_plan.call_args.kwargs
        # Either as kwarg or positional after the canonical (plan, db,
        # schema, source_files) positional args.
        if "target_name" in kwargs:
            assert kwargs["target_name"] == "DEFAULT"
        else:
            args = mock_decl.serialize_plan.call_args.args
            # serialize_plan(plan, db, schema, source_files, target_name)
            assert len(args) >= 5
            assert args[4] == "DEFAULT"


# ===========================================================================
# apply — L1–L7 plan-file lifecycle, relocated to out/plan/
# ===========================================================================


class TestApplyCommand:
    """The L1–L7 invariants are PRESERVED, only the directory moves
    from ``<cwd>/.snowflake/plans/`` → ``<project_root>/out/plan/``."""

    def _wire_plan_file(self, mock_decl, *, target_name="DEFAULT"):
        """Make ``deserialize_plan`` return a usable PlanFile mock."""
        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"
        plan_file_obj.target_name = target_name
        return plan_file_obj

    # --- L1: Required-Plan ---

    def test_apply_returns_no_plan_when_out_plan_empty(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L1: no unapplied plan under ``<project_root>/out/plan/`` →
        ``status='no_plan'`` whose error names ``out/plan/`` (D8)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "no_plan"
        joined = " ".join(result.get("errors", []))
        # Error must point operators at out/plan/, not at .snowflake/plans/.
        assert "out/plan" in joined or "out/plan/" in joined
        mock_decl.execute_plan.assert_not_called()

    # --- L2: Latest-Wins ---

    def test_apply_picks_newest_unapplied_plan_under_out_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L2: lex sort by filename (UTC ts is monotonic at 1s)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        older = plans / "feature_plan_20260101T000000.json"
        newer = plans / "feature_plan_20260102T000000.json"
        older.write_text(_make_plan_json(target_database="OLDER_DB"))
        newer.write_text(_make_plan_json(target_database="TEST_DB"))

        self._wire_plan_file(mock_decl)
        FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )

        passed_json = mock_decl.deserialize_plan.call_args.args[0]
        assert "TEST_DB" in passed_json
        assert "OLDER_DB" not in passed_json

    # --- L3: Discard-Older ---

    def test_apply_renames_older_plans_to_discarded(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L3: older plans renamed ``.discarded`` before execution."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        older = plans / "feature_plan_20260101T000000.json"
        newer = plans / "feature_plan_20260102T000000.json"
        older.write_text(_make_plan_json())
        newer.write_text(_make_plan_json())

        self._wire_plan_file(mock_decl)
        FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )

        assert not older.exists()
        assert (plans / (older.name + ".discarded")).exists()

    # --- L4: Mark-Applied ---

    def test_apply_renames_plan_to_applied_after_success(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L4: success → ``<name>.applied``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        plan_path = plans / "feature_plan_20260507T120000.json"
        plan_path.write_text(_make_plan_json())

        self._wire_plan_file(mock_decl)
        FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )

        applied = plans / (plan_path.name + ".applied")
        assert applied.exists()
        assert not plan_path.exists()

    # --- L5: Mark-Failed-Stays-Unapplied ---

    def test_apply_leaves_plan_unrenamed_on_execution_failure(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L5: ``execute_plan`` raises → plan file stays at original name."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        plan_path = plans / "feature_plan_20260507T120000.json"
        plan_path.write_text(_make_plan_json())

        self._wire_plan_file(mock_decl)
        mock_decl.execute_plan.side_effect = RuntimeError("boom")

        try:
            FeatureManager().apply(
                from_dir=tmp_path,
                target_name=None,
                plan_file=None,
                dev_mode=False,
                allow_recreate=False,
            )
        except RuntimeError:
            pass
        finally:
            mock_decl.execute_plan.side_effect = None

        assert plan_path.exists()
        assert not (plans / (plan_path.name + ".applied")).exists()

    def test_apply_destructive_plan_without_allow_recreate_returns_refused(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``execute_plan`` returns ``refused`` → manager threads it
        through; plan file stays unrenamed."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        plan_path = plans / "feature_plan_20260507T120000.json"
        plan_path.write_text(_make_plan_json())

        self._wire_plan_file(mock_decl)
        refused = mock.MagicMock()
        refused.status = "refused"
        refused.ops = [{"operation": "RECREATE_FV", "name": "X", "status": "refused"}]
        refused.warnings = []
        refused.errors = ["Apply refused: --allow-recreate required."]
        mock_decl.execute_plan.return_value = refused

        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "refused"
        assert plan_path.exists()
        assert not (plans / (plan_path.name + ".applied")).exists()
        assert "--allow-recreate" in " ".join(result["errors"])

    # --- L6: Target-Match (account + target_name) ---

    def test_apply_account_mismatch_returns_target_mismatch_status(
        self,
        mock_execute_query,
        mock_decl,
        mock_account_identifier,
        tmp_path,
    ):
        """L6: connection account ≠ manifest target.account_identifier →
        ``status='target_mismatch'`` (NOT a CliError — apply surfaces a
        structured status so scripts can branch on it)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.identifiers import AccountIdentifier

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        (plans / "feature_plan_20260507T120000.json").write_text(_make_plan_json())
        self._wire_plan_file(mock_decl)

        mock_account_identifier.return_value = AccountIdentifier(
            "OTHER_ORG", "OTHER_ACCT"
        )

        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "target_mismatch"
        joined = " ".join(result.get("errors", []))
        assert "OTHER_ORG" in joined or "OTHER_ACCT" in joined
        mock_decl.execute_plan.assert_not_called()

    def test_apply_target_name_mismatch_returns_target_mismatch_status(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L6 / D4-ext: ``--target X`` against a plan with
        ``target_name=Y`` → ``status='target_mismatch'``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        # Manifest with two targets so we can request DEV but apply a
        # plan generated for PROD.
        _write_manifest(
            tmp_path,
            yaml_text=textwrap.dedent(
                """\
                manifest_version: 1
                type: feature_store
                default_target: DEV
                targets:
                  DEV:
                    account_identifier: TEST_ORG-TEST_ACCT
                    database: TEST_DB
                    schema: TEST_SCHEMA
                  PROD:
                    account_identifier: TEST_ORG-TEST_ACCT
                    database: TEST_DB
                    schema: TEST_SCHEMA
                """
            ),
        )
        plan_path = tmp_path / "prod_plan.json"
        plan_path.write_text(_make_plan_json(target_name="PROD"))
        self._wire_plan_file(mock_decl, target_name="PROD")

        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name="DEV",
            plan_file=str(plan_path),
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "target_mismatch"
        joined = " ".join(result.get("errors", []))
        assert "PROD" in joined
        assert "DEV" in joined
        mock_decl.execute_plan.assert_not_called()

    def test_apply_target_name_match_is_case_insensitive(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``plan.target_name.upper() == requested_target.upper()`` passes."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(_make_plan_json(target_name="default"))
        self._wire_plan_file(mock_decl, target_name="default")

        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name="DEFAULT",
            plan_file=str(plan_path),
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["status"] == "applied"

    # --- L7: --plan escape hatch ---

    def test_apply_plan_file_with_no_target_kwarg_works_for_legacy_plans(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L7: a plan with empty ``target_name`` (legacy / pre-D4-ext)
        applies cleanly without ``--target``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plan_path = tmp_path / "legacy.json"
        plan_path.write_text(_make_plan_json(target_name=""))
        self._wire_plan_file(mock_decl, target_name="")

        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=str(plan_path),
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["status"] == "applied"

    def test_apply_forwards_warehouse_from_connection_to_execute_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """D2 / Bug C: ``warehouse`` always comes from the active connection."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        (plans / "feature_plan_20260507T120000.json").write_text(_make_plan_json())
        self._wire_plan_file(mock_decl)

        FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )

        call_kwargs = mock_decl.execute_plan.call_args.kwargs
        assert call_kwargs.get("warehouse") == "TEST_WH"

    def test_apply_with_allow_recreate_sets_overwrite_in_options(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Bug G: when ``--allow-recreate`` is passed, ``PlanOptions.overwrite``
        must also be ``True`` so that ``CREATE_FV`` ops forward
        ``overwrite=True`` to ``register_feature_view`` and do not fail
        with a version-conflict error when the FV already exists."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans = _make_plans_dir(tmp_path)
        (plans / "feature_plan_20260507T120000.json").write_text(_make_plan_json())
        self._wire_plan_file(mock_decl)

        FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=True,
        )

        options = mock_decl.execute_plan.call_args.kwargs.get("options")
        assert options is not None, "execute_plan must receive an options= kwarg"
        assert options.allow_recreate is True
        assert options.overwrite is True, (
            "PlanOptions.overwrite must be True when allow_recreate=True so "
            "CREATE_FV ops can succeed against existing FV versions (Bug G)"
        )


# ===========================================================================
# list_specs / describe / export_specs — every Snowflake-bound command
# resolves the manifest first.
# ===========================================================================


class TestFeatureManagerListSpecs:
    def test_list_specs_returns_dict(self, mock_execute_query, mock_decl, tmp_path):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        result = FeatureManager().list_specs(from_dir=tmp_path, target_name=None)
        assert isinstance(result, dict)

    def test_list_specs_calls_list_state_queries(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().list_specs(from_dir=tmp_path, target_name=None)
        mock_decl.list_state_queries.assert_called_once_with("TEST_DB", "TEST_SCHEMA")

    def test_no_alter_session_priming_is_issued(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Pins the post-May-22 architecture:
        ``ENABLE_FEATURE_STORE_DESCRIBE_OFT_SPECIFICATION`` is enabled
        by default at the account level, so the declarative client
        MUST NOT issue an ``ALTER SESSION`` on any read path.  See
        ``declarative_feature_store/ARCHITECTURE.md`` ("DESCRIBE …
        TYPE = SPECIFICATION is enabled by default at the account
        level — the declarative client no longer issues an ALTER
        SESSION to flip the flag") and
        ``snowml/snowflake/ml/feature_store/decl/DESIGN.md`` ("no
        client-side session priming is issued").
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().list_specs(from_dir=tmp_path, target_name=None)
        FeatureManager().describe(from_dir=tmp_path, target_name=None, name="X")

        for sql in _executed_sqls(mock_execute_query):
            assert "ALTER SESSION" not in sql.upper(), (
                "decl client must not issue ALTER SESSION; "
                "ENABLE_FEATURE_STORE_DESCRIBE_OFT_SPECIFICATION is "
                f"account-default. got: {sql}"
            )

    def test_list_specs_missing_manifest_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError):
            FeatureManager().list_specs(from_dir=tmp_path, target_name=None)

    def test_list_specs_target_info_uses_manifest_db_schema(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(
            tmp_path,
            yaml_text=textwrap.dedent(
                """\
                manifest_version: 1
                type: feature_store
                default_target: DEFAULT
                targets:
                  DEFAULT:
                    account_identifier: TEST_ORG-TEST_ACCT
                    database: MFST_DB
                    schema: MFST_SCH
                """
            ),
        )
        result = FeatureManager().list_specs(from_dir=tmp_path, target_name=None)
        assert result.get("target_database") == "MFST_DB"
        assert result.get("target_schema") == "MFST_SCH"


class TestFeatureManagerDescribe:
    def test_describe_returns_dict_when_oft_not_found(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        result = FeatureManager().describe(
            from_dir=tmp_path, target_name=None, name="MY_ENTITY"
        )
        assert isinstance(result, dict)

    def test_describe_missing_manifest_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError):
            FeatureManager().describe(from_dir=tmp_path, target_name=None, name="X")


class TestFeatureManagerExportSpecsRemoved:
    """The public ``FeatureManager.export_specs`` is gone (init subsumes it).

    The export pipeline is still reachable, but only as a private
    helper invoked from :meth:`FeatureManager.init` — operators no
    longer call ``export`` directly.
    """

    def test_export_specs_method_no_longer_present(self):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        assert not hasattr(FeatureManager, "export_specs"), (
            "FeatureManager.export_specs must be removed; the export "
            "pipeline now runs only as part of FeatureManager.init"
        )


# ===========================================================================
# Surface deletions — D1 hard-break
# ===========================================================================


class TestSurfaceDeletions:
    """The legacy CLI surface (positional ``INPUT_FILES``,
    ``--config``, ``--overwrite``, ``./...``, ``_is_full_sync``,
    ``_expand_with_datasources``) is gone (D1)."""

    def test_expand_with_datasources_no_longer_present(self):
        from snowflake.cli._plugins.feature import manager

        assert not hasattr(manager.FeatureManager, "_expand_with_datasources")

    def test_is_full_sync_no_longer_present_in_commands(self):
        from snowflake.cli._plugins.feature import commands

        assert not hasattr(commands, "_is_full_sync")

    def test_input_files_arg_no_longer_present_on_apply(self):
        """``snow feature apply`` no longer accepts a positional
        ``INPUT_FILES`` argument."""
        import inspect

        from snowflake.cli._plugins.feature import commands

        sig = inspect.signature(commands.apply)
        assert "input_files" not in sig.parameters

    def test_config_flag_no_longer_present_on_plan(self):
        """``snow feature plan`` no longer accepts ``--config`` (D5)."""
        import inspect

        from snowflake.cli._plugins.feature import commands

        sig = inspect.signature(commands.plan)
        assert "config" not in sig.parameters

    def test_overwrite_flag_no_longer_present_on_apply(self):
        """``snow feature apply`` no longer accepts ``--overwrite`` (D1)."""
        import inspect

        from snowflake.cli._plugins.feature import commands

        sig = inspect.signature(commands.apply)
        assert "overwrite" not in sig.parameters


# ===========================================================================
# Manager surface invariants — boundary rule + warehouse-from-connection.
# ===========================================================================


class TestManagerBoundary:
    """Architecture boundary: ``manager.py`` MUST NOT contain SQL strings
    (Acceptance #6).  ``warehouse`` MUST come from the connection
    (Acceptance #5)."""

    def test_manager_source_has_no_sql_keywords_in_code(self):
        """Acceptance #6: only docstring text may contain SQL keywords."""
        import inspect
        import re

        from snowflake.cli._plugins.feature import manager

        source = inspect.getsource(manager)
        # Strip docstrings / comments by walking the file as text and
        # collapsing every triple-quoted block.
        no_docstrings = re.sub(r'"""[\s\S]*?"""', "", source, flags=re.MULTILINE)
        no_docstrings = re.sub(r"'''[\s\S]*?'''", "", no_docstrings, flags=re.MULTILINE)
        # Drop ``# ...`` comments line-by-line so they don't trip the grep.
        no_comments = "\n".join(
            line.split("#", 1)[0] for line in no_docstrings.splitlines()
        )
        for kw in ("ALTER ", "SHOW ", "CREATE ", "DROP ", "DESCRIBE ", "SELECT "):
            assert kw not in no_comments, (
                f"manager.py code contains SQL keyword {kw!r} outside "
                f"docstrings/comments — boundary rule violated"
            )


# ===========================================================================
# Ingest / Query — preserved snowml-core delegation contract.
# Phase 3+4 changes the surface (from_dir / target_name kwargs added),
# but the underlying snowml-core delegation is unchanged.
# ===========================================================================


class TestFeatureManagerIngest:
    """``ingest`` delegates to ``FeatureStore.stream_ingest`` after the
    client-side schema preflight."""

    @staticmethod
    def _stream_source_with_schema(field_names):
        fields = []
        for fname in field_names:
            f = mock.MagicMock(name=f"field_{fname}")
            f.name = fname
            fields.append(f)
        schema = mock.MagicMock(name="schema")
        schema.fields = fields
        src = mock.MagicMock(name="stream_source")
        src.schema = schema
        return src

    def _patch_fs(self, accepted=1):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_fs = mock.MagicMock(name="feature_store")
        mock_fs.stream_ingest.return_value = accepted
        mock_fs.get_stream_source.return_value = self._stream_source_with_schema(
            ["col_a"]
        )
        return (
            mock.patch.object(
                FeatureManager,
                "_get_feature_store",
                create=True,
                return_value=mock_fs,
            ),
            mock_fs,
        )

    def test_ingest_calls_fs_stream_ingest(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs(accepted=3)

        with patcher:
            FeatureManager().ingest(
                from_dir=tmp_path,
                target_name=None,
                source_name="MY_STREAM",
                records=[{"col_a": 1}],
            )

        mock_fs.stream_ingest.assert_called_once_with("MY_STREAM", [{"col_a": 1}])

    def test_ingest_returns_accepted_count_envelope(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, _ = self._patch_fs(accepted=100)

        with patcher:
            result = FeatureManager().ingest(
                from_dir=tmp_path,
                target_name=None,
                source_name="MY_STREAM",
                records=[{"col_a": 1}],
            )

        assert result["accepted_count"] == 100
        assert result["target_database"] == "TEST_DB"
        assert result["target_schema"] == "TEST_SCHEMA"
        assert result["target_warehouse"] == "TEST_WH"

    def test_ingest_preflight_rejects_records_missing_required_fields(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs()
        mock_fs.get_stream_source.return_value = self._stream_source_with_schema(
            ["USER_ID", "PAGE_URL"]
        )

        with patcher:
            with pytest.raises(ValueError) as exc:
                FeatureManager().ingest(
                    from_dir=tmp_path,
                    target_name=None,
                    source_name="X",
                    records=[{"USER_ID": "u1"}],
                )
        assert "PAGE_URL" in str(exc.value)
        mock_fs.stream_ingest.assert_not_called()


class TestFeatureManagerQuery:
    def _make_mock_fv(self, entities_join_keys):
        fv = mock.MagicMock(name="fv")
        fv.entities = []
        for jk in entities_join_keys:
            ent = mock.MagicMock(name="entity")
            ent.join_keys = jk
            fv.entities.append(ent)
        return fv

    def _patch_fs(self, join_keys_per_entity, rows_records=None):
        import pandas as pd
        from snowflake.cli._plugins.feature.manager import FeatureManager

        if rows_records is None:
            rows_records = [{"USER_ID": "u1"}]
        mock_fv = self._make_mock_fv(join_keys_per_entity)
        mock_fs = mock.MagicMock(name="fs")
        mock_fs.get_feature_view.return_value = mock_fv
        mock_fs.read_feature_view.return_value = pd.DataFrame(rows_records)
        return (
            mock.patch.object(
                FeatureManager,
                "_get_feature_store",
                create=True,
                return_value=mock_fs,
            ),
            mock_fs,
        )

    def test_query_passes_version_to_get_feature_view(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        patcher, mock_fs = self._patch_fs([["USER_ID"]])
        with patcher:
            FeatureManager().query(
                from_dir=tmp_path,
                target_name=None,
                feature_view_name="FV",
                version="V1",
                keys=[{"USER_ID": "u1"}],
            )
        mock_fs.get_feature_view.assert_called_once_with("FV", "V1")

    def test_query_returns_rows_envelope(self, mock_execute_query, mock_decl, tmp_path):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        rows = [{"USER_ID": "u1", "X": 1}]
        patcher, _ = self._patch_fs([["USER_ID"]], rows_records=rows)
        with patcher:
            result = FeatureManager().query(
                from_dir=tmp_path,
                target_name=None,
                feature_view_name="FV",
                version="V1",
                keys=[{"USER_ID": "u1"}],
            )
        assert result["rows"] == rows
        assert result["target_database"] == "TEST_DB"
        assert result["target_warehouse"] == "TEST_WH"

    def test_query_raises_clear_error_for_missing_join_key(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        patcher, mock_fs = self._patch_fs([["USER_ID"], ["SESSION_ID"]])
        with patcher:
            with pytest.raises(ValueError, match="SESSION_ID"):
                FeatureManager().query(
                    from_dir=tmp_path,
                    target_name=None,
                    feature_view_name="FV",
                    version="V1",
                    keys=[{"USER_ID": "u1"}],
                )
        mock_fs.read_feature_view.assert_not_called()


# ===========================================================================
# online-service — get_status / initialize_service / destroy_service
# ===========================================================================


_STAGING_MANIFEST_YAML = textwrap.dedent(
    """\
    manifest_version: 1
    type: feature_store
    default_target: STAGING
    targets:
      STAGING:
        account_identifier: TEST_ORG-TEST_ACCT
        database: STAGING_DB
        schema: STAGING_SCHEMA
        role: STAGING_ROLE
    """
)


class TestOnlineServiceManagerResolvesTarget:
    """``get_status`` / ``initialize_service`` / ``destroy_service`` now
    accept ``from_dir`` + ``target_name`` and route through
    :meth:`FeatureManager._resolve_project` so the manifest target's
    ``database`` / ``schema`` / ``role`` drives the
    ``SYSTEM$*_FEATURE_STORE_ONLINE_SERVICE(...)`` calls.

    When no manifest is reachable AND ``target_name`` is ``None`` the
    manager silently falls back to today's connection-only behaviour
    (operators still run ``snow feature online-service --drop`` from
    a non-project dir to tear down a runtime).  An explicit
    ``--target`` against a manifest-less directory is a hard error so
    operators don't accidentally point at the wrong location.
    """

    def test_get_status_uses_resolved_target_database_and_schema(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path, yaml_text=_STAGING_MANIFEST_YAML)

        FeatureManager().get_status(from_dir=tmp_path, target_name="STAGING")

        assert mock_decl.service_sql.called
        args = mock_decl.service_sql.call_args.args
        assert args[0] == "STAGING_DB"
        assert args[1] == "STAGING_SCHEMA"

    def test_get_status_no_manifest_falls_back_to_connection(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        FeatureManager().get_status(from_dir=tmp_path, target_name=None)

        args = mock_decl.service_sql.call_args.args
        assert args[0] == "TEST_DB"
        assert args[1] == "TEST_SCHEMA"

    def test_get_status_no_manifest_with_explicit_target_returns_error_envelope(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """No manifest + explicit ``--target`` is a hard mismatch; the
        wrapper catches the :class:`CliError` and surfaces it through
        the result envelope so the CLI still renders ``Status: error``
        (the spinner / poll loop relies on this convention)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        result = FeatureManager().get_status(from_dir=tmp_path, target_name="PROD")
        assert result.get("status") == "error"
        assert "manifest.yml" in result.get("error", "")

    def test_initialize_service_uses_resolved_target_role_as_producer(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path, yaml_text=_STAGING_MANIFEST_YAML)

        FeatureManager().initialize_service(
            from_dir=tmp_path,
            target_name="STAGING",
            producer_role=None,
            consumer_role=None,
        )

        # service_sql is invoked twice (once inside get_status, once for
        # the CREATE call).  Only the CREATE call carries roles.
        create_calls = [
            c for c in mock_decl.service_sql.call_args_list if len(c.args) >= 4
        ]
        assert create_calls, "service_sql with roles was never invoked"
        args = create_calls[-1].args
        assert args[0] == "STAGING_DB"
        assert args[1] == "STAGING_SCHEMA"
        assert args[2] == "STAGING_ROLE"
        assert args[3] == "PUBLIC"

    def test_initialize_service_explicit_producer_role_overrides_target_role(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path, yaml_text=_STAGING_MANIFEST_YAML)

        FeatureManager().initialize_service(
            from_dir=tmp_path,
            target_name="STAGING",
            producer_role="EXPLICIT_ROLE",
            consumer_role="CUSTOM_CONSUMER",
        )

        create_calls = [
            c for c in mock_decl.service_sql.call_args_list if len(c.args) >= 4
        ]
        args = create_calls[-1].args
        assert args[2] == "EXPLICIT_ROLE"
        assert args[3] == "CUSTOM_CONSUMER"

    def test_initialize_service_no_manifest_falls_back_to_connection(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        FeatureManager().initialize_service(
            from_dir=tmp_path,
            target_name=None,
            producer_role=None,
            consumer_role=None,
        )

        create_calls = [
            c for c in mock_decl.service_sql.call_args_list if len(c.args) >= 4
        ]
        args = create_calls[-1].args
        assert args[0] == "TEST_DB"
        assert args[1] == "TEST_SCHEMA"
        assert args[2] == "TEST_ROLE"
        assert args[3] == "PUBLIC"

    def test_destroy_service_uses_resolved_target_database_and_schema(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path, yaml_text=_STAGING_MANIFEST_YAML)

        FeatureManager().destroy_service(from_dir=tmp_path, target_name="STAGING")

        args = mock_decl.service_sql.call_args.args
        assert args[0] == "STAGING_DB"
        assert args[1] == "STAGING_SCHEMA"

    def test_destroy_service_no_manifest_falls_back_to_connection(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        FeatureManager().destroy_service(from_dir=tmp_path, target_name=None)

        args = mock_decl.service_sql.call_args.args
        assert args[0] == "TEST_DB"
        assert args[1] == "TEST_SCHEMA"

    def test_destroy_service_no_manifest_with_explicit_target_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError, match="manifest.yml"):
            FeatureManager().destroy_service(from_dir=tmp_path, target_name="PROD")


# ===========================================================================
# Wave 3B — stream-source applied-state read path
#
# ``FeatureManager._fetch_stream_source_rows`` is the CLI's seam onto
# ``decl_api.fetch_stream_source_rows`` (added in snowml commit
# 995a628d1).  It mirrors the ``_fetch_entity_rows`` / ``_fetch_feature_view_rows``
# / ``_fetch_feature_group_rows`` graceful-degrade contract: a
# ``FeatureStoreNotInitializedError`` propagates (so the CLI rewraps it
# with the actionable ``snow feature init`` message), every other
# exception swallowed to ``[]`` with a debug log so a missing privilege
# downgrades fidelity rather than crashing the bundle.
#
# The rows MUST then be threaded into ``decl_api.fetch_applied_state(...,
# stream_source_rows=...)`` from both the ``plan()`` and ``write_plan()``
# call sites so the planner sees runtime-authoritative ``Datasource``
# AppliedObjects and emits the correct ``CREATE_SOURCE`` /
# ``UPDATE_SOURCE`` / ``RECREATE_SOURCE`` / ``DROP_SOURCE`` /
# ``NO_CHANGE`` decision per ``plans/stream_source_contract.md`` §§7–8.
# ===========================================================================


def _make_target():
    """Return a vanilla ``FSTarget`` for the default test manifest."""
    from snowflake.ml.feature_store.decl.manifest import FSTarget

    return FSTarget(
        name="DEFAULT",
        account_identifier="TEST_ORG-TEST_ACCT",
        database="TEST_DB",
        schema="TEST_SCHEMA",
        role="TEST_ROLE",
    )


class TestFetchStreamSourceRowsThreading:
    """Wave 3B — ``_fetch_stream_source_rows`` + bundle threading."""

    # ------------------------------------------------------------------
    # _fetch_stream_source_rows direct unit tests
    # ------------------------------------------------------------------

    def test_fetch_stream_source_rows_calls_decl_api_facade(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``_fetch_stream_source_rows(target)`` delegates to
        ``decl_api.fetch_stream_source_rows(session, database, schema,
        warehouse)`` and returns the result verbatim."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        target = _make_target()
        rows = [
            {
                "name": "USER_CLICKS_STREAM",
                "schema": [{"name": "USER_ID", "type": "StringType"}],
                "desc": "click events",
                "owner": "OPS",
            }
        ]
        mock_decl.fetch_stream_source_rows.return_value = rows

        result = FeatureManager()._fetch_stream_source_rows(target)  # noqa: SLF001

        assert result == rows
        mock_decl.fetch_stream_source_rows.assert_called_once()
        args = mock_decl.fetch_stream_source_rows.call_args.args
        # (session, database, schema, warehouse) positional signature —
        # mirrors fetch_entity_rows / fetch_feature_view_rows /
        # fetch_feature_group_rows so the manager's helpers stay
        # uniform.  ``warehouse`` flows from the active connection
        # context (``ctx.connection.warehouse``), matching the other
        # _fetch_* helpers.
        assert args[1] == "TEST_DB"
        assert args[2] == "TEST_SCHEMA"
        assert args[3] == "TEST_WH"

    def test_fetch_stream_source_rows_graceful_degrades_on_generic_exception(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """Any non-init-required exception → ``[]`` (debug log).  A
        missing privilege downgrades fidelity rather than crashing the
        applied-state bundle."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        target = _make_target()
        mock_decl.fetch_stream_source_rows.side_effect = RuntimeError(
            "SQL access control error: Insufficient privileges"
        )

        result = FeatureManager()._fetch_stream_source_rows(target)  # noqa: SLF001

        assert result == []

    def test_fetch_stream_source_rows_reraises_not_initialized(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``FeatureStoreNotInitializedError`` is first-class — the CLI's
        command-layer wrapper rewraps it into the actionable ``snow
        feature init`` message, so this helper MUST propagate rather
        than swallow."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.errors import (
            FeatureStoreNotInitializedError,
        )

        target = _make_target()
        mock_decl.fetch_stream_source_rows.side_effect = (
            FeatureStoreNotInitializedError(
                "TEST_DB",
                "TEST_SCHEMA",
                RuntimeError("missing SNOWML_FEATURE_STORE_OBJECT tag"),
            )
        )

        with pytest.raises(FeatureStoreNotInitializedError):
            FeatureManager()._fetch_stream_source_rows(target)  # noqa: SLF001

    # ------------------------------------------------------------------
    # Bundle threading — ``plan()`` / ``write_plan()`` MUST forward the
    # rows into ``decl_api.fetch_applied_state(stream_source_rows=...)``.
    # ------------------------------------------------------------------

    def test_plan_threads_stream_source_rows_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``plan()`` MUST call ``_fetch_stream_source_rows`` and feed
        the rows into ``fetch_applied_state(..., stream_source_rows=...)``
        so the planner's source-diff branch sees runtime-authoritative
        ``Datasource`` AppliedObjects."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        rows = [
            {
                "name": "USER_CLICKS_STREAM",
                "schema": [{"name": "USER_ID", "type": "StringType"}],
                "desc": "click events",
                "owner": "OPS",
            }
        ]
        mock_decl.fetch_stream_source_rows.return_value = rows

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        assert call.kwargs.get("stream_source_rows") == rows

    def test_write_plan_threads_stream_source_rows_into_fetch_applied_state(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``write_plan()`` MUST also feed stream-source rows into
        ``fetch_applied_state`` — both code paths share the
        applied-state bundle contract."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        rows = [
            {
                "name": "USER_CLICKS_STREAM",
                "schema": [{"name": "USER_ID", "type": "StringType"}],
                "desc": "click events",
                "owner": "OPS",
            }
        ]
        mock_decl.fetch_stream_source_rows.return_value = rows

        FeatureManager().write_plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            out_path=str(tmp_path / "plan.json"),
        )

        mock_decl.fetch_applied_state.assert_called_once()
        call = mock_decl.fetch_applied_state.call_args
        assert call.kwargs.get("stream_source_rows") == rows
