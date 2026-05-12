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
        m.export_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "describe_template": 'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}"',
            "describe_specification_template": (
                'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}" '
                "TYPE = SPECIFICATION"
            ),
        }
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


_ALTER_SESSION_SQL = (
    "ALTER SESSION SET ENABLE_FEATURE_STORE_DESCRIBE_OFT_SPECIFICATION = TRUE"
)


def _executed_sqls(mock_execute_query):
    sqls = []
    for call in mock_execute_query.call_args_list:
        if call.args:
            sqls.append(str(call.args[0]))
    return sqls


def _wire_real_session_setup(mock_decl):
    """Make ``decl_api.ensure_session_setup`` actually invoke the executor."""

    def fake_ensure(execute_query):
        execute_query(_ALTER_SESSION_SQL)

    mock_decl.ensure_session_setup.side_effect = fake_ensure


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
    """``FeatureManager.init`` writes ``manifest.yml`` (D6 auto-derive),
    scaffolds ``sources/{entities,datasources,feature_views}/`` +
    ``out/plan/.gitkeep``, then calls ``FeatureStore(...,
    creation_mode=CREATE_IF_NOT_EXIST)`` for the Snowflake-side setup.
    """

    def test_init_writes_manifest_yml(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Happy path: a fresh ``init`` writes a parseable ``manifest.yml``."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        manifest_path = tmp_path / "manifest.yml"
        assert manifest_path.is_file()
        parsed = yaml.safe_load(manifest_path.read_text())
        assert parsed["manifest_version"] == 1
        assert parsed["type"] == "feature_store"
        assert parsed["default_target"] == "DEFAULT"
        assert "DEFAULT" in parsed["targets"]

    def test_init_populates_manifest_from_active_connection(
        self, mock_execute_query, mock_cli_context, mock_account_identifier, tmp_path
    ):
        """D6 auto-derive: db / schema / role come from the connection;
        ``account_identifier`` comes from
        :func:`get_account_identifier` (the canonical
        ``<ORG>-<ACCOUNT>`` form), NOT ``connection.account``.

        ``connection.account`` is the host-prefix used to log in
        (e.g. ``"feature_store_vnext2"``), which is not the canonical
        Snowflake account identifier and would fail every subsequent
        L6 ``get_account_identifier``-based mismatch check.
        """
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.identifiers import AccountIdentifier

        # Connection's bare ``account`` looks like a host prefix —
        # this is what ``connections.toml`` stores for most users.
        mock_cli_context.return_value.connection.account = "my_acct"
        mock_cli_context.return_value.connection.database = "MY_DB"
        mock_cli_context.return_value.connection.schema = "MY_SCHEMA"
        mock_cli_context.return_value.connection.role = "MY_ROLE"

        # ``get_account_identifier`` returns the canonical form by
        # querying ``CURRENT_ORGANIZATION_NAME / CURRENT_ACCOUNT_NAME``.
        mock_account_identifier.return_value = AccountIdentifier("MY_ORG", "MY_ACCT")

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        parsed = yaml.safe_load((tmp_path / "manifest.yml").read_text())
        target = parsed["targets"]["DEFAULT"]
        # Canonical org-account form, NOT the host-prefix
        # ``my_acct`` from ``connection.account``.
        assert target["account_identifier"] == "MY_ORG-MY_ACCT"
        assert target["database"] == "MY_DB"
        assert target["schema"] == "MY_SCHEMA"
        assert target["role"] == "MY_ROLE"

    def test_init_account_identifier_uses_canonical_org_account_form(
        self, mock_execute_query, mock_cli_context, mock_account_identifier, tmp_path
    ):
        """Regression for the live smoke-test bug: ``connection.account``
        is the host-prefix (``feature_store_vnext2``); the manifest
        MUST instead store the canonical form returned by
        :func:`get_account_identifier`
        (``SFENGINEERING-FEATURE_STORE_VNEXT2``) so the L6 account
        check at apply time matches.
        """
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.identifiers import AccountIdentifier

        mock_cli_context.return_value.connection.account = "feature_store_vnext2"
        mock_account_identifier.return_value = AccountIdentifier(
            "SFENGINEERING", "FEATURE_STORE_VNEXT2"
        )

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        parsed = yaml.safe_load((tmp_path / "manifest.yml").read_text())
        assert (
            parsed["targets"]["DEFAULT"]["account_identifier"]
            == "SFENGINEERING-FEATURE_STORE_VNEXT2"
        )

    def test_init_account_identifier_falls_back_when_query_fails(
        self, mock_execute_query, mock_cli_context, mock_account_identifier, tmp_path
    ):
        """If :func:`get_account_identifier` raises (e.g., a session
        issue), ``init`` must still produce a writable manifest using
        the connection's ``account`` field so the operator can
        manually correct the value rather than losing the scaffold.
        """
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_cli_context.return_value.connection.account = "fallback_acct"
        mock_account_identifier.side_effect = RuntimeError("simulated session failure")

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        parsed = yaml.safe_load((tmp_path / "manifest.yml").read_text())
        # Best-effort fallback to ``connection.account`` so the
        # scaffold lands; the operator can edit the manifest after.
        assert parsed["targets"]["DEFAULT"]["account_identifier"] == "fallback_acct"

    def test_init_does_not_write_warehouse_field(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """D2: ``warehouse`` MUST NOT appear in the generated manifest."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        text = (tmp_path / "manifest.yml").read_text()
        assert "warehouse" not in text.lower()

    def test_init_scaffolds_sources_subdirs(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """``sources/{entities,datasources,feature_views}/`` exist after init."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        for sub in ("entities", "datasources", "feature_views"):
            assert (
                tmp_path / "sources" / sub
            ).is_dir(), f"Expected sources/{sub}/ to exist after init scaffold"

    def test_init_writes_out_plan_gitkeep(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Acceptance #10: ``init`` writes ``out/plan/.gitkeep`` so the
        plan-discovery directory is git-trackable but doesn't pollute
        later plan-discovery."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        gitkeep = tmp_path / "out" / "plan" / ".gitkeep"
        assert gitkeep.is_file(), (
            "init must write out/plan/.gitkeep so the directory is "
            "tracked but not polluted with a stray plan file"
        )

    def test_init_calls_feature_store_with_create_if_not_exist(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Snowflake-side init runs ``FeatureStore(..., CREATE_IF_NOT_EXIST)``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ) as mock_fs_cls, mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        mock_fs_cls.assert_called_once()
        kwargs = mock_fs_cls.call_args[1]
        assert kwargs["creation_mode"] == mock_cm.CREATE_IF_NOT_EXIST

    def test_init_returns_status_initialized(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """init() returns ``{status, project_root, manifest_path, target}``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = FeatureManager().init(from_dir=tmp_path, no_scaffold=False)

        assert result["status"] == "initialized"
        assert Path(result["project_root"]) == tmp_path.resolve()
        assert Path(result["manifest_path"]) == (tmp_path / "manifest.yml").resolve()
        assert result["target"] == "DEFAULT"

    def test_init_fails_fast_when_manifest_exists(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """init-exist (locked decision): ``manifest.yml`` already there →
        ``CliError`` with the canonical message; NO ``--force`` escape."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        # Pre-existing manifest (contents irrelevant — even invalid blocks).
        (tmp_path / "manifest.yml").write_text("invalid: yaml: content:\n")

        with mock.patch("snowflake.ml.feature_store.feature_store.FeatureStore"):
            with pytest.raises(CliError) as excinfo:
                FeatureManager().init(from_dir=tmp_path, no_scaffold=False)
        msg = str(excinfo.value)
        assert "manifest.yml" in msg
        assert "already exists" in msg
        assert str(tmp_path.resolve()) in msg or "manifest.yml" in msg

    def test_init_no_scaffold_skips_manifest_and_dirs_and_feature_store(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """``--no-scaffold`` skips manifest write, dir scaffolding, AND
        the Snowflake-side FeatureStore init."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ) as mock_fs_cls:
            FeatureManager().init(from_dir=tmp_path, no_scaffold=True)

        assert not (tmp_path / "manifest.yml").exists()
        assert not (tmp_path / "sources").exists()
        assert not (tmp_path / "out").exists()
        mock_fs_cls.assert_not_called()


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

    def test_list_specs_runs_session_setup_before_state_queries(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        _wire_real_session_setup(mock_decl)
        FeatureManager().list_specs(from_dir=tmp_path, target_name=None)
        sqls = _executed_sqls(mock_execute_query)
        assert sqls and sqls[0] == _ALTER_SESSION_SQL

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

    def test_describe_runs_session_setup(self, mock_execute_query, mock_decl, tmp_path):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        _wire_real_session_setup(mock_decl)
        FeatureManager().describe(from_dir=tmp_path, target_name=None, name="MY_FV")
        sqls = _executed_sqls(mock_execute_query)
        assert sqls[0] == _ALTER_SESSION_SQL

    def test_describe_missing_manifest_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError):
            FeatureManager().describe(from_dir=tmp_path, target_name=None, name="X")


class TestFeatureManagerExportSpecs:
    """Strict full-fidelity SPECIFICATION flow, manifest-driven."""

    def _setup_show_specification(
        self, mock_execute_query, show_rows, specification_rows=None
    ):
        spec_rows = (
            specification_rows
            if specification_rows is not None
            else [{"specification": "{}"}]
        )

        def side_effect(query, **kwargs):
            if "SHOW ONLINE FEATURE TABLES" in query:
                return iter(show_rows)
            if "TYPE = SPECIFICATION" in query:
                return iter(list(spec_rows))
            return iter([])

        mock_execute_query.side_effect = side_effect

    def test_export_specs_returns_dict(self, mock_execute_query, mock_decl, tmp_path):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        self._setup_show_specification(mock_execute_query, show_rows=[])

        result = FeatureManager().export_specs(
            from_dir=tmp_path, target_name=None, output_dir=str(tmp_path / "out")
        )
        assert isinstance(result, dict)

    def test_export_specs_missing_manifest_raises_cli_error(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError):
            FeatureManager().export_specs(
                from_dir=tmp_path, target_name=None, output_dir="."
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
