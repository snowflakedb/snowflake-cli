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

"""Integration tests: ``FeatureManager`` wired to the real ``decl_api``.

The Snowflake connection (``execute_query`` / ``_build_session``) is
mocked; every call to the declarative library
(:mod:`snowflake.ml.feature_store.decl.api`) runs against the real
implementation installed from the ``snowflake_ml_feature_store_decl``
wheel.

The CLI surface is the Phase 3+4 manifest-driven shape:

* ``--from <dir>`` locates ``manifest.yml`` (default: cwd).
* ``--target <name>`` selects the target (default:
  ``manifest.default_target``).
* No positional spec arguments, no ``--config``, no ``--overwrite``.

Spec trees live under ``<project_root>/sources/{entities,
datasources, feature_views}/``; UDF Python source lives under a
non-canonical sub-directory the project loader does NOT walk
(``sources/udfs/`` here) so a bare ``pd.DataFrame`` annotation in
the UDF body cannot trip ``importlib`` during spec discovery.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest import mock

import pytest

from tests.feature.test_manager import (  # noqa: F401 (autouse fixtures)
    mock_account_identifier,
    mock_build_session,
    mock_cli_context,
)

# ---------------------------------------------------------------------------
# Manifest project helpers
# ---------------------------------------------------------------------------


_MANIFEST_YAML = textwrap.dedent(
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


_ENTITY_YAML = textwrap.dedent(
    """\
    kind: Entity
    name: user
    join_keys:
      - name: user_id
        type: StringType
    """
)


_FV_YAML = textwrap.dedent(
    """\
    kind: StreamingFeatureView
    name: user_clicks
    version: V1
    ordered_entity_column_names:
      - user_id
    sources: []
    features:
      - source_column:
          name: event
          type: StringType
        output_column:
          name: event
          type: StringType
    """
)


def _write_minimal_project(project_root: Path) -> Path:
    """Lay out a Phase 3+4 manifest project under *project_root*.

    Tree::

        <project_root>/
          manifest.yml
          sources/
            entities/user.yaml
            datasources/  (empty — keeps the loader happy)
            feature_views/user_clicks.yaml

    Returns:
        Path to *project_root* (the dir the manager resolves via
        ``_resolve_project``).
    """
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "manifest.yml").write_text(_MANIFEST_YAML)
    sources = project_root / "sources"
    (sources / "entities").mkdir(parents=True)
    (sources / "datasources").mkdir(parents=True)
    (sources / "feature_views").mkdir(parents=True)
    (sources / "entities" / "user.yaml").write_text(_ENTITY_YAML)
    (sources / "feature_views" / "user_clicks.yaml").write_text(_FV_YAML)
    return project_root


def _write_minimal_plan_json(
    path: Path,
    target_database: str = "TEST_DB",
    target_schema: str = "TEST_SCHEMA",
    target_name: str = "DEFAULT",
) -> None:
    """Write a minimal valid ``PlanFile`` JSON envelope for wet-run apply tests.

    Wet-run ``apply`` is a *pure plan-file consumer* (Phase 3+4 D1):
    it deserializes the on-disk envelope and hands it to
    ``decl_api.execute_plan``.  These integration tests exercise the
    real ``deserialize_plan`` against a minimally-valid envelope while
    mocking ``execute_plan`` to return the desired ``ApplyResult``.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": "1",
                "created_at": "2026-05-11T00:00:00+00:00",
                "target_database": target_database,
                "target_schema": target_schema,
                "target_name": target_name,
                "source_files": [],
                "plan": {"ops": [], "warnings": []},
                "summary": {},
            }
        )
    )


@pytest.fixture
def project_dir(tmp_path):
    """Provide a tmp_path containing a minimal manifest project."""
    return _write_minimal_project(tmp_path)


@pytest.fixture
def mock_execute_query():
    """Patch ``FeatureManager.execute_query`` so tests don't need a real connection."""
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


# ---------------------------------------------------------------------------
# manager.plan — read-only validate+plan path
# ---------------------------------------------------------------------------


class TestPlanIntegration:
    """End-to-end tests for ``manager.plan`` against the real decl
    library (with mocked ``execute_query``).
    """

    def test_plan_returns_status_ready(self, project_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        result = FeatureManager().plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["status"] == "ready"

    def test_plan_executes_no_ddl(self, project_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        result = FeatureManager().plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["executed"] == 0

    def test_plan_returns_ops_list(self, project_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        result = FeatureManager().plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )
        assert isinstance(result["ops"], list)
        assert result["executed"] == 0

    def test_plan_calls_show_queries(self, project_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        FeatureManager().plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )
        calls = [c.args[0] for c in mock_execute_query.call_args_list if c.args]
        assert any("SHOW" in str(sql).upper() for sql in calls)


# ---------------------------------------------------------------------------
# apply — wet-run via plan-file consumer surface
# ---------------------------------------------------------------------------


class TestApplyExecuteIntegration:
    def test_apply_returns_applied_status(self, project_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.types import ApplyResult

        plan_path = project_dir / "out" / "plan" / "feature_plan.json"
        _write_minimal_plan_json(plan_path)
        mock_result = ApplyResult(
            status="applied",
            ops=[
                {"operation": "CREATE_FV", "name": "user_clicks", "status": "success"}
            ],
        )

        with mock.patch(
            "snowflake.ml.feature_store.decl.api.execute_plan",
            return_value=mock_result,
        ):
            result = FeatureManager().apply(
                from_dir=project_dir,
                target_name=None,
                plan_file=str(plan_path),
                dev_mode=False,
                allow_recreate=False,
            )
        assert result["status"] == "applied"

    def test_apply_result_has_ops_and_executed_keys(
        self, project_dir, mock_execute_query
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.types import ApplyResult

        plan_path = project_dir / "out" / "plan" / "feature_plan.json"
        _write_minimal_plan_json(plan_path)
        mock_result = ApplyResult(
            status="applied",
            ops=[
                {"operation": "CREATE_ENTITY", "name": "user", "status": "success"},
                {"operation": "CREATE_FV", "name": "user_clicks", "status": "success"},
            ],
        )

        with mock.patch(
            "snowflake.ml.feature_store.decl.api.execute_plan",
            return_value=mock_result,
        ):
            result = FeatureManager().apply(
                from_dir=project_dir,
                target_name=None,
                plan_file=str(plan_path),
                dev_mode=False,
                allow_recreate=False,
            )
        assert "ops" in result
        assert isinstance(result["ops"], list)
        assert "executed" in result
        assert result["executed"] == 2


# ---------------------------------------------------------------------------
# list_specs — real library
# ---------------------------------------------------------------------------


class TestListSpecsIntegration:
    """``list_specs`` always reads from Snowflake under the
    Phase 3+4 manifest-driven surface (D1 deletes the file-positional
    surface).  The result envelope's ``source`` field is always
    ``"snowflake"`` — the legacy file-mode listings live on as
    ``snow feature export`` instead.
    """

    def test_list_specs_returns_source_snowflake(self, project_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        result = FeatureManager().list_specs(from_dir=project_dir, target_name=None)
        assert result["source"] == "snowflake"

    def test_list_specs_yields_dict_envelope(self, project_dir, mock_execute_query):
        """Even on an empty deployed schema the envelope is shaped
        ``{"source": "snowflake", "specs": [...]}``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        result = FeatureManager().list_specs(from_dir=project_dir, target_name=None)
        assert "specs" in result
        assert isinstance(result["specs"], list)


# ---------------------------------------------------------------------------
# Validation error path
# ---------------------------------------------------------------------------


_BAD_FV_YAML = textwrap.dedent(
    """\
    kind: StreamingFeatureView
    name: bad_fv
    version: null
    ordered_entity_column_names: []
    sources: []
    features: []
    """
)


class TestValidationErrorPath:
    def test_validation_error_returns_validation_failed(
        self, tmp_path, mock_execute_query
    ):
        """A spec with MISSING_VERSION should surface as validation_failed."""
        project_dir = _write_minimal_project(tmp_path)
        # Replace the well-formed FV with one that fails validation.
        (project_dir / "sources" / "feature_views" / "user_clicks.yaml").write_text(
            _BAD_FV_YAML
        )

        from snowflake.cli._plugins.feature.manager import FeatureManager

        result = FeatureManager().plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["status"] == "validation_failed"
        assert "errors" in result


# ---------------------------------------------------------------------------
# init — end-to-end with mocked Session/FeatureStore
# ---------------------------------------------------------------------------


class TestInitIntegration:
    """``init`` end-to-end against the real FeatureStore class.

    The autouse ``mock_cli_context`` fixture (imported from
    test_manager.py) supplies the connection's account /
    db / schema / role — the manifest scaffolder reads these to
    auto-derive the default-target body (D6 fail-fast on existing
    manifest, no ``--force``).
    """

    def test_init_returns_initialized_status(self, mock_execute_query, tmp_path):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = FeatureManager().init(from_dir=tmp_path, no_scaffold=False)
        assert result["status"] == "initialized"

    def test_init_scaffold_creates_directories(self, mock_execute_query, tmp_path):
        """Default ``init`` scaffolds the three canonical sub-dirs
        under ``sources/`` plus an empty ``out/plan/`` placeholder
        with a committable ``.gitkeep`` (D7 / D8)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)
        for d in ("entities", "datasources", "feature_views"):
            assert (
                tmp_path / "sources" / d
            ).is_dir(), f"sources/{d}/ must exist after scaffold"
        assert (tmp_path / "out" / "plan").is_dir()

    def test_init_no_scaffold_skips_manifest_and_dirs(
        self, mock_execute_query, tmp_path
    ):
        """``--no-scaffold`` skips both the manifest write AND every
        directory creation (D6).  The on-disk tree must look exactly
        the same before and after the call."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = FeatureManager().init(from_dir=tmp_path, no_scaffold=True)
        assert result["status"] == "skipped"
        assert not (tmp_path / "manifest.yml").exists()
        assert not (tmp_path / "sources").exists()
        assert not (tmp_path / "out").exists()

    def test_init_writes_out_plan_gitkeep(self, mock_execute_query, tmp_path):
        """Acceptance #10: ``init`` writes ``out/plan/.gitkeep`` so the
        relocated plan-file lifecycle (D8) has a deterministic,
        committable home in fresh projects."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)
        assert (tmp_path / "out" / "plan" / ".gitkeep").is_file()

    def test_init_fails_fast_on_existing_manifest(self, mock_execute_query, tmp_path):
        """Acceptance D6: ``init`` refuses to overwrite an existing
        ``manifest.yml`` — there is no ``--force`` escape."""
        (tmp_path / "manifest.yml").write_text(_MANIFEST_YAML)

        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.exceptions import CliError

        with pytest.raises(CliError):
            FeatureManager().init(from_dir=tmp_path, no_scaffold=False)


# ---------------------------------------------------------------------------
# Connection context wiring through to execute_plan
# ---------------------------------------------------------------------------


class TestApplySqlUsesConnectionContext:
    """Connection ``warehouse`` must reach ``execute_plan`` while
    ``database`` / ``schema`` come from the *plan file's*
    ``target_database`` / ``target_schema`` (which were sourced from
    the manifest target when the plan was written).  Bug C: plan
    files are warehouse-agnostic — the warehouse is always pulled
    from the active connection at apply time (D2 / D4).
    """

    def test_execute_plan_receives_connection_context(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.types import ApplyResult

        project_dir = _write_minimal_project(tmp_path)

        # Override the autouse mock with specific values
        ctx = mock_cli_context.return_value
        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"
        ctx.connection.warehouse = "MY_WH"

        mock_result = ApplyResult(
            status="applied",
            ops=[{"operation": "CREATE_FV", "name": "x", "status": "success"}],
        )

        plan_path = project_dir / "out" / "plan" / "feature_plan.json"
        _write_minimal_plan_json(
            plan_path,
            target_database="TEST_DB",
            target_schema="TEST_SCHEMA",
        )

        with mock.patch(
            "snowflake.ml.feature_store.decl.api.execute_plan",
            return_value=mock_result,
        ) as mock_exec:
            FeatureManager().apply(
                from_dir=project_dir,
                target_name=None,
                plan_file=str(plan_path),
                dev_mode=True,
                allow_recreate=False,
            )

        mock_exec.assert_called_once()
        call_kwargs = mock_exec.call_args.kwargs
        assert call_kwargs.get("database") == "TEST_DB", (
            f"database must match plan target_database. Got "
            f"{call_kwargs.get('database')!r}."
        )
        assert call_kwargs.get("schema") == "TEST_SCHEMA", (
            f"schema must match plan target_schema. Got "
            f"{call_kwargs.get('schema')!r}."
        )
        assert call_kwargs.get("warehouse") == "MY_WH", (
            f"Bug C: warehouse must come from the active connection, "
            f"not the plan file. Got {call_kwargs.get('warehouse')!r}."
        )


# ---------------------------------------------------------------------------
# write_plan + apply with plan_file
# ---------------------------------------------------------------------------


class TestWritePlanIntegration:
    """``write_plan()`` end-to-end: real validator / planner /
    serializer against the manifest project.
    """

    def test_write_plan_creates_json_file(
        self, project_dir, mock_execute_query, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = tmp_path / "plan.json"
        FeatureManager().write_plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=True,
            out_path=str(out_path),
        )
        parsed = json.loads(out_path.read_text())
        assert parsed["version"] == "1"
        assert "plan" in parsed
        assert "ops" in parsed["plan"]
        assert "summary" in parsed

    def test_write_plan_records_source_files(
        self, project_dir, mock_execute_query, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = tmp_path / "plan.json"
        FeatureManager().write_plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=True,
            out_path=str(out_path),
        )
        parsed = json.loads(out_path.read_text())
        assert isinstance(parsed["source_files"], list)

    def test_write_plan_records_target_info(
        self, project_dir, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Plan envelope records ``target_database`` /
        ``target_schema`` / ``target_name`` from the resolved
        manifest target — NOT from the active connection.  This is
        what lets ``apply`` later refuse a ``target_mismatch`` if
        the plan was written against a different target than the
        operator now requests (L6 / D4)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = tmp_path / "plan.json"
        FeatureManager().write_plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=True,
            out_path=str(out_path),
        )
        parsed = json.loads(out_path.read_text())
        assert parsed["target_database"] == "TEST_DB"
        assert parsed["target_schema"] == "TEST_SCHEMA"
        assert parsed["target_name"] == "DEFAULT"


class TestApplyWithPlanFileIntegration:
    """``apply(plan_file=...)`` end-to-end: deserialize the on-disk
    envelope and execute it through the real ``decl_api`` (with
    ``execute_plan`` mocked to surface the result without touching
    Snowflake)."""

    def test_apply_from_plan_file_returns_dict(
        self, project_dir, mock_execute_query, mock_cli_context, tmp_path
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.types import ApplyResult

        plan_path = tmp_path / "plan.json"
        FeatureManager().write_plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=True,
            out_path=str(plan_path),
        )

        mock_result = ApplyResult(status="applied", ops=[])
        with mock.patch(
            "snowflake.ml.feature_store.decl.api.execute_plan",
            return_value=mock_result,
        ):
            result = FeatureManager().apply(
                from_dir=project_dir,
                target_name=None,
                plan_file=str(plan_path),
                dev_mode=False,
                allow_recreate=False,
            )
        assert isinstance(result, dict)

    def test_apply_from_plan_file_skips_state_queries(
        self, project_dir, mock_execute_query, mock_cli_context, tmp_path
    ):
        """``apply(plan_file=...)`` skips SHOW queries since state is
        pre-computed in the envelope — the lifecycle invariant L5."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.ml.feature_store.decl.types import ApplyResult

        plan_path = tmp_path / "plan.json"
        FeatureManager().write_plan(
            from_dir=project_dir,
            target_name=None,
            variables=[],
            dev_mode=True,
            out_path=str(plan_path),
        )

        # Reset call count after write_plan
        mock_execute_query.reset_mock()

        mock_result = ApplyResult(status="applied", ops=[])
        with mock.patch(
            "snowflake.ml.feature_store.decl.api.execute_plan",
            return_value=mock_result,
        ):
            FeatureManager().apply(
                from_dir=project_dir,
                target_name=None,
                plan_file=str(plan_path),
                dev_mode=False,
                allow_recreate=False,
            )

        for call in mock_execute_query.call_args_list:
            sql = str(call.args[0]) if call.args else ""
            assert (
                "SHOW ONLINE FEATURE TABLES" not in sql
            ), f"State query executed unnecessarily: {sql}"


# ---------------------------------------------------------------------------
# ingest — client-side preflight against the registered StreamSource schema
# ---------------------------------------------------------------------------


class TestIngestSchemaPreflight:
    """End-to-end CLI → ``FeatureManager.ingest`` →
    mocked ``FeatureStore`` happy-path / sad-path against the
    registered StreamSource schema.

    Only ``_get_feature_store`` is mocked, so the preflight code path
    runs verbatim.  The CLI ``runner`` fixture (from
    ``tests/conftest.py``) gives us the real Typer command-error
    mapping (``ClickException`` → non-zero exit code with the message
    in stderr), which is the contract operators actually see.
    """

    _SIX_COL = [
        "USER_ID",
        "SESSION_ID",
        "PAGE_URL",
        "EVENT_TYPE",
        "TIMESTAMP",
        "TIME_ON_PAGE_SECONDS",
    ]

    @staticmethod
    def _stream_source_with_schema(field_names: list[str]) -> mock.MagicMock:
        """Build a mock ``StreamSource`` whose ``schema.fields[].name``
        sequence matches ``field_names`` exactly.  Mirrors the helper
        in ``test_manager.py`` so the unit and integration tests use
        an identical mock shape."""
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

    def _patch_fs(self, accepted: int = 0) -> tuple:
        """Patch ``FeatureManager._get_feature_store`` to return a
        ``MagicMock`` whose ``get_stream_source`` reports the
        canonical 6-column schema and whose ``stream_ingest`` returns
        ``accepted``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_fs = mock.MagicMock(name="feature_store")
        mock_fs.get_stream_source.return_value = self._stream_source_with_schema(
            self._SIX_COL
        )
        mock_fs.stream_ingest.return_value = accepted
        patcher = mock.patch.object(
            FeatureManager,
            "_get_feature_store",
            create=True,
            return_value=mock_fs,
        )
        return patcher, mock_fs

    def test_ingest_command_surfaces_missing_field_clickexception(
        self, runner, project_dir, monkeypatch
    ):
        """A 4-key events.json against a 6-column registered source
        must fail at the CLI with a ``ClickException`` whose message
        names the missing column (``PAGE_URL``).  Critically,
        ``fs.stream_ingest`` must never be invoked: zero HTTP traffic
        on bad input is the whole point of the preflight."""
        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        events = project_dir / "events.json"
        events.write_text(
            '[{"USER_ID": "u1", "SESSION_ID": "s1", '
            '"EVENT_TYPE": "page_view", '
            '"TIMESTAMP": "2026-05-07T18:00:00Z"}]'
        )
        patcher, mock_fs = self._patch_fs()

        with patcher:
            result = runner.invoke(
                [
                    "feature",
                    "ingest",
                    "CLICKSTREAM_EVENTS",
                    "--from",
                    str(project_dir),
                    "--data",
                    str(events),
                ]
            )

        assert result.exit_code != 0, (
            f"Bad payload must yield non-zero exit, "
            f"got exit_code=0; output={result.output!r}"
        )
        assert (
            "PAGE_URL" in result.output
        ), f"CLI error must name the missing column.  Output: {result.output!r}"
        mock_fs.stream_ingest.assert_not_called()

    def test_ingest_command_succeeds_with_matching_schema(
        self, runner, project_dir, monkeypatch
    ):
        """A 6-key events.json against a 6-column registered source
        passes the preflight and reaches ``fs.stream_ingest`` with
        the records unchanged.  The CLI emits the standard
        ``accepted_count`` envelope and exits 0."""
        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        events = project_dir / "events.json"
        events.write_text(
            "["
            '{"USER_ID": "u1", "SESSION_ID": "s1", '
            '"PAGE_URL": "https://example.com/p/1", '
            '"EVENT_TYPE": "page_view", '
            '"TIMESTAMP": "2026-05-07T18:00:00Z", '
            '"TIME_ON_PAGE_SECONDS": 12}'
            "]"
        )
        patcher, mock_fs = self._patch_fs(accepted=1)

        with patcher:
            result = runner.invoke(
                [
                    "feature",
                    "ingest",
                    "CLICKSTREAM_EVENTS",
                    "--from",
                    str(project_dir),
                    "--data",
                    str(events),
                ]
            )

        assert result.exit_code == 0, (
            f"Happy-path ingest must exit 0, got "
            f"exit_code={result.exit_code}; output={result.output!r}"
        )
        assert "accepted_count" in result.output, (
            f"CLI output must surface the accepted_count envelope.  "
            f"Output: {result.output!r}"
        )
        mock_fs.stream_ingest.assert_called_once()
