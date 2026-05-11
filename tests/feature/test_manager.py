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

"""Tests for FeatureManager — mocks the decl library."""

from unittest import mock

import pytest


@pytest.fixture
def mock_execute_query():
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


@pytest.fixture
def mock_decl():
    """Patch the entire decl api module used inside the manager."""
    with mock.patch("snowflake.cli._plugins.feature.manager.decl_api") as m:
        m.load_specs.return_value = mock.MagicMock(name="batch")
        m.fetch_applied_state.return_value = mock.MagicMock(name="state")
        m.validate_specs.return_value = []
        m.generate_plan.return_value = mock.MagicMock(name="plan", ops=[], warnings=[])
        m.serialize_plan.return_value = '{"version": "1", "plan": {"ops": [], "warnings": []}, "summary": {}, "created_at": "", "target_database": "TEST_DB", "target_schema": "TEST_SCHEMA", "source_files": []}'
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
        m.describe_specification_query.return_value = 'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."x" TYPE = SPECIFICATION'
        m.parse_specification_rows.return_value = None
        # execute_plan returns an ApplyResult-like mock (used for wet_run)
        exec_result = mock.MagicMock()
        exec_result.status = "applied"
        exec_result.ops = []
        exec_result.warnings = []
        exec_result.errors = []
        m.execute_plan.return_value = exec_result
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
        yield m


@pytest.fixture(autouse=True)
def mock_cli_context():
    """Patch get_cli_context for all manager tests."""
    with mock.patch("snowflake.cli._plugins.feature.manager.get_cli_context") as m:
        ctx = mock.MagicMock()

        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"
        ctx.connection.warehouse = "TEST_WH"
        ctx.connection.role = "TEST_ROLE"
        m.return_value = ctx
        yield m


@pytest.fixture(autouse=True)
def mock_build_session():
    """Patch _build_session so wet-run apply tests don't need a real connection."""
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._build_session",
        return_value=mock.MagicMock(name="session"),
    ):
        yield


_ALTER_SESSION_SQL = (
    "ALTER SESSION SET ENABLE_FEATURE_STORE_DESCRIBE_OFT_SPECIFICATION = TRUE"
)


def _executed_sqls(mock_execute_query):
    """Extract executed SQL strings from a mocked execute_query call list."""
    sqls = []
    for call in mock_execute_query.call_args_list:
        if call.args:
            sqls.append(str(call.args[0]))
    return sqls


def _wire_real_session_setup(mock_decl):
    """Make decl_api.ensure_session_setup actually invoke the executor.

    The shared ``mock_decl`` fixture replaces the entire decl_api module
    with a MagicMock, so by default ``ensure_session_setup`` is a no-op
    mock.  Tests that need to observe the priming SQL hitting
    ``execute_query`` install this side effect.
    """

    def fake_ensure(execute_query):
        execute_query(_ALTER_SESSION_SQL)

    mock_decl.ensure_session_setup.side_effect = fake_ensure


class TestFeatureManagerPlan:
    """``FeatureManager.plan`` must run validate + generate_plan and
    return ``{status, ops, warnings, errors}`` *without* generating
    SQL or executing anything against Snowflake.

    These tests pin the validate-then-plan contract that replaced
    the legacy ``apply(dry_run=True)`` → ``generate_apply_sql`` path.
    ``decl_api.generate_apply_sql`` and ``decl/sql_generator.py`` have
    been deleted; this suite is the lock-in that prevents a contributor
    from re-introducing SQL generation inside the planning code path.
    """

    def test_plan_returns_status_ready_with_ops_when_no_validation_errors(
        self, mock_execute_query, mock_decl
    ):
        """Happy path: validate_specs returns no ERROR results,
        generate_plan returns a Plan with two ops, manager.plan
        returns ``{status: "ready", ops: [...], errors: []}``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.validate_specs.return_value = []  # no errors
        from snowflake.ml.feature_store.decl.enums import OpKind

        op_no_change = mock.MagicMock()
        op_no_change.kind = OpKind.NO_CHANGE
        op_no_change.name = "USER"
        op_no_change.reason = ""
        op_no_change.destructive = False
        plan_obj = mock.MagicMock(name="plan", ops=[op_no_change], warnings=[])
        mock_decl.generate_plan.return_value = plan_obj

        mgr = FeatureManager()
        result = mgr.plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            allow_recreate=False,
            no_delete=True,
        )

        assert result["status"] == "ready", (
            f"plan() must return status=ready when validate_specs returns no "
            f"ERROR results.  Got status={result.get('status')!r}."
        )
        assert result["errors"] == []
        assert isinstance(result.get("ops"), list)
        assert len(result["ops"]) == 1

    def test_plan_returns_validation_failed_with_errors_when_validate_specs_returns_errors(
        self, mock_execute_query, mock_decl
    ):
        """When validate_specs surfaces an ERROR, plan() must short-circuit:
        return ``{status: validation_failed, errors: [...], ops: []}``
        and NOT call generate_plan."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        err = mock.MagicMock()
        err.severity = "ERROR"
        err.code = "VERSION_CONFLICT"
        err.message = "Version conflict on FV X"
        err.__str__ = lambda self: "VERSION_CONFLICT: Version conflict on FV X"
        warn = mock.MagicMock()
        warn.severity = "WARNING"
        warn.__str__ = lambda self: "WARNING: ..."
        mock_decl.validate_specs.return_value = [err, warn]

        mgr = FeatureManager()
        result = mgr.plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            allow_recreate=False,
            no_delete=True,
        )

        assert result["status"] == "validation_failed"
        assert result["errors"], "validation_failed must carry errors"
        assert any(
            "VERSION_CONFLICT" in str(e) for e in result["errors"]
        ), f"validation errors must include the underlying code; got {result['errors']!r}"
        assert result["ops"] == []
        mock_decl.generate_plan.assert_not_called()

    def test_plan_calls_resolve_datasource_columns(self, mock_execute_query, mock_decl):
        """plan() must invoke ``decl_api.resolve_datasource_columns`` so
        FV source refs carry the datasource's column schema before
        validation/planning.  Promoting this from a private helper to
        a public ``decl_api`` entry point also lets ``write_plan`` share
        the same pre-pass (parity invariant)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.validate_specs.return_value = []

        mgr = FeatureManager()
        mgr.plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            allow_recreate=False,
            no_delete=True,
        )

        mock_decl.resolve_datasource_columns.assert_called_once()

    def test_plan_calls_validate_specs_before_generate_plan(
        self, mock_execute_query, mock_decl
    ):
        """Pin the order: validate must run before generate_plan so a
        validation failure short-circuits without invoking the planner.
        We assert this via a Mock side effect that records call order."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        call_order: list[str] = []

        def record_validate(*_a, **_kw):
            call_order.append("validate_specs")
            return []

        def record_plan(*_a, **_kw):
            call_order.append("generate_plan")
            return mock.MagicMock(ops=[], warnings=[])

        mock_decl.validate_specs.side_effect = record_validate
        mock_decl.generate_plan.side_effect = record_plan

        mgr = FeatureManager()
        mgr.plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            allow_recreate=False,
            no_delete=True,
        )

        assert call_order == [
            "validate_specs",
            "generate_plan",
        ], f"validate_specs must run before generate_plan; got order={call_order!r}"

    def test_plan_does_not_invoke_execute_plan_or_sql_generation(
        self, mock_execute_query, mock_decl
    ):
        """plan() is read-only with respect to Snowflake state.  It must
        never call ``decl_api.execute_plan`` (Snowflake-side-effecting)
        and must not synthesise SQL strings via the deleted
        ``generate_apply_sql`` path.

        The mock_decl fixture no longer wires ``generate_apply_sql`` at
        all — this test pins that the manager-side code never re-introduces
        SQL generation during planning.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.validate_specs.return_value = []

        mgr = FeatureManager()
        mgr.plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            allow_recreate=False,
            no_delete=True,
        )

        mock_decl.execute_plan.assert_not_called()
        assert (
            not hasattr(mock_decl.generate_apply_sql, "called")
            or not mock_decl.generate_apply_sql.called
        ), (
            "plan() must not invoke decl_api.generate_apply_sql — that "
            "code path has been removed."
        )


class TestFeatureManagerListSpecs:
    def test_list_specs_returns_dict(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(input_files=(), config=None)
        assert isinstance(result, dict)

    def test_list_specs_load_specs_error_propagates(
        self, mock_execute_query, mock_decl
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.load_specs.side_effect = ValueError("bad spec file")
        mgr = FeatureManager()
        with pytest.raises(ValueError, match="bad spec file"):
            mgr.list_specs(input_files=("specs.yaml",), config=None)

    def test_list_specs_calls_list_state_queries(self, mock_execute_query, mock_decl):
        """list_specs() asks decl_api for the SQL set, never builds SQL itself."""
        mock_decl.enrich_list_results.return_value = []
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.list_specs(input_files=(), config=None)
        mock_decl.list_state_queries.assert_called_once_with("TEST_DB", "TEST_SCHEMA")

    def test_list_specs_runs_session_setup_before_state_queries(
        self, mock_execute_query, mock_decl
    ):
        """The Snowflake-bound list_specs branch must prime the session
        with ``ALTER SESSION SET ENABLE_FEATURE_STORE_DESCRIBE_OFT_SPECIFICATION
        = TRUE`` *before* any state SHOW/DESCRIBE query."""
        _wire_real_session_setup(mock_decl)
        mock_decl.enrich_list_results.return_value = []
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.list_specs(input_files=(), config=None)

        sqls = _executed_sqls(mock_execute_query)
        assert sqls, "expected at least one executed SQL"
        assert sqls[0] == _ALTER_SESSION_SQL, (
            f"first executed SQL must be the session-priming ALTER SESSION; "
            f"got: {sqls[0]!r} (full call list: {sqls})"
        )

    def test_list_specs_with_input_files_skips_session_setup(
        self, mock_execute_query, mock_decl
    ):
        """File-only list_specs must not touch the Snowflake session."""
        _wire_real_session_setup(mock_decl)
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.list_specs(input_files=("specs.yaml",), config=None)

        mock_decl.ensure_session_setup.assert_not_called()
        sqls = _executed_sqls(mock_execute_query)
        assert (
            _ALTER_SESSION_SQL not in sqls
        ), f"file-only list_specs must not run ALTER SESSION; got: {sqls}"

    def test_list_specs_aborts_when_session_setup_fails(
        self, mock_execute_query, mock_decl
    ):
        """If ALTER SESSION priming fails, ``SessionSetupError`` must
        propagate from list_specs and *no* further SQL must be issued."""
        from snowflake.ml.feature_store.decl.errors import SessionSetupError

        def fake_ensure(execute_query):
            try:
                execute_query(_ALTER_SESSION_SQL)
            except Exception as exc:
                raise SessionSetupError(_ALTER_SESSION_SQL, exc) from exc

        mock_decl.ensure_session_setup.side_effect = fake_ensure
        mock_decl.SessionSetupError = SessionSetupError
        mock_execute_query.side_effect = RuntimeError("priming SQL failed")

        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        with pytest.raises(SessionSetupError):
            mgr.list_specs(input_files=(), config=None)

        sqls = _executed_sqls(mock_execute_query)
        assert sqls == [
            _ALTER_SESSION_SQL
        ], f"only the ALTER SESSION attempt should have been made; got: {sqls}"

    def test_list_specs_forwards_entity_rows_and_spec_map(
        self, mock_execute_query, mock_decl
    ):
        """list_specs() must pass entity_rows and specification_map kwargs
        to enrich_list_results so the table can include all three kinds."""
        mock_decl.enrich_list_results.return_value = []
        mock_decl.parse_specification_rows.return_value = {
            "kind": "StreamingFeatureView",
            "metadata": {
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "name": "fv",
                "version": "v1",
            },
            "spec": {
                "ordered_entity_column_names": ["user_id"],
                "sources": [{"name": "src", "source_type": "Stream"}],
                "features": [],
            },
        }
        mock_decl.fetch_entity_rows.return_value = [
            {
                "name": "SNOWML_FEATURE_STORE_ENTITY_USER",
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
                "allowed_values": '["USER_ID"]',
            }
        ]
        # show_ofts row + DESCRIBE SPECIFICATION row.  Entity rows now come
        # from ``decl_api.fetch_entity_rows`` (imperative path), not from a
        # raw ``SHOW TAGS`` query, so this iterator no longer feeds a
        # SHOW TAGS response.
        from snowflake.connector.cursor import DictCursor  # noqa: F401

        responses = iter(
            [
                iter(
                    [
                        {
                            "name": "FV$V1$ONLINE",
                            "database_name": "TEST_DB",
                            "schema_name": "TEST_SCHEMA",
                        }
                    ]
                ),
                iter([{"specification": "{}"}]),
            ]
        )
        mock_execute_query.side_effect = lambda *a, **kw: next(responses)

        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        with mock.patch.object(
            FeatureManager, "_build_session", return_value=mock.MagicMock()
        ):
            mgr.list_specs(input_files=(), config=None)

        call = mock_decl.enrich_list_results.call_args
        kwargs = call.kwargs
        assert "entity_rows" in kwargs
        assert "specification_map" in kwargs
        # Spec map populated with the parsed result.
        assert "FV$V1$ONLINE" in kwargs["specification_map"]
        # Entity row passed through (translated SHOW TAGS shape).
        assert any(
            r.get("name", "").startswith("SNOWML_FEATURE_STORE_ENTITY_")
            for r in kwargs["entity_rows"]
        )

    def test_fetch_entity_rows_uses_decl_api_facade(self, mock_decl):
        """The CLI must delegate entity-row fetching to ``decl_api.fetch_entity_rows``
        and never issue a raw ``SHOW TAGS`` query of its own.

        Pin two invariants:
          - ``decl_api.fetch_entity_rows`` is invoked exactly once with
            the connection-context db/schema and a Snowpark session;
          - no SQL is sent through ``execute_query`` from this helper.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.fetch_entity_rows.return_value = [
            {"name": "SNOWML_FEATURE_STORE_ENTITY_X", "allowed_values": "[]"}
        ]
        sentinel_session = mock.MagicMock(name="snowpark_session")

        mgr = FeatureManager()
        ctx = mock.MagicMock()
        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"
        ctx.connection.warehouse = "TEST_WH"

        with mock.patch.object(
            FeatureManager, "_build_session", return_value=sentinel_session
        ) as build, mock.patch.object(FeatureManager, "execute_query") as execute:
            # SLF001: this is a unit test that explicitly pins the
            # private helper's delegation contract (the CLI-side
            # entity read path).  Promoting `_fetch_entity_rows` to a
            # public method would expand the manager's API surface
            # solely to satisfy the linter, which is the wrong
            # trade-off here.
            rows = mgr._fetch_entity_rows(ctx)  # noqa: SLF001

        assert rows == mock_decl.fetch_entity_rows.return_value
        build.assert_called_once_with()
        mock_decl.fetch_entity_rows.assert_called_once_with(
            sentinel_session, "TEST_DB", "TEST_SCHEMA", "TEST_WH"
        )
        execute.assert_not_called()

    def test_fetch_entity_rows_tolerates_failure(self, mock_decl):
        """Missing-privilege paths must still let list complete by
        returning an empty list when the imperative call raises."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.fetch_entity_rows.side_effect = RuntimeError("denied")

        mgr = FeatureManager()
        ctx = mock.MagicMock()
        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"

        with mock.patch.object(
            FeatureManager, "_build_session", return_value=mock.MagicMock()
        ):
            # SLF001: same rationale as
            # ``test_fetch_entity_rows_uses_decl_api_facade`` — this
            # test pins the private helper's graceful-degradation
            # contract (empty list on imperative-side failure), which
            # is internal-only behaviour.
            rows = mgr._fetch_entity_rows(ctx)  # noqa: SLF001
        assert rows == []


class TestFeatureManagerDescribe:
    def test_describe_returns_dict(self, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.describe(name="MY_ENTITY")
        assert isinstance(result, dict)

    def test_describe_runs_session_setup(self, mock_execute_query, mock_decl):
        """describe() must prime the session before issuing any state SQL."""
        _wire_real_session_setup(mock_decl)
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.describe(name="MY_FV")

        sqls = _executed_sqls(mock_execute_query)
        assert sqls, "expected at least one executed SQL"
        assert sqls[0] == _ALTER_SESSION_SQL, (
            f"first executed SQL must be the session-priming ALTER SESSION; "
            f"got: {sqls[0]!r} (full call list: {sqls})"
        )


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------


class TestFeatureManagerGetStatus:
    def test_get_status_returns_dict(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """get_status() always returns a dict."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "message": "ok",
            "endpoints": [],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert isinstance(result, dict)

    def test_get_status_calls_service_sql_with_db_and_schema(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """get_status() calls service_sql with the connection database and schema."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "endpoints": [],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.get_status()
        mock_decl.service_sql.assert_called_once_with("TEST_DB", "TEST_SCHEMA")

    def test_get_status_calls_parse_service_status(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """get_status() delegates parsing to decl_api.parse_service_status."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("some_raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "message": "All systems go",
            "endpoints": [{"name": "query", "url": "https://example.com/query"}],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        mock_decl.parse_service_status.assert_called_once_with("some_raw_json")
        assert result["status"] == "RUNNING"

    def test_get_status_has_status_field(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """Result dict includes 'status' with the correct value."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "message": "ok",
            "endpoints": [],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert "status" in result
        assert result["status"] == "RUNNING"

    def test_get_status_empty_response_returns_error(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """When execute_query returns no rows, get_status returns an error dict."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert result["status"] == "error"
        mock_decl.parse_service_status.assert_not_called()

    def test_get_status_error_on_execute_exception(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """When execute_query raises, get_status returns an error dict."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.side_effect = RuntimeError("DB connection failed")
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert result["status"] == "error"
        assert "error" in result


# ---------------------------------------------------------------------------
# export_specs
# ---------------------------------------------------------------------------

_SHOW_ROW_SINGLE = {
    "name": "my_fv$v1$ONLINE",
    "database_name": "TEST_DB",
    "schema_name": "TEST_SCHEMA",
    "scheduling_state": "RUNNING",
}

_DESCRIBE_ROWS_SINGLE = [
    {
        "name": "USER_ID",
        "type": "VARCHAR(16777216)",
        "kind": "COLUMN",
        "null?": "N",
        "primary key": "Y",
        "unique key": "N",
    },
    {
        "name": "CLICK_COUNT_1H",
        "type": "NUMBER(38,0)",
        "kind": "COLUMN",
        "null?": "Y",
        "primary key": "N",
        "unique key": "N",
    },
    {
        "name": "TIMESTAMP",
        "type": "TIMESTAMP_NTZ(9)",
        "kind": "COLUMN",
        "null?": "Y",
        "primary key": "N",
        "unique key": "N",
    },
]


def _make_show_row(fv_name, entity_col="USER_ID"):
    return {
        "name": f"{fv_name}$v1$ONLINE",
        "database_name": "TEST_DB",
        "schema_name": "TEST_SCHEMA",
        "scheduling_state": "RUNNING",
    }


def _make_describe_rows(entity_col="USER_ID"):
    return [
        {
            "name": entity_col,
            "type": "VARCHAR(16777216)",
            "kind": "COLUMN",
            "null?": "N",
            "primary key": "Y",
            "unique key": "N",
        },
        {
            "name": "FEATURE_COL",
            "type": "NUMBER(38,0)",
            "kind": "COLUMN",
            "null?": "Y",
            "primary key": "N",
            "unique key": "N",
        },
    ]


# ---------------------------------------------------------------------------
# target_info
# ---------------------------------------------------------------------------


class TestTargetInfo:
    def test_apply_result_contains_target_database(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """apply() result includes target_database from the CLI connection context."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert "target_database" in result
        assert result["target_database"] == "TEST_DB"

    def test_apply_result_contains_target_schema(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """apply() result includes target_schema from the CLI connection context."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert "target_schema" in result
        assert result["target_schema"] == "TEST_SCHEMA"

    def test_apply_result_contains_target_warehouse(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """apply() result includes target_warehouse from the CLI connection context."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert "target_warehouse" in result
        assert result["target_warehouse"] == "TEST_WH"

    def test_apply_wet_run_result_contains_target_info(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """apply() returns target info."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert result.get("target_database") == "TEST_DB"
        assert result.get("target_schema") == "TEST_SCHEMA"

    def test_list_specs_from_snowflake_contains_target_info(
        self, mock_execute_query, mock_decl
    ):
        """list_specs() from Snowflake returns target info."""
        mock_decl.enrich_list_results.return_value = []
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(input_files=(), config=None)
        assert "target_database" in result
        assert result["target_database"] == "TEST_DB"
        assert "target_schema" in result
        assert result["target_schema"] == "TEST_SCHEMA"

    def test_target_info_empty_when_connection_has_no_warehouse(
        self, mock_execute_query, mock_decl, mock_cli_context, tmp_path, monkeypatch
    ):
        """target_warehouse is empty string when connection warehouse is None."""
        mock_cli_context.return_value.connection.warehouse = None
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert result.get("target_warehouse") == ""


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestFeatureManagerInit:
    def test_init_calls_feature_store_with_create_if_not_exist(
        self, mock_execute_query, mock_cli_context
    ):
        """init() constructs FeatureStore with CreationMode.CREATE_IF_NOT_EXIST."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ) as mock_fs_cls, mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            mgr.init(no_scaffold=True)
            mock_fs_cls.assert_called_once()
            call_kwargs = mock_fs_cls.call_args[1]
            assert call_kwargs["creation_mode"] == mock_cm.CREATE_IF_NOT_EXIST

    def test_init_no_scaffold_skips_directory_creation(
        self, mock_execute_query, mock_cli_context, tmp_path, monkeypatch
    ):
        """init(no_scaffold=True) should not create any local directories."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = mgr.init(no_scaffold=True)
        assert result["directories"] == []
        assert not (tmp_path / "entities").exists()
        assert not (tmp_path / "datasources").exists()
        assert not (tmp_path / "feature_views").exists()

    def test_init_creates_three_directories(
        self, mock_execute_query, mock_cli_context, tmp_path, monkeypatch
    ):
        """init() (scaffold=True default) creates entities/, datasources/, feature_views/."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = mgr.init(no_scaffold=False)
        assert set(result["directories"]) == {
            "entities",
            "datasources",
            "feature_views",
        }
        assert (tmp_path / "entities").is_dir()
        assert (tmp_path / "datasources").is_dir()
        assert (tmp_path / "feature_views").is_dir()

    def test_init_returns_status_initialized(
        self, mock_execute_query, mock_cli_context, tmp_path, monkeypatch
    ):
        """init() returns a dict with status='initialized'."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = mgr.init(no_scaffold=True)
        assert result["status"] == "initialized"
        assert result["database"] == "TEST_DB"
        assert result["schema"] == "TEST_SCHEMA"


# ---------------------------------------------------------------------------
# export_specs
# ---------------------------------------------------------------------------


class TestFeatureManagerExportSpecs:
    """Tests for export_specs — verifies the strict full-fidelity SPECIFICATION flow."""

    def _setup_show_specification(
        self, mock_execute_query, show_rows, specification_rows=None
    ):
        """Configure execute_query mock for SHOW + DESCRIBE TYPE = SPECIFICATION queries."""
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

    def test_export_delegates_to_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """export_specs calls decl_api.export_specs with collected rows."""
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        self._setup_show_specification(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))
        mock_decl.export_specs.assert_called_once()
        assert result["status"] == "exported"

    def test_export_passes_correct_args_to_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """export_specs forwards show_rows, an empty describe_map, output_dir, db, schema."""
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        self._setup_show_specification(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        call_args = mock_decl.export_specs.call_args
        show_rows_arg, describe_map_arg, out_dir_arg, db_arg, schema_arg = call_args[0]
        assert show_rows_arg == [_SHOW_ROW_SINGLE]
        # Column-level DESCRIBE has been removed — full fidelity comes from
        # the SPECIFICATION JSON, so the legacy describe_map is empty.
        assert describe_map_arg == {}
        assert out_dir_arg == str(tmp_path)
        assert db_arg == "TEST_DB"
        assert schema_arg == "TEST_SCHEMA"

    def test_export_returns_decl_api_result(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """export_specs returns the dict from decl_api.export_specs unchanged."""
        expected = {
            "status": "exported",
            "directory": "/some/dir",
            "files": ["a.yaml", "b.yaml"],
        }
        mock_decl.export_specs.return_value = expected
        self._setup_show_specification(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))
        assert result == expected

    def test_export_empty_schema_skips_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """When SHOW returns no rows, decl_api.export_specs is not called."""
        mock_execute_query.return_value = iter([])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))
        assert result["status"] == "exported"
        assert result["files"] == []
        mock_decl.export_specs.assert_not_called()

    def test_export_specs_runs_session_setup_then_show_ofts_then_specification(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """Strict flow: ALTER SESSION priming → SHOW OFTs → DESCRIBE TYPE = SPECIFICATION per OFT."""
        _wire_real_session_setup(mock_decl)
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        mock_decl.parse_specification_rows.return_value = {"kind": "FeatureView"}
        show_rows = [_make_show_row("fv_a"), _make_show_row("fv_b")]
        self._setup_show_specification(mock_execute_query, show_rows)
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        sqls = _executed_sqls(mock_execute_query)
        assert sqls, "expected at least one executed SQL"
        # 1: ALTER SESSION priming first.
        assert sqls[0] == _ALTER_SESSION_SQL, (
            f"first executed SQL must be the session-priming ALTER SESSION; "
            f"got: {sqls[0]!r} (full call list: {sqls})"
        )
        # 2: SHOW ONLINE FEATURE TABLES second.
        assert "SHOW ONLINE FEATURE TABLES" in sqls[1], (
            f"second executed SQL must be SHOW ONLINE FEATURE TABLES; "
            f"got: {sqls[1]!r}"
        )
        # 3+: One DESCRIBE TYPE = SPECIFICATION per OFT row, in order.
        spec_sqls = [s for s in sqls[2:] if "TYPE = SPECIFICATION" in s]
        assert len(spec_sqls) == len(show_rows), (
            f"expected one DESCRIBE TYPE = SPECIFICATION per OFT "
            f"(rows={len(show_rows)}); got {len(spec_sqls)}: {spec_sqls}"
        )
        # No legacy column-level DESCRIBE issued.
        legacy = [
            s
            for s in sqls
            if "DESCRIBE ONLINE FEATURE TABLE" in s and "TYPE = SPECIFICATION" not in s
        ]
        assert legacy == [], (
            f"export_specs must not issue legacy column-level DESCRIBE; "
            f"got: {legacy}"
        )

    def test_export_specs_passes_specification_map_to_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """decl_api.export_specs receives a non-empty specification_map keyed by OFT name."""
        parsed_spec = {
            "kind": "StreamingFeatureView",
            "metadata": {
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "name": "fv_a",
                "version": "v1",
            },
            "spec": {"sources": [], "features": []},
        }
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        mock_decl.parse_specification_rows.return_value = parsed_spec
        show_rows = [_make_show_row("fv_a"), _make_show_row("fv_b")]
        self._setup_show_specification(mock_execute_query, show_rows)
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        call = mock_decl.export_specs.call_args
        assert (
            "specification_map" in call.kwargs
        ), f"expected specification_map keyword arg; got kwargs={list(call.kwargs)}"
        spec_map = call.kwargs["specification_map"]
        assert (
            isinstance(spec_map, dict) and spec_map
        ), f"expected non-empty specification_map dict; got: {spec_map!r}"
        oft_names = {row["name"] for row in show_rows}
        assert set(spec_map.keys()) == oft_names, (
            f"specification_map keys must match OFT names; "
            f"got keys={set(spec_map.keys())}, expected={oft_names}"
        )

    def test_export_specs_aborts_when_specification_parse_fails(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """If parse_specification_rows raises, the error propagates and decl_api.export_specs is not called."""
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        mock_decl.parse_specification_rows.side_effect = ValueError(
            "spec payload malformed"
        )
        self._setup_show_specification(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        with pytest.raises(ValueError, match="spec payload malformed"):
            mgr.export_specs(str(tmp_path))

        mock_decl.export_specs.assert_not_called()

    def test_export_specs_fetches_entity_rows(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """export_specs must call decl_api.fetch_entity_rows so orphan tags are exported.

        Without this call the exporter falls back to FV-derived entity
        emission, which silently drops orphan tags and breaks the
        export ↔ plan round-trip invariant in full-directory mode.
        """
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        mock_decl.fetch_entity_rows.return_value = [
            {
                "name": "SNOWML_FEATURE_STORE_ENTITY_USER_ID",
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
                "allowed_values": '["USER_ID"]',
            }
        ]
        self._setup_show_specification(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        assert mock_decl.fetch_entity_rows.called, (
            "manager.export_specs must call decl_api.fetch_entity_rows so the "
            "exporter has the authoritative list of registered entity tags"
        )

    def test_export_specs_forwards_entity_rows_to_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """The fetched entity_rows must be passed as entity_rows= kwarg to decl_api.export_specs."""
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        sentinel_rows = [
            {
                "name": "SNOWML_FEATURE_STORE_ENTITY_USER_ID",
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
                "allowed_values": '["USER_ID"]',
                "comment": "user identifier",
            },
            {
                "name": "SNOWML_FEATURE_STORE_ENTITY_ORPHAN_KEY",
                "database_name": "TEST_DB",
                "schema_name": "TEST_SCHEMA",
                "allowed_values": '["ORPHAN_KEY"]',
            },
        ]
        mock_decl.fetch_entity_rows.return_value = sentinel_rows
        self._setup_show_specification(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        call = mock_decl.export_specs.call_args
        assert (
            "entity_rows" in call.kwargs
        ), f"expected entity_rows keyword arg; got kwargs={list(call.kwargs)}"
        assert call.kwargs["entity_rows"] is sentinel_rows, (
            "manager must forward the rows from decl_api.fetch_entity_rows verbatim "
            f"into decl_api.export_specs(entity_rows=...); got {call.kwargs['entity_rows']!r}"
        )

    def test_export_specs_forwards_empty_entity_rows_when_none_registered(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """When no entities are registered, the manager still forwards the empty list.

        An explicit empty list signals "no entities" — distinct from
        ``None`` which would trigger the legacy fallback in the
        exporter.  The manager must always call fetch_entity_rows and
        always forward whatever it returns.
        """
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        mock_decl.fetch_entity_rows.return_value = []
        self._setup_show_specification(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        call = mock_decl.export_specs.call_args
        assert call.kwargs.get("entity_rows") == [], (
            "manager must forward an empty list verbatim (not None) so the "
            f"exporter can distinguish 'no entities' from 'caller forgot'; got {call.kwargs!r}"
        )


# ---------------------------------------------------------------------------
# write_plan
# ---------------------------------------------------------------------------


class TestWritePlan:
    def test_write_plan_creates_file(self, mock_execute_query, mock_decl, tmp_path):
        """write_plan() writes a JSON file to the given path."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        assert (tmp_path / "plan.json").exists()

    def test_write_plan_file_is_valid_json(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """File written by write_plan() is valid JSON."""
        import json

        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        content = (tmp_path / "plan.json").read_text()
        parsed = json.loads(content)
        assert isinstance(parsed, dict)
        assert parsed["version"] == "1"

    def test_write_plan_returns_path(self, mock_execute_query, mock_decl, tmp_path):
        """write_plan() returns the path to the written file."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        result = mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        assert result == out_path

    def test_write_plan_creates_parent_dirs(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """write_plan() creates parent directories if they don't exist."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plans" / "subdir" / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        assert (tmp_path / "plans" / "subdir" / "plan.json").exists()

    def test_write_plan_calls_load_specs(self, mock_execute_query, mock_decl, tmp_path):
        """write_plan() calls decl_api.load_specs with the provided files."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        mock_decl.load_specs.assert_called_once()

    def test_write_plan_calls_generate_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """write_plan() calls decl_api.generate_plan."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        mock_decl.generate_plan.assert_called_once()

    def test_write_plan_calls_serialize_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """write_plan() calls decl_api.serialize_plan."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.serialize_plan.return_value = '{"version": "1", "plan": {"ops": [], "warnings": []}, "summary": {}, "created_at": "", "target_database": "TEST_DB", "target_schema": "TEST_SCHEMA", "source_files": []}'
        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        mock_decl.serialize_plan.assert_called_once()

    def test_write_plan_forwards_connection_context_to_generate_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """``write_plan`` must thread the active connection's database +
        schema into ``decl_api.generate_plan`` so the planner qualifies
        every spec key against the same context the planning path
        (``manager.plan`` → ``decl_api.generate_plan``) qualifies
        against.

        Without this, the disk plan and the terminal-rendered plan
        diverge for any spec missing explicit ``database:`` /
        ``schema:`` (single-file invocations, bare entity YAMLs, etc.):
        ``write_plan`` calls the planner with no context, the
        unqualified key never collides with the fully-qualified
        applied-state key, and every existing object materialises as
        a phantom ``CREATE_*`` (and, in full-directory mode, a phantom
        ``DROP_*``).

        The ``mock_cli_context`` fixture (autouse) installs
        ``database="TEST_DB"`` and ``schema="TEST_SCHEMA"`` on the CLI
        context, so this test pins the contract that ``write_plan``
        forwards exactly those two values to the planner.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )

        mock_decl.generate_plan.assert_called_once()
        call_kwargs = mock_decl.generate_plan.call_args.kwargs
        assert call_kwargs.get("database") == "TEST_DB", (
            f"write_plan failed to forward connection database to generate_plan; "
            f"got kwargs={call_kwargs}.  This is the parity bug: the disk plan "
            f"will diverge from the planning UI plan whenever a spec "
            f"omits an explicit database field."
        )
        assert call_kwargs.get("schema") == "TEST_SCHEMA", (
            f"write_plan failed to forward connection schema to generate_plan; "
            f"got kwargs={call_kwargs}."
        )


# ---------------------------------------------------------------------------
# apply with plan_file
# ---------------------------------------------------------------------------


class TestApplyWithPlanFile:
    def _write_plan_file(self, path: str) -> None:
        """Write a minimal plan JSON file for testing apply(plan_file=...)."""
        import json

        plan_data = {
            "version": "1",
            "created_at": "2024-01-01T00:00:00",
            "target_database": "TEST_DB",
            "target_schema": "TEST_SCHEMA",
            "source_files": ["specs.yaml"],
            "plan": {"ops": [], "warnings": []},
            "summary": {},
        }
        with open(path, "w") as f:
            json.dump(plan_data, f)

    def test_apply_with_plan_file_returns_dict(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) returns a dict."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=[],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        assert isinstance(result, dict)

    def test_apply_with_plan_file_calls_deserialize(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) calls decl_api.deserialize_plan with the file content."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=[],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        mock_decl.deserialize_plan.assert_called_once()

    def test_apply_with_plan_file_skips_load_specs(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) does not call load_specs (plan is pre-computed)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=[],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        mock_decl.load_specs.assert_not_called()

    def test_apply_with_plan_file_skips_generate_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) does not call generate_plan (plan is pre-computed)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=[],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        mock_decl.generate_plan.assert_not_called()

    def test_apply_from_plan_file_forwards_warehouse_from_connection(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """A6 (Bug C): when applying from a plan file in wet-run mode,
        the manager must forward the *connection's* warehouse to
        ``decl_api.execute_plan``, not the empty string currently
        hardcoded in ``_apply_from_plan_file``.

        Plan files are warehouse-agnostic by design — they encode
        ``target_database`` / ``target_schema`` (the destination of the
        DDL), but the warehouse is a session-level execution detail
        owned by the active ``snow`` connection.  The lazy
        ``FeatureStore`` constructor needs a real warehouse for any
        FV-bearing plan; an empty string fails it.

        Pin: ``execute_plan`` is called with ``warehouse="TEST_WH"``
        sourced from the autouse ``mock_cli_context`` fixture.

        Expected on ``main``: warehouse is ``""`` (hardcoded). Test fails.
        After Phase 1's Bug C fix: warehouse is ``"TEST_WH"``. Test
        passes.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=[],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )

        mock_decl.execute_plan.assert_called_once()
        kwargs = mock_decl.execute_plan.call_args.kwargs
        assert kwargs.get("warehouse") == "TEST_WH", (
            "Bug C: _apply_from_plan_file must forward the connection's "
            "warehouse to execute_plan, not the empty string hardcoded "
            "today.  Plan files are warehouse-agnostic; the active "
            "connection owns warehouse selection so the same plan can "
            "run from any compatible warehouse.  See "
            "'Apply Lifecycle Resilience' plan, A6 / Bug C."
        )


# ---------------------------------------------------------------------------
# Apply lifecycle artifacts: required-plan, latest-wins, discard-older,
# mark-applied / mark-failed-stays-unapplied, target-match (L1–L7).
# Tests A1–A5 from the 'Apply Lifecycle Resilience' plan.
# ---------------------------------------------------------------------------


class TestApplyLifecycleArtifacts:
    """L1–L7 invariants for ``apply <path>`` (no ``--plan``).

    The new contract: ``apply`` is a pure plan-file consumer.  These
    tests pin the plan-file lifecycle (unapplied → applied / discarded)
    that replaces the wet-run re-plan branch.

    All tests use ``tmp_path`` as a faux CWD (via ``monkeypatch.chdir``)
    so that the ``.snowflake/plans/`` discovery resolves into a
    sandboxed directory and never touches the real workspace.
    """

    @staticmethod
    def _make_plan_json(
        target_database: str = "TEST_DB",
        target_schema: str = "TEST_SCHEMA",
    ) -> str:
        """Return a minimal valid PlanFile JSON envelope as a string."""
        import json

        return json.dumps(
            {
                "version": "1",
                "created_at": "2026-05-07T00:00:00+00:00",
                "target_database": target_database,
                "target_schema": target_schema,
                "source_files": ["specs.yaml"],
                "plan": {"ops": [], "warnings": []},
                "summary": {},
            }
        )

    @staticmethod
    def _make_plans_dir(cwd) -> "object":
        """Create ``<cwd>/.snowflake/plans/`` and return the Path."""
        plans_dir = cwd / ".snowflake" / "plans"
        plans_dir.mkdir(parents=True, exist_ok=True)
        return plans_dir

    # --- A1: L1 — Required-Plan ---

    def test_apply_without_plan_file_errors_when_no_plan_exists(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """A1 (L1): ``apply <path>`` with no unapplied plan file must
        fail fast with a clear error pointing the operator at
        ``snow feature plan <path>``.

        The new architecture removes the wet-run re-plan branch
        entirely — there is no fallback.  If the plans directory is
        empty (or absent), apply must NOT call ``generate_plan``,
        ``execute_plan``, or any other planning primitive.  It must
        return a structured ``status="no_plan"`` result whose error
        text mentions ``plan`` so the user knows what to do next.

        Expected on ``main``: today's wet-run branch silently re-plans
        from source, so ``generate_plan`` IS called and the test
        fails.  After Phase 1: the wet-run branch is gone, the test
        passes.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        # No .snowflake/plans/ at all — apply must error, not re-plan.

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        assert isinstance(result, dict)
        assert result.get("status") == "no_plan", (
            "L1 invariant violated: apply <path> with no unapplied plan "
            "file must return status='no_plan'.  Got "
            f"status={result.get('status')!r}.  See 'Apply Lifecycle "
            "Resilience' plan, A1 / L1."
        )
        errors = result.get("errors", [])
        assert errors, "L1: no_plan result must carry an error message"
        joined = " ".join(errors).lower()
        assert "plan" in joined, (
            f"L1: no_plan error text must mention 'plan' to direct the "
            f"operator at `snow feature plan`.  Got: {errors!r}"
        )
        # No re-planning, no execution.
        mock_decl.generate_plan.assert_not_called()
        mock_decl.execute_plan.assert_not_called()

    # --- A2: L2 — Latest-Wins ---

    def test_apply_picks_latest_unapplied_plan_file(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """A2 (L2): when multiple unapplied plans exist under
        ``<cwd>/.snowflake/plans/``, ``apply`` picks the newest by
        filename timestamp (lexicographic sort works because the format
        ``feature_plan_<UTC YYYYMMDDTHHMMSS>.json`` is monotonic).

        Pin: ``decl_api.deserialize_plan`` is called with the *content*
        of the newer file, not the older one.  We disambiguate by
        embedding distinct ``target_schema`` values in the two files
        and asserting the JSON passed to ``deserialize_plan`` carries
        the newer schema.

        Expected on ``main``: no auto-discovery exists — the wet-run
        branch re-plans from source instead.  Test fails.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        older = plans_dir / "feature_plan_20260101T000000.json"
        newer = plans_dir / "feature_plan_20260102T000000.json"
        older.write_text(self._make_plan_json(target_schema="OLDER_SCHEMA"))
        newer.write_text(self._make_plan_json(target_schema="TEST_SCHEMA"))

        # Wire deserialize_plan to pretend it parses whatever JSON
        # string the manager passes in — our pin is on the JSON
        # *string* the manager hands to deserialize_plan, regardless of
        # the structured PlanFile object.
        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        mock_decl.deserialize_plan.assert_called_once()
        passed_json = mock_decl.deserialize_plan.call_args.args[0]
        assert "TEST_SCHEMA" in passed_json, (
            "L2: apply must pick the *newer* unapplied plan file.  "
            "Got JSON content from the older file (carries OLDER_SCHEMA)."
        )
        assert (
            "OLDER_SCHEMA" not in passed_json
        ), "L2: the older plan file's content must NOT be loaded."

    # --- A3: L3 — Discard-Older ---

    def test_apply_renames_older_unapplied_plans_to_discarded(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """A3 (L3): when ``apply`` runs with multiple unapplied plans,
        all plans except the newest are renamed to
        ``<name>.discarded`` *before* execution begins.  This keeps
        the plans directory in a normalised state (at most one
        unapplied plan at any moment a user might inspect it).

        Pin: after apply returns, the older file no longer exists at
        its original name and a sibling ``<name>.discarded`` exists.

        Expected on ``main``: no rename happens; older file still
        exists.  Test fails.
        """
        from pathlib import Path

        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        older = plans_dir / "feature_plan_20260101T000000.json"
        newer = plans_dir / "feature_plan_20260102T000000.json"
        older.write_text(self._make_plan_json())
        newer.write_text(self._make_plan_json())

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        older_discarded = Path(str(older) + ".discarded")
        assert older_discarded.exists(), (
            f"L3: older unapplied plan must be renamed to .discarded "
            f"before execution.  Expected {older_discarded.name} to exist."
        )
        assert not older.exists(), (
            f"L3: original older plan file must NOT survive after apply.  "
            f"Got {older.name} still present."
        )

    # --- A4: L4 — Mark-Applied + L5 — Mark-Failed-Stays-Unapplied ---

    def test_apply_renames_plan_to_applied_after_success(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """A4 (L4): on successful execution, the consumed plan file is
        renamed to ``<name>.applied``.  Together with L3 this means
        a typical ``plan; apply`` cycle leaves a single ``.applied``
        file as a forensic record.

        Pin: after apply (with a mocked ``execute_plan`` returning
        ``status="applied"``), the bare ``.json`` is gone and
        ``.json.applied`` exists.

        Expected on ``main``: no rename; bare ``.json`` still exists.
        Test fails.
        """
        from pathlib import Path

        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        plan_path = plans_dir / "feature_plan_20260507T120000.json"
        plan_path.write_text(self._make_plan_json())

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"
        # execute_plan is already wired to return status="applied" by
        # the autouse mock_decl fixture.

        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        applied_marker = Path(str(plan_path) + ".applied")
        assert applied_marker.exists(), (
            f"L4: successful apply must rename plan to .applied.  "
            f"Expected {applied_marker.name} to exist."
        )
        assert not plan_path.exists(), (
            f"L4: original plan file must NOT survive after a "
            f"successful apply.  Got {plan_path.name} still present."
        )

    def test_apply_leaves_plan_file_unrenamed_on_execution_failure(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """A4 companion (L5): if ``execute_plan`` raises (or returns a
        non-applied status), the plan file is NOT renamed — the
        operator can inspect, fix the underlying issue, and retry the
        same plan file.

        Pin: after a failing apply, the bare ``.json`` still exists at
        its original name and there is NO ``.applied`` sibling.

        Expected on ``main``: passes vacuously today (no rename logic
        exists), but Phase 1 must preserve this contract while adding
        the success-path rename.
        """
        from pathlib import Path

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        plan_path = plans_dir / "feature_plan_20260507T120000.json"
        plan_path.write_text(self._make_plan_json())

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"
        # Make execute_plan raise to simulate a partial failure mid-apply.
        mock_decl.execute_plan.side_effect = RuntimeError("simulated failure")

        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        try:
            mgr.apply(
                input_files=["specs.yaml"],
                config=None,
                dev_mode=False,
                overwrite=False,
                allow_recreate=False,
            )
        except RuntimeError:
            pass  # expected — execute_plan raised
        finally:
            mock_decl.execute_plan.side_effect = None

        applied_marker = Path(str(plan_path) + ".applied")
        assert plan_path.exists(), (
            f"L5: failed apply must leave plan file unrenamed for retry.  "
            f"Expected {plan_path.name} to still exist."
        )
        assert not applied_marker.exists(), (
            f"L5: failed apply must NOT mark plan as .applied.  "
            f"Got unexpected {applied_marker.name}."
        )

    # --- A4 / A4 companion: refused-status (apply-time allow_recreate gate) ---

    def test_apply_destructive_plan_without_flag_returns_refused_and_keeps_plan_unrenamed(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """Refused-status companion to L5 (Mark-Failed-Stays-Unapplied):
        when ``decl_api.execute_plan`` returns
        ``ApplyResult(status="refused", ...)`` because the plan
        carried at least one ``destructive=True`` op and the operator
        omitted ``--allow-recreate``, the manager must:

        1. Surface ``status="refused"`` to the caller (so the CLI
           ``Status:`` header renders ``refused``).
        2. Forward the ``--allow-recreate`` directive in ``errors``.
        3. NOT rename the plan file to ``.applied`` — the operator
           needs the same ``feature_plan_*.json`` available to retry
           with ``--allow-recreate``.

        This pins the BUG_BASH §14 fix: today's main consumes the
        plan and marks it ``.applied`` even on a destructive plain
        apply (because ``imperative_executor.execute_plan`` runs
        destructive ops unconditionally), so the next
        ``apply --allow-recreate`` discovers no unapplied plan and
        reports ``Status: no_plan``.

        See plans/apply_allow_recreate_destructive_gate_*.plan.md.
        """
        from pathlib import Path

        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        plan_path = plans_dir / "feature_plan_20260507T120000.json"
        plan_path.write_text(self._make_plan_json())

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        # Wire ``execute_plan`` to mimic the post-fix executor's refuse
        # response for a destructive plan with ``allow_recreate=False``.
        refused_result = mock.MagicMock()
        refused_result.status = "refused"
        refused_result.ops = [
            {
                "operation": "RECREATE_FV",
                "name": "USER_CLICK_STATS_DECL",
                "reason": "Full-spec change detected (UDF source).",
                "destructive": True,
                "status": "refused",
            }
        ]
        refused_result.warnings = []
        refused_result.errors = [
            "Apply refused: 1 destructive operation(s) require "
            "--allow-recreate. Re-run with `snow feature apply "
            "--allow-recreate <path>`."
        ]
        mock_decl.execute_plan.return_value = refused_result

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        assert result.get("status") == "refused", (
            f"Refused execute_plan status must be threaded straight to "
            f"the caller.  Got status={result.get('status')!r}."
        )
        joined_errors = " ".join(result.get("errors", []))
        assert "--allow-recreate" in joined_errors, (
            f"Refused error must direct operator at --allow-recreate.  "
            f"Got errors={result.get('errors')!r}."
        )

        # L5 invariant for the refused branch: plan stays unrenamed.
        applied_marker = Path(str(plan_path) + ".applied")
        assert plan_path.exists(), (
            f"Refused apply must leave the plan file unrenamed so the "
            f"operator can retry with --allow-recreate.  Expected "
            f"{plan_path.name} to still exist."
        )
        assert not applied_marker.exists(), (
            f"Refused apply must NOT mark plan as .applied.  "
            f"Got unexpected {applied_marker.name}."
        )

        # Plan file path threaded back unchanged for the operator.
        assert result.get("plan_file") == str(plan_path)

    def test_apply_then_apply_with_allow_recreate_consumes_same_plan_file(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """End-to-end pin for the BUG_BASH §14 retry workflow: a plain
        ``apply`` that refuses leaves the plan file in place, and a
        follow-up ``apply --allow-recreate`` consumes the SAME file
        (L4 rename to ``.applied`` only on success).

        This is the test that would have caught the original cascade
        before it shipped: today on ``main`` the first apply silently
        executes the destructive op, renames the plan ``.applied``,
        and the second call with ``--allow-recreate`` discovers no
        unapplied plan and returns ``no_plan``.
        """
        from pathlib import Path

        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        plan_path = plans_dir / "feature_plan_20260507T120000.json"
        plan_path.write_text(self._make_plan_json())

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        # First call: plain apply → execute_plan returns refused.
        refused_result = mock.MagicMock()
        refused_result.status = "refused"
        refused_result.ops = [
            {
                "operation": "RECREATE_FV",
                "name": "USER_CLICK_STATS_DECL",
                "reason": "Full-spec change detected (UDF source).",
                "destructive": True,
                "status": "refused",
            }
        ]
        refused_result.warnings = []
        refused_result.errors = [
            "Apply refused: 1 destructive operation(s) require " "--allow-recreate."
        ]
        mock_decl.execute_plan.return_value = refused_result

        mgr = FeatureManager()
        first_result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        assert first_result.get("status") == "refused"
        assert plan_path.exists(), (
            "After plain apply with refused status, the plan file "
            "must remain at its original .json path so the operator "
            "can retry with --allow-recreate."
        )

        # Second call: apply --allow-recreate → execute_plan returns
        # applied.  The manager must re-discover the SAME plan file
        # (L1: Required-Plan + L2: Latest-Wins; only one unapplied
        # plan in the directory) and rename it to ``.applied`` (L4).
        applied_result = mock.MagicMock()
        applied_result.status = "applied"
        applied_result.ops = [
            {
                "operation": "RECREATE_FV",
                "name": "USER_CLICK_STATS_DECL",
                "status": "success",
            }
        ]
        applied_result.warnings = []
        applied_result.errors = []
        mock_decl.execute_plan.return_value = applied_result
        mock_decl.execute_plan.reset_mock()

        # Reset deserialize_plan call args between calls so we can
        # inspect the second call's path independently.
        mock_decl.deserialize_plan.reset_mock()

        second_result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=True,
        )

        assert second_result.get("status") == "applied"
        # The deserialize_plan call must have been driven by the same
        # plan-file content (we only have one plan file in the dir).
        mock_decl.deserialize_plan.assert_called_once()
        # L4: the consumed plan is renamed .applied on success.
        applied_marker = Path(str(plan_path) + ".applied")
        assert applied_marker.exists(), (
            "Successful apply --allow-recreate must rename the same "
            "plan file to .applied (L4)."
        )
        assert (
            not plan_path.exists()
        ), "L4: original plan must NOT survive a successful retry."
        assert second_result.get("plan_file") == str(applied_marker)

    # --- A5: L6 — Target-Match ---

    def test_apply_rejects_plan_with_target_database_mismatch(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """A5 (L6): ``apply`` must abort with a clear error if the
        plan's ``target_database`` does not match the active
        connection's database.  This prevents accidentally applying a
        plan generated for store A against store B.

        Pin: with a plan carrying ``target_database="OTHER_DB"`` and
        ``mock_cli_context`` set to ``database="TEST_DB"``, apply
        returns ``status="target_mismatch"`` whose error message names
        both sides.  ``execute_plan`` is NOT called.  The plan file
        stays unapplied so the operator can retry against the right
        connection.

        Expected on ``main``: no target-match check; apply proceeds
        and ``execute_plan`` IS called.  Test fails.
        """
        from pathlib import Path

        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        plan_path = plans_dir / "feature_plan_20260507T120000.json"
        plan_path.write_text(
            self._make_plan_json(
                target_database="OTHER_DB", target_schema="TEST_SCHEMA"
            )
        )

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "OTHER_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        assert result.get("status") == "target_mismatch", (
            f"L6: target_database mismatch must yield "
            f"status='target_mismatch'.  Got status={result.get('status')!r}."
        )
        errors = result.get("errors", [])
        joined = " ".join(errors)
        assert "OTHER_DB" in joined and "TEST_DB" in joined, (
            f"L6: target-mismatch error must name both sides "
            f"(OTHER_DB / TEST_DB).  Got: {errors!r}"
        )
        mock_decl.execute_plan.assert_not_called()
        # Plan file stays unapplied so the operator can retry.
        applied_marker = Path(str(plan_path) + ".applied")
        assert plan_path.exists()
        assert not applied_marker.exists()

    def test_apply_rejects_plan_with_target_schema_mismatch(
        self, mock_execute_query, mock_decl, tmp_path, monkeypatch
    ):
        """A5 companion (L6): same as the database mismatch test, but
        the schema differs while the database matches.  Both halves
        of the (database, schema) pair must agree."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        plans_dir = self._make_plans_dir(tmp_path)

        plan_path = plans_dir / "feature_plan_20260507T120000.json"
        plan_path.write_text(
            self._make_plan_json(
                target_database="TEST_DB", target_schema="OTHER_SCHEMA"
            )
        )

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "OTHER_SCHEMA"

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )

        assert result.get("status") == "target_mismatch", (
            f"L6: target_schema mismatch must yield "
            f"status='target_mismatch'.  Got {result.get('status')!r}."
        )
        joined = " ".join(result.get("errors", []))
        assert "OTHER_SCHEMA" in joined and "TEST_SCHEMA" in joined, (
            f"L6: schema-mismatch error must name both schemas.  "
            f"Got: {result.get('errors')!r}"
        )
        mock_decl.execute_plan.assert_not_called()


class TestFeatureManagerIngest:
    """``FeatureManager.ingest`` must delegate to
    ``FeatureStore.stream_ingest`` (snowml-core) instead of building a
    REST envelope and POSTing through ``decl_api.post_service_json``.

    These tests pin the new contract so the GREEN refactor cannot
    silently regress the wire path or the result-envelope shape.
    """

    def _patch_fs(self, accepted: int = 1):
        """Return a context manager that patches ``_get_feature_store``
        on the class to yield a MagicMock with ``stream_ingest``
        pre-wired to return ``accepted`` and ``get_stream_source``
        pre-wired with a permissive single-column ``col_a`` schema
        that matches every legacy ``TestFeatureManagerIngest`` record
        shape.

        ``create=True`` lets the tests run before the GREEN commit
        adds the helper (RED state) — the patch attaches a class-level
        attribute regardless of whether the underlying method exists.

        Tests that need a different schema (e.g. the new preflight
        tests below) overwrite ``mock_fs.get_stream_source.return_value``
        directly after the call.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_fs = mock.MagicMock(name="feature_store")
        mock_fs.stream_ingest.return_value = accepted
        # Default schema: a single ``col_a`` column.  The preflight
        # in ``FeatureManager.ingest`` reads this and short-circuits
        # without raising for the legacy tests' ``{"col_a": …}``
        # records.
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
        self, mock_execute_query, mock_decl, monkeypatch
    ):
        """``ingest(source, records)`` must invoke
        ``FeatureStore.stream_ingest(source, records)`` exactly once
        with the verbatim arguments the caller supplied — no
        envelope-shape rewriting at the manager layer."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs(accepted=3)

        with patcher:
            mgr = FeatureManager()
            mgr.ingest("MY_STREAM_SOURCE", [{"col_a": 1}, {"col_a": 2}])

        mock_fs.stream_ingest.assert_called_once_with(
            "MY_STREAM_SOURCE", [{"col_a": 1}, {"col_a": 2}]
        )

    def test_ingest_returns_accepted_count_envelope(
        self, mock_execute_query, mock_decl, monkeypatch
    ):
        """Result envelope is ``{**target_info, "accepted_count": <int>}``.

        The chosen shape surfaces snowml-core's raw return value
        (``int``) instead of synthesising a fake ``status: success``
        field.  The verifier script (Phase 5) is updated to match.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, _ = self._patch_fs(accepted=100)

        with patcher:
            mgr = FeatureManager()
            result = mgr.ingest("MY_STREAM_SOURCE", [{"col_a": 1}])

        assert result.get("accepted_count") == 100, (
            f"GREEN must return {{'accepted_count': 100, ...}}; " f"got {result!r}"
        )
        # Target-info envelope is preserved (apply / list / describe
        # results all include this triple — ingest is no exception).
        assert result.get("target_database") == "TEST_DB"
        assert result.get("target_schema") == "TEST_SCHEMA"
        assert result.get("target_warehouse") == "TEST_WH"

    def test_ingest_surfaces_runtime_error_when_pat_missing(
        self, mock_execute_query, mock_decl, monkeypatch
    ):
        """When ``stream_ingest`` raises (snowml-core's PAT enforcement
        path, or any other transport failure), the manager re-raises
        unchanged.

        The CLI's previous local PAT preflight is removed in GREEN —
        snowml-core owns the diagnosis.  This test pins that the
        manager does not swallow snowml-core errors into a status
        dict.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs()
        mock_fs.stream_ingest.side_effect = RuntimeError(
            "SNOWFLAKE_PAT environment variable is required."
        )

        with patcher:
            mgr = FeatureManager()
            with pytest.raises(RuntimeError, match="SNOWFLAKE_PAT"):
                mgr.ingest("MY_STREAM_SOURCE", [{"col_a": 1}])

    # ------------------------------------------------------------------
    # Preflight contract — pin the new client-side schema validation
    # in ``FeatureManager.ingest``.  The preflight inspects
    # ``fs.get_stream_source(name).schema.fields[].name`` and rejects
    # any record whose key set diverges from the registered schema,
    # ``before`` ``fs.stream_ingest`` is invoked.  These tests pin:
    #
    #   * named missing/extra fields surface in the error message,
    #   * ``fs.stream_ingest`` is *never* called on bad input,
    #   * exact-match payloads pass through unchanged,
    #   * per-record errors are aggregated (so the operator sees every
    #     bad row in one shot, not just the first).
    #
    # See [docs/BUG_BASH.md] step 7 — the operator-facing error today
    # is a server-side 207 ``MISSING_REQUIRED_FIELD``; the preflight
    # converts that into a deterministic client-side ``ValueError``
    # naming the columns and the record indices, with zero HTTP
    # traffic.
    # ------------------------------------------------------------------

    @staticmethod
    def _stream_source_with_schema(field_names: list[str]) -> mock.MagicMock:
        """Build a mock ``StreamSource`` whose ``schema.fields[].name``
        sequence matches ``field_names`` exactly.

        The preflight reads ``src.schema.fields`` and picks ``.name``
        off each entry; mirroring snowml-core's
        ``_stream_source_schema_field_names`` helper keeps the test
        contract aligned with the real wire shape.
        """
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

    _CLICKSTREAM_6_COL = [
        "USER_ID",
        "SESSION_ID",
        "PAGE_URL",
        "EVENT_TYPE",
        "TIMESTAMP",
        "TIME_ON_PAGE_SECONDS",
    ]

    def test_ingest_preflight_rejects_records_missing_required_fields(
        self, mock_execute_query, mock_decl, monkeypatch
    ):
        """A record missing a registered column (``PAGE_URL``) must
        raise ``ValueError`` whose message names both the missing
        column AND the offending record index — and ``stream_ingest``
        must never be called (zero HTTP traffic on bad input)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs(accepted=0)
        mock_fs.get_stream_source.return_value = self._stream_source_with_schema(
            self._CLICKSTREAM_6_COL
        )

        with patcher:
            mgr = FeatureManager()
            with pytest.raises(ValueError) as excinfo:
                mgr.ingest(
                    "CLICKSTREAM_EVENTS",
                    [
                        {
                            "USER_ID": "u1",
                            "SESSION_ID": "s1",
                            "EVENT_TYPE": "page_view",
                            "TIMESTAMP": "2026-05-07T18:00:00Z",
                            "TIME_ON_PAGE_SECONDS": 12,
                        },
                    ],
                )

        msg = str(excinfo.value)
        assert (
            "PAGE_URL" in msg
        ), f"Preflight error must name the missing column.  Got: {msg!r}"
        assert "0" in msg, (
            f"Preflight error must name the offending record index "
            f"(record 0 was bad).  Got: {msg!r}"
        )
        mock_fs.stream_ingest.assert_not_called()

    def test_ingest_preflight_rejects_records_with_extra_fields(
        self, mock_execute_query, mock_decl, monkeypatch
    ):
        """Records with extra keys (``IP_ADDRESS``) the registered
        schema does not declare must also be rejected before any HTTP
        call — extras are just as much a footgun as missing fields
        because the server will silently drop or 400 on them."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs(accepted=0)
        mock_fs.get_stream_source.return_value = self._stream_source_with_schema(
            self._CLICKSTREAM_6_COL
        )

        with patcher:
            mgr = FeatureManager()
            with pytest.raises(ValueError) as excinfo:
                mgr.ingest(
                    "CLICKSTREAM_EVENTS",
                    [
                        {
                            "USER_ID": "u1",
                            "SESSION_ID": "s1",
                            "PAGE_URL": "https://example.com/p/1",
                            "EVENT_TYPE": "page_view",
                            "TIMESTAMP": "2026-05-07T18:00:00Z",
                            "TIME_ON_PAGE_SECONDS": 12,
                            "IP_ADDRESS": "10.0.0.1",
                        },
                    ],
                )

        msg = str(excinfo.value)
        assert (
            "IP_ADDRESS" in msg
        ), f"Preflight error must name the extra column.  Got: {msg!r}"
        mock_fs.stream_ingest.assert_not_called()

    def test_ingest_preflight_passes_when_keys_exactly_match(
        self, mock_execute_query, mock_decl, monkeypatch
    ):
        """Records whose key set exactly equals the registered schema
        must pass straight through to ``fs.stream_ingest`` unchanged
        and the manager must shape the standard
        ``{**target_info, accepted_count: <int>}`` envelope.

        This is the happy-path contract that step 7 of the bug bash
        exercises after the GREEN reconcile lands.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs(accepted=2)
        mock_fs.get_stream_source.return_value = self._stream_source_with_schema(
            self._CLICKSTREAM_6_COL
        )

        records = [
            {
                "USER_ID": "u1",
                "SESSION_ID": "s1",
                "PAGE_URL": "https://example.com/p/1",
                "EVENT_TYPE": "page_view",
                "TIMESTAMP": "2026-05-07T18:00:00Z",
                "TIME_ON_PAGE_SECONDS": 12,
            },
            {
                "USER_ID": "u2",
                "SESSION_ID": "s2",
                "PAGE_URL": "https://example.com/p/2",
                "EVENT_TYPE": "click",
                "TIMESTAMP": "2026-05-07T18:00:30Z",
                "TIME_ON_PAGE_SECONDS": 5,
            },
        ]

        with patcher:
            mgr = FeatureManager()
            result = mgr.ingest("CLICKSTREAM_EVENTS", records)

        mock_fs.stream_ingest.assert_called_once_with("CLICKSTREAM_EVENTS", records)
        assert (
            result.get("accepted_count") == 2
        ), f"Happy-path envelope must surface accepted_count.  Got: {result!r}"
        assert result.get("target_database") == "TEST_DB"
        assert result.get("target_schema") == "TEST_SCHEMA"
        assert result.get("target_warehouse") == "TEST_WH"

    def test_ingest_preflight_aggregates_per_record_errors(
        self, mock_execute_query, mock_decl, monkeypatch
    ):
        """Multiple bad records must be reported in one
        ``ValueError`` — the operator sees every divergent row and
        the named columns in a single shot, not record-by-record on
        successive runs.

        Pins record indices 0 and 2 in the message; record 1 is
        valid and must not appear as a finding.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        patcher, mock_fs = self._patch_fs(accepted=0)
        mock_fs.get_stream_source.return_value = self._stream_source_with_schema(
            self._CLICKSTREAM_6_COL
        )

        records = [
            {
                "USER_ID": "u1",
                "SESSION_ID": "s1",
                "EVENT_TYPE": "page_view",
                "TIMESTAMP": "2026-05-07T18:00:00Z",
                "TIME_ON_PAGE_SECONDS": 12,
            },
            {
                "USER_ID": "u2",
                "SESSION_ID": "s2",
                "PAGE_URL": "https://example.com/p/2",
                "EVENT_TYPE": "click",
                "TIMESTAMP": "2026-05-07T18:00:30Z",
                "TIME_ON_PAGE_SECONDS": 5,
            },
            {
                "USER_ID": "u3",
                "SESSION_ID": "s3",
                "EVENT_TYPE": "purchase",
                "TIMESTAMP": "2026-05-07T18:01:00Z",
                "TIME_ON_PAGE_SECONDS": 30,
            },
        ]

        with patcher:
            mgr = FeatureManager()
            with pytest.raises(ValueError) as excinfo:
                mgr.ingest("CLICKSTREAM_EVENTS", records)

        msg = str(excinfo.value)
        assert (
            "PAGE_URL" in msg
        ), f"Aggregated error must name the missing column.  Got: {msg!r}"
        # Both bad indices must appear; the valid record (index 1)
        # must NOT be flagged.
        assert "record 0" in msg, f"Aggregated error must flag record 0.  Got: {msg!r}"
        assert "record 2" in msg, f"Aggregated error must flag record 2.  Got: {msg!r}"
        assert (
            "record 1" not in msg
        ), f"Aggregated error must NOT flag the valid record 1.  Got: {msg!r}"
        mock_fs.stream_ingest.assert_not_called()


class TestFeatureManagerQuery:
    """``FeatureManager.query`` must delegate to
    ``FeatureStore.read_feature_view`` (snowml-core) with positional
    keys derived from the FV's join-key order.

    These tests pin the GREEN-phase contract:

    1. ``query`` requires both ``feature_view_name`` AND ``version``
       because snowml-core's ``get_feature_view(name, version)``
       requires both (no "latest" semantics for string names).
    2. Dict-shaped CLI keys are translated to positional list-of-list
       in the FV's declared join-key order.
    3. The pandas DataFrame returned by snowml-core is rendered as
       ``{"rows": [...]}`` via ``df.to_dict("records")``.
    4. Missing join-key columns surface a clear ``ValueError`` —
       not a Snowpark error from a wrong-shape positional list.
    """

    def _make_mock_fv(self, entities_join_keys: list[list[str]]) -> "mock.MagicMock":
        """Build a MagicMock FeatureView with ``.entities`` configured
        to expose the requested join-key layout.

        Each inner list represents one Entity's ``join_keys``.  Stored
        as plain strings so ``str(jk)`` round-trips unchanged (matches
        snowml-core's ``Entity.join_keys: list[SqlIdentifier]`` since
        ``SqlIdentifier.__str__`` returns the identifier string).
        """
        fv = mock.MagicMock(name="feature_view")
        fv.entities = []
        for jk_list in entities_join_keys:
            ent = mock.MagicMock(name="entity")
            ent.join_keys = jk_list
            fv.entities.append(ent)
        return fv

    def _patch_fs(
        self,
        join_keys_per_entity: list[list[str]],
        rows_records: list[dict] | None = None,
    ):
        """Return (patcher, mock_fs) prepped for query tests.

        ``join_keys_per_entity`` controls the FV's entity layout.
        ``rows_records`` becomes the pandas DataFrame returned from
        ``fs.read_feature_view``; defaults to a single trivial row.
        """
        import pandas as pd
        from snowflake.cli._plugins.feature.manager import FeatureManager

        if rows_records is None:
            rows_records = [{"USER_ID": "u1", "FEATURE_X": 42}]

        mock_fv = self._make_mock_fv(join_keys_per_entity)
        mock_fs = mock.MagicMock(name="feature_store")
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
        self, mock_execute_query, mock_decl
    ):
        """``query(name, version, keys)`` must invoke
        ``fs.get_feature_view(name, version)`` with both arguments
        verbatim — snowml-core has no "latest version" lookup for
        string names.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        patcher, mock_fs = self._patch_fs([["USER_ID"]])
        with patcher:
            mgr = FeatureManager()
            mgr.query("USER_CLICK_STATS_DECL", "V1", [{"USER_ID": "u1"}])

        mock_fs.get_feature_view.assert_called_once_with("USER_CLICK_STATS_DECL", "V1")

    def test_query_calls_read_feature_view_with_positional_keys(
        self, mock_execute_query, mock_decl
    ):
        """Single-entity FV: input ``[{"USER_ID": "u1"}]`` is
        translated to ``keys=[["u1"]]`` and ``read_feature_view`` is
        called with ``store_type="ONLINE"`` and ``as_pandas=True``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        patcher, mock_fs = self._patch_fs([["USER_ID"]])
        with patcher:
            mgr = FeatureManager()
            mgr.query("FV_NAME", "V1", [{"USER_ID": "u1"}])

        mock_fs.read_feature_view.assert_called_once()
        call = mock_fs.read_feature_view.call_args
        # First positional is the FV object returned by get_feature_view.
        assert call.args[0] is mock_fs.get_feature_view.return_value
        assert call.kwargs.get("keys") == [["u1"]], (
            f"single-entity dict must translate to positional list-of-list; "
            f"got {call.kwargs.get('keys')!r}"
        )
        # ``ONLINE`` is what snowml-core's StoreType enum accepts as a
        # string.  ``as_pandas=True`` forces the pandas-DataFrame path.
        assert call.kwargs.get("store_type") == "ONLINE"
        assert call.kwargs.get("as_pandas") is True

    def test_query_translates_multi_entity_keys_in_join_key_order(
        self, mock_execute_query, mock_decl
    ):
        """Multi-entity FV with entities=[USER (USER_ID),
        SESSION (SESSION_ID)] must translate
        ``[{"USER_ID": "u1", "SESSION_ID": "s1"}]`` to
        ``[["u1", "s1"]]`` (order from the FV's join_keys, NOT the
        dict's iteration order)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        patcher, mock_fs = self._patch_fs(
            [["USER_ID"], ["SESSION_ID"]],
            rows_records=[{"USER_ID": "u1", "SESSION_ID": "s1", "FEATURE_X": 1}],
        )
        with patcher:
            mgr = FeatureManager()
            # Provide the dict in the *opposite* order from the FV's
            # entity declaration to prove the translator uses the FV
            # order, not insertion order.
            mgr.query("FV", "V1", [{"SESSION_ID": "s1", "USER_ID": "u1"}])

        call = mock_fs.read_feature_view.call_args
        assert call.kwargs.get("keys") == [["u1", "s1"]], (
            "multi-entity translation must follow fv.entities[*].join_keys "
            "order (USER_ID, SESSION_ID), regardless of input dict order; "
            f"got {call.kwargs.get('keys')!r}"
        )

    def test_query_returns_rows_envelope(self, mock_execute_query, mock_decl):
        """Result envelope is ``{**target_info, "rows": [...]}`` with
        ``rows`` being ``df.to_dict("records")`` of the pandas
        DataFrame returned from ``read_feature_view``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        rows = [
            {"USER_ID": "u1", "TOTAL_ENGAGEMENT_1H": 5},
            {"USER_ID": "u2", "TOTAL_ENGAGEMENT_1H": 7},
        ]
        patcher, _ = self._patch_fs([["USER_ID"]], rows_records=rows)
        with patcher:
            mgr = FeatureManager()
            result = mgr.query("FV", "V1", [{"USER_ID": "u1"}, {"USER_ID": "u2"}])

        assert (
            result.get("rows") == rows
        ), f"GREEN must return df.to_dict('records'); got {result.get('rows')!r}"
        assert result.get("target_database") == "TEST_DB"
        assert result.get("target_schema") == "TEST_SCHEMA"
        assert result.get("target_warehouse") == "TEST_WH"

    def test_query_raises_clear_error_for_unknown_key_columns(
        self, mock_execute_query, mock_decl
    ):
        """Input dict missing a declared join-key surfaces a
        ``ValueError`` naming the missing column.  The translator must
        fail loudly *before* calling ``read_feature_view`` so users
        get a precise diagnostic instead of a Snowpark "wrong tuple
        length" error from the wire.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        patcher, mock_fs = self._patch_fs(
            [["USER_ID"], ["SESSION_ID"]],
        )
        with patcher:
            mgr = FeatureManager()
            with pytest.raises(ValueError, match="SESSION_ID"):
                # Missing SESSION_ID — translator should refuse.
                mgr.query("FV", "V1", [{"USER_ID": "u1"}])

        # And in the failure case, read_feature_view must NOT be called
        # — we want loud-fail, not silent-degrade.
        mock_fs.read_feature_view.assert_not_called()
