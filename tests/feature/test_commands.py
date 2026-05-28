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

"""Tests for ``snow feature`` Typer commands (Phase 3+4 manifest-driven surface).

The CLI surface mirrors DCM (D3 / D5 / D8 in
``MANIFEST_YML_LAYOUT_DECISIONS.md``):

* Every state-driving command takes ``--from <dir>`` (default cwd)
  to locate ``manifest.yml`` and ``--target <name>`` (default
  ``manifest.default_target``).
* ``apply`` is a *pure plan-file consumer*: no positional spec
  paths, no ``--config``, no ``--overwrite``.  Plans are produced
  by ``snow feature plan`` and discovered from
  ``<project_root>/out/plan/`` (or passed via ``--plan <path>``).
* ``plan`` writes its envelope to ``<project_root>/out/plan/`` by
  default, with ``--out`` as the only override.
* ``--variable -D key=value`` is the only template-variable
  surface (``--config`` is gone).
* ``init`` derives the manifest from the live connection and is
  fail-fast on a pre-existing ``manifest.yml`` (no ``--force``).
"""

from pathlib import Path
from unittest import mock

FEATURE_MANAGER = "snowflake.cli._plugins.feature.commands.FeatureManager"


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_apply_no_positional_args_runs_with_defaults(mock_manager, runner):
    """``apply`` accepts zero positional arguments — the new surface
    is fully flag-driven (``--from``, ``--target``, ``--plan``).

    The legacy contract was the opposite: ``apply`` required at
    least one ``INPUT_FILE`` positional and exited with usage code 2
    otherwise.  Phase 3+4 deletes the positional surface entirely
    (D1) so re-running the bare command must succeed and delegate
    to the manager — confirming the positional argument really is
    gone, not just optional.
    """
    mock_manager.return_value.apply.return_value = {
        "status": "no_plan",
        "ops": [],
        "executed": 0,
    }
    result = runner.invoke(["feature", "apply"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.apply.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_apply_passes_from_target_and_plan_flags(mock_manager, runner, tmp_path):
    """``apply --from <dir> --target NAME --plan FILE`` forwards each
    flag on the manager call exactly once, in the new kwarg shape.
    """
    plan_file = tmp_path / "feature_plan.json"
    plan_file.write_text("{}")
    mock_manager.return_value.apply.return_value = {
        "status": "applied",
        "ops": [],
        "executed": 0,
    }
    result = runner.invoke(
        [
            "feature",
            "apply",
            "--from",
            str(tmp_path),
            "--target",
            "PROD",
            "--plan",
            str(plan_file),
        ]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args.kwargs
    assert call_kwargs["from_dir"] == Path(str(tmp_path))
    assert call_kwargs["target_name"] == "PROD"
    assert call_kwargs["plan_file"] == str(plan_file)


@mock.patch(FEATURE_MANAGER)
def test_apply_dev_flag(mock_manager, runner):
    """``apply --dev`` propagates ``dev_mode=True`` to the manager."""
    mock_manager.return_value.apply.return_value = {"status": "applied", "ops": []}
    result = runner.invoke(["feature", "apply", "--dev"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.apply.call_args.kwargs["dev_mode"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_allow_recreate_flag(mock_manager, runner):
    """``apply --allow-recreate`` propagates ``allow_recreate=True``."""
    mock_manager.return_value.apply.return_value = {"status": "applied", "ops": []}
    result = runner.invoke(["feature", "apply", "--allow-recreate"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.apply.call_args.kwargs["allow_recreate"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_rejects_overwrite_flag(mock_manager, runner):
    """``--overwrite`` was removed in Phase 3+4 (D1).

    Rolling back to the legacy "wipe + reapply" semantics is no longer
    possible from the CLI — operators must drop / reapply explicitly.
    Pin the rejection so a future contributor cannot silently
    re-introduce the destructive flag.
    """
    result = runner.invoke(["feature", "apply", "--overwrite"])
    assert result.exit_code != 0, result.output
    assert "--overwrite" in result.output


@mock.patch(FEATURE_MANAGER)
def test_apply_rejects_config_flag(mock_manager, runner):
    """``--config`` was removed in Phase 3+4 (D5) — ``-D key=value``
    is the only template-variable surface now."""
    result = runner.invoke(["feature", "apply", "--config", "vars.yaml"])
    assert result.exit_code != 0, result.output


@mock.patch(FEATURE_MANAGER)
def test_apply_help_shows_dcm_strict_surface(mock_manager, runner):
    """``apply --help`` must surface the new flag set and prove the
    deleted flags are gone.

    Acceptance #8 in the requirement file: ``--from``, ``--target``,
    and ``--variable`` appear on every relevant command's ``--help``.
    The deleted flags (``--overwrite``, ``--config``, ``--dry``)
    must NOT appear or a regression has slipped a legacy code path
    back in.
    """
    result = runner.invoke(["feature", "apply", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--from" in output
    assert "--target" in output
    assert "--variable" in output
    assert "--dev" in output
    assert "--allow-recreate" in output
    assert "--plan" in output
    assert "--dry" not in output
    assert "--overwrite" not in output
    assert "--config" not in output


# ---------------------------------------------------------------------------
# Status: header (apply / plan)
# ---------------------------------------------------------------------------
#
# These tests pin the behaviour added when fixing the bug-bash step-6
# TODO: ``apply succeeded (rc=0) but output missing 'Status: success'``.


def test_print_status_header_emits_status_and_counts(capsys):
    """Helper writes ``Status: <status>  Operations: N (executed: K)`` to stderr."""
    from snowflake.cli._plugins.feature.commands import _print_status_header

    _print_status_header(
        {
            "status": "applied",
            "ops": [
                {"operation": "CREATE_FV", "name": "X", "status": "success"},
                {"operation": "NO_CHANGE", "name": "Y", "status": "skipped"},
            ],
            "executed": 1,
        }
    )
    captured = capsys.readouterr()
    assert "Status: applied" in captured.err
    assert "Operations: 2" in captured.err
    assert "executed: 1" in captured.err
    assert captured.out == ""


def test_print_status_header_empty_ops_still_emits_status(capsys):
    """Empty ops list still emits the header — this is the explicit
    counterpart to the bug-bash TODO: even ``no_plan`` must surface
    a parseable status line on stderr."""
    from snowflake.cli._plugins.feature.commands import _print_status_header

    _print_status_header({"status": "no_plan", "ops": [], "executed": 0})
    captured = capsys.readouterr()
    assert "Status: no_plan" in captured.err
    assert "Operations: 0" in captured.err
    assert "executed: 0" in captured.err


def test_print_status_header_derives_executed_from_ops_when_missing(capsys):
    """When ``executed`` is absent from the result dict, the helper
    falls back to counting ops with ``status == "success"``."""
    from snowflake.cli._plugins.feature.commands import _print_status_header

    _print_status_header(
        {
            "status": "applied",
            "ops": [
                {"status": "success"},
                {"status": "success"},
                {"status": "skipped"},
            ],
        }
    )
    captured = capsys.readouterr()
    assert "Status: applied" in captured.err
    assert "Operations: 3" in captured.err
    assert "executed: 2" in captured.err


def test_print_status_header_silent_when_status_missing(capsys):
    """No ``status`` field → no header.  Guards against a mid-pipeline
    sub-result accidentally polluting stderr."""
    from snowflake.cli._plugins.feature.commands import _print_status_header

    _print_status_header({"ops": [], "executed": 0})
    captured = capsys.readouterr()
    assert "Status:" not in captured.err
    assert captured.out == ""


def test_print_target_header_includes_target_name_when_present(capsys):
    """``_print_target_header`` includes the resolved ``target_name``
    in the rendered header — the target-name surface introduced in
    Phase 3+4 (D3) is what lets operators distinguish multiple
    manifest profiles in a single shell scrollback.
    """
    from snowflake.cli._plugins.feature.commands import _print_target_header

    _print_target_header(
        {
            "target_database": "DB",
            "target_schema": "SCH",
            "target_warehouse": "WH",
            "target_name": "PROD",
        }
    )
    captured = capsys.readouterr()
    assert "Target: PROD @ DB.SCH (warehouse: WH)" in captured.err


def test_print_target_header_falls_back_when_no_target_name(capsys):
    """Pre-target legacy results (no ``target_name`` key) still render
    a sensible header rather than printing ``Target:  @ DB.SCH``."""
    from snowflake.cli._plugins.feature.commands import _print_target_header

    _print_target_header(
        {
            "target_database": "DB",
            "target_schema": "SCH",
            "target_warehouse": "WH",
        }
    )
    captured = capsys.readouterr()
    assert "Target: DB.SCH (warehouse: WH)" in captured.err
    assert "@" not in captured.err


@mock.patch(FEATURE_MANAGER)
def test_apply_calls_print_status_header_on_success(mock_manager, runner):
    """``snow feature apply`` calls ``_print_status_header`` on a
    successful CREATE_FV — the bug-bash step-6 finding.
    """
    mock_manager.return_value.apply.return_value = {
        "status": "applied",
        "ops": [{"operation": "CREATE_FV", "name": "X", "status": "success"}],
        "executed": 1,
    }
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(["feature", "apply"])
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "applied"


@mock.patch(FEATURE_MANAGER)
def test_apply_calls_print_status_header_on_validation_failed(mock_manager, runner):
    """Even on the validation-failed early-return branch, the header
    must fire so the operator sees ``Status: validation_failed``."""
    mock_manager.return_value.apply.return_value = {
        "status": "validation_failed",
        "ops": [],
        "errors": ["VERSION_CONFLICT: ..."],
    }
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(["feature", "apply"])
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "validation_failed"


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_print_status_header_on_success(mock_manager, runner, tmp_path):
    """``snow feature plan`` also fires ``_print_status_header`` so
    its output is symmetric with ``snow feature apply``."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = {
        "status": "ready",
        "ops": [{"operation": "NO_CHANGE", "name": "X"}],
    }
    mock_manager.return_value.write_plan.return_value = str(out_path)
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "ready"


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_print_status_header_on_validation_failed(
    mock_manager, runner, tmp_path
):
    """``plan`` short-circuits on ``validation_failed`` and must fire
    the header before returning."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = {
        "status": "validation_failed",
        "ops": [],
        "errors": ["..."],
    }
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "validation_failed"
    mock_manager.return_value.write_plan.assert_not_called()


def test_ops_result_message_body_no_longer_includes_status_line():
    """The empty-ops summary message must NOT include ``Status:``
    anymore — the header is emitted on stderr by
    ``_print_status_header``; duplicating it on stdout would
    produce two ``Status:`` lines per invocation."""
    from snowflake.cli._plugins.feature.commands import _ops_result

    cmd_result = _ops_result({"status": "applied", "ops": [], "warnings": []})
    rendered = cmd_result.message
    assert "Status:" not in rendered, rendered
    assert "Operations: 0" in rendered, rendered


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_plan_no_positional_args_runs_with_defaults(mock_manager, runner):
    """``plan`` accepts zero positional arguments — D1 deletes the
    legacy ``INPUT_FILES`` surface.  Bare ``snow feature plan`` runs
    against the project rooted at the current working directory.
    """
    mock_manager.return_value.plan.return_value = {"status": "ready", "ops": []}
    mock_manager.return_value.write_plan.return_value = "out/plan/feature_plan_x.json"
    result = runner.invoke(["feature", "plan"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.plan.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_manager_plan(mock_manager, runner):
    """``plan`` delegates to ``FeatureManager.plan`` (validate +
    generate_plan, no SQL).  ``apply`` MUST NOT be called by the
    plan command — the two commands are now disjoint code paths.
    """
    mock_manager.return_value.plan.return_value = {"status": "ready", "ops": []}
    mock_manager.return_value.write_plan.return_value = "out/plan/feature_plan_x.json"
    result = runner.invoke(["feature", "plan"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.plan.assert_called_once()
    mock_manager.return_value.apply.assert_not_called()


@mock.patch(FEATURE_MANAGER)
def test_plan_does_not_write_plan_file_on_validation_failed(
    mock_manager, runner, tmp_path
):
    """``plan`` must NOT write a plan file when ``manager.plan``
    returns ``validation_failed``.  The previous flow wrote the
    plan file *before* running validation, so a failed plan still
    left a stale ``feature_plan_*.json`` on disk that operators
    could mistake for a successful run.  The fix runs validation
    first and short-circuits before ``write_plan`` is invoked.
    """
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = {
        "status": "validation_failed",
        "ops": [],
        "errors": ["VERSION_CONFLICT: ..."],
    }
    result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.write_plan.assert_not_called()
    assert not out_path.exists()


@mock.patch(FEATURE_MANAGER)
def test_plan_writes_plan_file_on_success(mock_manager, runner, tmp_path):
    """``plan`` invokes ``write_plan`` when ``manager.plan`` reports
    a non-failed status."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = {
        "status": "ready",
        "ops": [{"operation": "NO_CHANGE", "name": "x"}],
    }
    mock_manager.return_value.write_plan.return_value = str(out_path)
    result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.write_plan.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_plan_passes_variables_via_dash_d_flag(mock_manager, runner):
    """``-D key=value`` (and the long form ``--variable``) are the
    only template-variable surface (D5).  The list of values is
    forwarded verbatim to the manager so the underlying
    ``decl_api.parse_variables`` sees the same string the operator
    typed.
    """
    mock_manager.return_value.plan.return_value = {"status": "ready", "ops": []}
    mock_manager.return_value.write_plan.return_value = "out/plan/feature_plan_x.json"
    result = runner.invoke(
        ["feature", "plan", "-D", "env=prod", "--variable", "region=us-west-2"]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.plan.call_args.kwargs
    assert call_kwargs["variables"] == ["env=prod", "region=us-west-2"]


@mock.patch(FEATURE_MANAGER)
def test_plan_passes_target_name(mock_manager, runner, tmp_path):
    """``plan --from <dir> --target NAME`` propagates both flags."""
    mock_manager.return_value.plan.return_value = {"status": "ready", "ops": []}
    mock_manager.return_value.write_plan.return_value = "out/plan/feature_plan_x.json"
    result = runner.invoke(
        ["feature", "plan", "--from", str(tmp_path), "--target", "STAGING"]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.plan.call_args.kwargs
    assert call_kwargs["from_dir"] == Path(str(tmp_path))
    assert call_kwargs["target_name"] == "STAGING"


@mock.patch(FEATURE_MANAGER)
def test_plan_help_shows_dcm_strict_surface(mock_manager, runner):
    """``plan --help`` surfaces ``--from`` / ``--target`` /
    ``--variable`` and hides the deleted flags."""
    result = runner.invoke(["feature", "plan", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--from" in output
    assert "--target" in output
    assert "--variable" in output
    assert "--out" in output
    assert "--no-delete" in output
    assert "--overwrite" not in output
    assert "--allow-recreate" not in output
    assert "--config" not in output


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_list_no_files_lists_deployed(mock_manager, runner):
    """Bare ``snow feature list`` calls the manager with the
    default ``--from`` (cwd) and a ``None`` target — the manager
    resolves both from the manifest.
    """
    mock_manager.return_value.list_specs.return_value = {"specs": []}
    result = runner.invoke(["feature", "list"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.list_specs.call_args.kwargs
    assert "from_dir" in call_kwargs
    assert call_kwargs["target_name"] is None


@mock.patch(FEATURE_MANAGER)
def test_list_passes_from_and_target(mock_manager, runner, tmp_path):
    """``list --from <dir> --target NAME`` propagates both flags."""
    mock_manager.return_value.list_specs.return_value = {"specs": []}
    result = runner.invoke(
        ["feature", "list", "--from", str(tmp_path), "--target", "PROD"]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.list_specs.call_args.kwargs
    assert call_kwargs["from_dir"] == Path(str(tmp_path))
    assert call_kwargs["target_name"] == "PROD"


@mock.patch(FEATURE_MANAGER)
def test_list_rejects_positional_arguments(mock_manager, runner):
    """``list`` no longer accepts positional spec paths (D1)."""
    result = runner.invoke(["feature", "list", "my_specs.yaml"])
    assert result.exit_code != 0, result.output


def test_list_table_display_columns_include_type():
    """The `type` column must be present so multi-kind rows can be
    distinguished (FeatureView / Entity / Datasource)."""
    from snowflake.cli._plugins.feature.commands import _TABLE_DISPLAY_COLUMNS

    assert "type" in _TABLE_DISPLAY_COLUMNS


def test_list_table_display_columns_omits_scheduling_state():
    """``scheduling_state`` is intentionally excluded from the table
    display columns: it is duplicated inside the ``details`` cell for
    FeatureView rows and is empty for Entity / Datasource rows, so
    surfacing it as its own column was pure noise."""
    from snowflake.cli._plugins.feature.commands import _TABLE_DISPLAY_COLUMNS

    assert "scheduling_state" not in _TABLE_DISPLAY_COLUMNS


def test_list_table_display_columns_omits_database_and_schema():
    """``database_name`` and ``schema_name`` are uniform across every
    row of a single ``snow feature list`` invocation, so duplicating
    them in every table row was wasted width.  They are now surfaced
    once, above the table, by the ``Database: ... Schema: ...``
    header line printed by ``_print_listing_scope_header``."""
    from snowflake.cli._plugins.feature.commands import _TABLE_DISPLAY_COLUMNS

    assert "database_name" not in _TABLE_DISPLAY_COLUMNS
    assert "schema_name" not in _TABLE_DISPLAY_COLUMNS


def test_listing_scope_uniform_rows_returns_single_value_pair():
    """When every row has the same database_name and schema_name, the
    helper returns those values verbatim so the header can render
    ``Database: <db>  Schema: <sch>``."""
    from snowflake.cli._plugins.feature.commands import _listing_scope

    rows = [
        {"name": "a", "database_name": "JKEW_DB", "schema_name": "JKEW_SCHEMA"},
        {"name": "b", "database_name": "JKEW_DB", "schema_name": "JKEW_SCHEMA"},
        {"name": "c", "database_name": "JKEW_DB", "schema_name": "JKEW_SCHEMA"},
    ]
    assert _listing_scope(rows) == ("JKEW_DB", "JKEW_SCHEMA")


def test_listing_scope_mixed_rows_returns_multiple_marker():
    """When rows disagree on database or schema, the corresponding
    side of the pair becomes ``"(multiple)"`` so the operator knows
    the table spans more than one scope."""
    from snowflake.cli._plugins.feature.commands import _listing_scope

    mixed_db = [
        {"database_name": "DB_A", "schema_name": "SCH"},
        {"database_name": "DB_B", "schema_name": "SCH"},
    ]
    assert _listing_scope(mixed_db) == ("(multiple)", "SCH")

    mixed_schema = [
        {"database_name": "DB", "schema_name": "SCH_A"},
        {"database_name": "DB", "schema_name": "SCH_B"},
    ]
    assert _listing_scope(mixed_schema) == ("DB", "(multiple)")


def test_listing_scope_returns_none_for_empty_or_unscoped():
    """Empty inputs (no rows) and rows that lack both database_name
    and schema_name signal that no header should be printed."""
    from snowflake.cli._plugins.feature.commands import _listing_scope

    assert _listing_scope([]) is None
    assert _listing_scope([{"name": "a"}, {"name": "b"}]) is None
    assert _listing_scope([{"database_name": "", "schema_name": ""}]) is None


def test_project_columns_aligns_heterogeneous_rows():
    """Every projected row must carry **all** display columns in the
    canonical ``_TABLE_DISPLAY_COLUMNS`` order, with empty strings for
    fields a particular row does not populate."""
    from snowflake.cli._plugins.feature.commands import (
        _TABLE_DISPLAY_COLUMNS,
        _project_columns,
    )

    fv_row = {
        "type": "FeatureView",
        "name": "click_fv",
        "version": "v1",
        "entities": "user_id",
        "database_name": "DB",
        "schema_name": "SCH",
        "created_on": "2024-01-01",
        "scheduling_state": "ACTIVE",
        "details": {"scheduling_state": "ACTIVE"},
    }
    entity_row = {
        "type": "Entity",
        "name": "user_id",
        "entities": "USER_ID",
        "database_name": "DB",
        "schema_name": "SCH",
        "details": {
            "join_keys": ["USER_ID"],
            "comment": "User identity entity",
        },
    }
    datasource_row = {
        "type": "Datasource",
        "name": "click_events_offline",
        "database_name": "DB",
        "schema_name": "SCH",
        "details": {"source_type": "OfflineTable", "column_count": 7},
    }

    projected = _project_columns([fv_row, entity_row, datasource_row])

    assert len(projected) == 3
    for row in projected:
        assert list(row.keys()) == _TABLE_DISPLAY_COLUMNS, (
            f"Expected canonical column order {_TABLE_DISPLAY_COLUMNS}, "
            f"got {list(row.keys())}"
        )
        assert "scheduling_state" not in row
        assert "database_name" not in row
        assert "schema_name" not in row

    fv_proj, entity_proj, ds_proj = projected

    assert fv_proj["type"] == "FeatureView"
    assert fv_proj["name"] == "click_fv"
    assert fv_proj["version"] == "v1"
    assert fv_proj["entities"] == "user_id"
    assert fv_proj["created_on"] == "2024-01-01"
    assert fv_proj["details"] == {"scheduling_state": "ACTIVE"}

    assert entity_proj["type"] == "Entity"
    assert entity_proj["name"] == "user_id"
    assert entity_proj["entities"] == "USER_ID"
    assert entity_proj["version"] == ""
    assert entity_proj["created_on"] == ""
    assert entity_proj["details"] == {
        "join_keys": ["USER_ID"],
        "comment": "User identity entity",
    }

    assert ds_proj["type"] == "OfflineTable"
    assert ds_proj["name"] == "click_events_offline"
    assert ds_proj["entities"] == ""
    assert ds_proj["version"] == ""
    assert ds_proj["created_on"] == ""
    assert ds_proj["details"] == {"source_type": "OfflineTable", "column_count": 7}


def test_project_columns_surfaces_datasource_source_type_in_type_column():
    """Datasource rows surface ``details.source_type`` in the rendered
    ``type`` column instead of the generic ``Datasource`` label.
    """
    from snowflake.cli._plugins.feature.commands import _project_columns

    stream_row = {
        "type": "Datasource",
        "name": "clickstream_events",
        "details": {"source_type": "Stream", "column_count": 6},
    }
    offline_row = {
        "type": "Datasource",
        "name": "click_events_offline",
        "details": {"source_type": "OfflineTable", "column_count": 7},
    }
    no_source_type_row = {
        "type": "Datasource",
        "name": "legacy_unknown",
        "details": {"column_count": 0},
    }
    no_details_row = {
        "type": "Datasource",
        "name": "legacy_no_details",
    }

    stream_proj, offline_proj, no_st_proj, no_det_proj = _project_columns(
        [stream_row, offline_row, no_source_type_row, no_details_row]
    )

    assert stream_proj["type"] == "Stream"
    assert offline_proj["type"] == "OfflineTable"
    assert no_st_proj["type"] == "Datasource"
    assert no_det_proj["type"] == "Datasource"

    fv_row = {"type": "StreamingFeatureView", "name": "x"}
    entity_row = {"type": "Entity", "name": "user_id"}
    fv_proj, ent_proj = _project_columns([fv_row, entity_row])
    assert fv_proj["type"] == "StreamingFeatureView"
    assert ent_proj["type"] == "Entity"


def test_project_columns_empty_input_returns_empty():
    from snowflake.cli._plugins.feature.commands import _project_columns

    assert _project_columns([]) == []


@mock.patch(FEATURE_MANAGER)
def test_list_renders_multi_kind_rows(mock_manager, runner):
    """The table output should accept rows of all three kinds with a type column."""
    mock_manager.return_value.list_specs.return_value = {
        "source": "snowflake",
        "specs": [
            {
                "type": "FeatureView",
                "name": "click_fv",
                "version": "v1",
                "entities": "user_id",
                "database_name": "DB",
                "schema_name": "SCH",
                "scheduling_state": "ACTIVE",
                "created_on": "2024-01-01",
            },
            {
                "type": "Entity",
                "name": "user",
                "version": "",
                "entities": "USER_ID",
                "database_name": "DB",
                "schema_name": "SCH",
            },
            {
                "type": "Datasource",
                "name": "user_events",
                "version": "",
                "entities": "",
                "database_name": "DB",
                "schema_name": "SCH",
            },
        ],
    }
    result = runner.invoke(["feature", "list"])
    assert result.exit_code == 0, result.output
    assert "Entity" in result.output
    assert "Dataso" in result.output  # Datasource wraps as "Dataso\nurce"
    assert "Featur" in result.output  # FeatureView wraps as "Featur\neView"
    assert "click_" in result.output
    assert "user_e" in result.output
    header_block = result.output.split("|--")[0]
    assert "scheduling_state" not in header_block
    assert "database_name" not in header_block
    assert "schema_name" not in header_block
    assert "Database: DB" in result.output
    assert "Schema: SCH" in result.output


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_describe_requires_name(mock_manager, runner):
    """describe with no name should exit with usage error."""
    result = runner.invoke(["feature", "describe"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_describe_passes_name_with_from_and_target(mock_manager, runner, tmp_path):
    """``describe NAME --from <dir> --target NAME`` forwards each
    flag on the manager call exactly once, in the new kwarg shape.
    """
    mock_manager.return_value.describe.return_value = {}
    result = runner.invoke(
        [
            "feature",
            "describe",
            "MY_ENTITY",
            "--from",
            str(tmp_path),
            "--target",
            "PROD",
        ]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.describe.call_args.kwargs
    assert call_kwargs["name"] == "MY_ENTITY"
    assert call_kwargs["from_dir"] == Path(str(tmp_path))
    assert call_kwargs["target_name"] == "PROD"


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_no_flags_returns_status(mock_manager, runner):
    """online-service with no flags should show runtime status."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "compute_pool": {"status": "ACTIVE", "name": "POOL"},
        "postgres": {"status": "READY", "name": "PG"},
        "service": {"status": "RUNNING", "name": "SVC"},
        "endpoints": [],
    }
    result = runner.invoke(["feature", "online-service"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.get_status.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_does_not_render_duplicate_table(mock_manager, runner):
    """The no-flag status branch must not duplicate the rich display as
    a key/value table on stdout.

    Pre-fix the command wrote the rich display to stderr (good) and
    then returned ``ObjectResult(result)`` (bad) — the latter rendered
    as a ``| key | value |`` table containing ``status``, ``message``,
    ``endpoints`` (JSON-encoded), ``created_at`` and ``updated_at``,
    duplicating every field already shown in the rich display. The
    fix returns a no-op ``MessageResult`` for the success path.

    With the runner's default ``mix_stderr=True`` both streams end up
    in ``result.output``; the rich display (stderr) is allowed there,
    but the key/value table markers must not appear.
    """
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "message": "Feature Store Online Service is running",
        "runtime_id": "rt-x",
        "endpoints": [
            {
                "name": "ingest",
                "url": "https://ingest.example.snowflakecomputing.app",
            },
        ],
        "compute_pool": {"status": "ACTIVE", "name": "POOL"},
        "postgres": {"status": "READY", "name": "PG"},
        "service": {"status": "RUNNING", "name": "SVC"},
        "created_at": 1779296785675,
        "updated_at": 1779297371850,
    }
    result = runner.invoke(["feature", "online-service"])
    assert result.exit_code == 0, result.output

    # The rich display banner is the only payload we expect.
    assert "Feature Store — Runtime Status" in result.output

    # The duplicate key/value table is recognisable by the explicit
    # column separators and the raw timestamp values it surfaced.
    assert "| status" not in result.output
    assert "| message" not in result.output
    assert "| created_at" not in result.output
    assert "| updated_at" not in result.output
    # The endpoints column previously rendered the raw list-of-dicts;
    # check the JSON-ish form does not leak through.
    assert "[{'name'" not in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_error_still_returned_as_object(mock_manager, runner):
    """When ``get_status`` returns an ``error`` envelope the command
    must still surface the error on stdout (so JSON-mode and human
    consumers both see it).  Only the success path drops the
    duplicate table.
    """
    mock_manager.return_value.get_status.return_value = {
        "status": "error",
        "error": "Something went wrong",
    }
    result = runner.invoke(["feature", "online-service"])
    assert result.exit_code == 0, result.output
    assert "Something went wrong" in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_already_running_is_noop(mock_manager, runner):
    """online-service --create should be a no-op when status is already RUNNING."""
    mock_manager.return_value.get_status.return_value = {"status": "RUNNING"}
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service already initialized",
    }
    result = runner.invoke(["feature", "online-service", "--create"])
    assert result.exit_code == 0, result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_and_polls(mock_manager, runner):
    """online-service --create should create the runtime and poll until RUNNING."""
    mock_manager.return_value.get_status.return_value = {"status": "STOPPED"}
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service initialized successfully",
    }
    result = runner.invoke(["feature", "online-service", "--create"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.initialize_service.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop(mock_manager, runner):
    """online-service --drop should drop OFTs then call FeatureManager.destroy_service."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "destroyed",
        "dropped_ofts": ["TABLE_A", "TABLE_B"],
    }
    result = runner.invoke(["feature", "online-service", "--drop"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.destroy_service.assert_called_once()


def test_online_service_both_flags_rejected(runner):
    """online-service with both --create and --drop should fail."""
    result = runner.invoke(["feature", "online-service", "--create", "--drop"])
    assert result.exit_code != 0


# --- --from / --target pass-through on online-service ------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_passes_from_and_target_to_manager(
    mock_manager, runner, tmp_path
):
    """``online-service --from DIR --target NAME`` forwards both flags
    to :meth:`FeatureManager.get_status` as kwargs.

    The status path used to be connection-only; the new contract
    threads the manifest target through so different targets can
    address independent online services in the same connection.
    """
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "compute_pool": {"status": "ACTIVE", "name": "POOL"},
        "postgres": {"status": "READY", "name": "PG"},
        "service": {"status": "RUNNING", "name": "SVC"},
        "endpoints": [],
    }
    result = runner.invoke(
        [
            "feature",
            "online-service",
            "--from",
            str(tmp_path),
            "--target",
            "STAGING",
        ]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.get_status.call_args.kwargs
    assert call_kwargs["from_dir"] == Path(str(tmp_path))
    assert call_kwargs["target_name"] == "STAGING"


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_defaults_to_cwd_and_none_target(mock_manager, runner):
    """Bare ``online-service`` (no flags) passes ``from_dir=Path.cwd()``
    and ``target_name=None`` so the manager can resolve the manifest's
    ``default_target`` (or fall back to the connection when no manifest
    is reachable).
    """
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "compute_pool": {"status": "ACTIVE", "name": "POOL"},
        "postgres": {"status": "READY", "name": "PG"},
        "service": {"status": "RUNNING", "name": "SVC"},
        "endpoints": [],
    }
    result = runner.invoke(["feature", "online-service"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.get_status.call_args.kwargs
    assert call_kwargs["from_dir"] == Path.cwd()
    assert call_kwargs["target_name"] is None


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_passes_from_and_target(mock_manager, runner, tmp_path):
    """``--create --from DIR --target NAME`` threads both flags to
    :meth:`FeatureManager.initialize_service`."""
    mock_manager.return_value.get_status.return_value = {"status": "STOPPED"}
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service initialized successfully",
    }
    result = runner.invoke(
        [
            "feature",
            "online-service",
            "--create",
            "--from",
            str(tmp_path),
            "--target",
            "PROD",
        ]
    )
    assert result.exit_code == 0, result.output
    init_kwargs = mock_manager.return_value.initialize_service.call_args.kwargs
    assert init_kwargs["from_dir"] == Path(str(tmp_path))
    assert init_kwargs["target_name"] == "PROD"
    status_kwargs = mock_manager.return_value.get_status.call_args.kwargs
    assert status_kwargs["from_dir"] == Path(str(tmp_path))
    assert status_kwargs["target_name"] == "PROD"


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_passes_from_and_target(mock_manager, runner, tmp_path):
    """``--drop --from DIR --target NAME`` threads both flags to
    :meth:`FeatureManager.destroy_service`."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "destroyed",
        "dropped_ofts": [],
    }
    result = runner.invoke(
        [
            "feature",
            "online-service",
            "--drop",
            "--from",
            str(tmp_path),
            "--target",
            "PROD",
        ]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.destroy_service.call_args.kwargs
    assert call_kwargs["from_dir"] == Path(str(tmp_path))
    assert call_kwargs["target_name"] == "PROD"


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_ingest_requires_source_name(mock_manager, runner):
    """ingest with no arguments should exit with usage error (code 2)."""
    result = runner.invoke(["feature", "ingest"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_ingest_reads_data_from_file(mock_manager, runner, tmp_path):
    """ingest --data <file> should parse JSON and pass records to manager."""
    data_file = tmp_path / "records.json"
    data_file.write_text('[{"user_id": "u1", "val": 42}]')
    mock_manager.return_value.ingest.return_value = {"ingested": 1}
    result = runner.invoke(["feature", "ingest", "my_source", "--data", str(data_file)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.ingest.assert_called_once()
    call_kwargs = mock_manager.return_value.ingest.call_args.kwargs
    assert call_kwargs["source_name"] == "my_source"
    assert call_kwargs["records"] == [{"user_id": "u1", "val": 42}]


@mock.patch(FEATURE_MANAGER)
def test_ingest_reads_from_stdin(mock_manager, runner):
    """ingest without --data (defaults to stdin) should read records from stdin."""
    mock_manager.return_value.ingest.return_value = {"ingested": 2}
    result = runner.invoke(
        ["feature", "ingest", "my_source"],
        input='[{"a": 1}, {"a": 2}]',
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.ingest.call_args.kwargs
    assert len(call_kwargs["records"]) == 2


@mock.patch(FEATURE_MANAGER)
def test_ingest_manager_error_propagates(mock_manager, runner):
    """ingest should propagate RuntimeError from manager (e.g. missing PAT)."""
    mock_manager.return_value.ingest.side_effect = RuntimeError(
        "SNOWFLAKE_PAT environment variable is required"
    )
    result = runner.invoke(
        ["feature", "ingest", "my_source"],
        input="[]",
    )
    assert result.exit_code != 0


@mock.patch(FEATURE_MANAGER)
def test_ingest_help_shows_data_option(mock_manager, runner):
    """ingest --help should show --data option."""
    result = runner.invoke(["feature", "ingest", "--help"])
    assert result.exit_code == 0, result.output
    assert "--data" in result.output


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_query_requires_feature_view_name(mock_manager, runner):
    """query with no arguments should exit with usage error (code 2)."""
    result = runner.invoke(["feature", "query"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_query_requires_keys(mock_manager, runner):
    """query without --keys (but with --version) should exit with usage error (code 2)."""
    result = runner.invoke(["feature", "query", "my_view", "--version", "V1"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_query_requires_version(mock_manager, runner):
    """query without --version should exit with usage error (code 2).

    snowml-core's ``FeatureStore.get_feature_view(name, version)``
    requires both args, so the CLI mirrors that surface — there is
    no "latest version" fallback for a bare name.
    """
    result = runner.invoke(
        ["feature", "query", "my_view", "--keys", '[{"USER_ID": "u1"}]']
    )
    assert result.exit_code == 2, result.output
    assert "--version" in result.output


@mock.patch(FEATURE_MANAGER)
def test_query_calls_manager_with_view_version_and_keys(mock_manager, runner):
    """query should pass feature_view_name, version, and parsed keys to manager."""
    mock_manager.return_value.query.return_value = {"rows": []}
    keys_json = '[{"user_id": "u1"}]'
    result = runner.invoke(
        ["feature", "query", "my_view", "--version", "V1", "--keys", keys_json]
    )
    assert result.exit_code == 0, result.output
    mock_manager.return_value.query.assert_called_once()
    call_kwargs = mock_manager.return_value.query.call_args.kwargs
    assert call_kwargs["feature_view_name"] == "my_view"
    assert call_kwargs["version"] == "V1"
    assert call_kwargs["keys"] == [{"user_id": "u1"}]


@mock.patch(FEATURE_MANAGER)
def test_query_manager_error_propagates(mock_manager, runner):
    """query should propagate RuntimeError from manager (e.g. missing PAT)."""
    mock_manager.return_value.query.side_effect = RuntimeError(
        "SNOWFLAKE_PAT environment variable is required"
    )
    result = runner.invoke(
        [
            "feature",
            "query",
            "my_view",
            "--version",
            "V1",
            "--keys",
            '[{"id": "1"}]',
        ]
    )
    assert result.exit_code != 0


@mock.patch(FEATURE_MANAGER)
def test_query_help_shows_keys_and_version_options(mock_manager, runner):
    """query --help should show both --keys and --version options."""
    result = runner.invoke(["feature", "query", "--help"])
    assert result.exit_code == 0, result.output
    assert "--keys" in result.output
    assert "--version" in result.output


# ---------------------------------------------------------------------------
# init — the unified bootstrap (subsumes the deleted `feature export` cmd)
# ---------------------------------------------------------------------------


_INIT_RESULT_STUB = {
    "status": "initialized",
    "project_root": "/tmp/proj",
    "manifest_path": "/tmp/proj/manifest.yml",
    "target": "DEFAULT",
    "manifest_written": True,
    "export": {"status": "exported", "directory": "", "files": []},
}


@mock.patch(FEATURE_MANAGER)
def test_init_help_lists_new_flags_and_drops_old_ones(mock_manager, runner):
    """``snow feature init --help`` shows the new flags and NOT the old ones."""
    result = runner.invoke(["feature", "init", "--help"])
    assert result.exit_code == 0, result.output
    text = result.output

    # The new init surface — local --target plus the global --database
    # / --schema connection flags (provided by `requires_connection`).
    assert "--target" in text
    assert "--database" in text
    assert "--schema" in text

    # Removed flags.
    assert "--no-scaffold" not in text
    assert "--from" not in text


@mock.patch(FEATURE_MANAGER)
def test_init_calls_manager_with_cwd_project_root(mock_manager, runner):
    """``snow feature init`` passes ``Path.cwd()`` as ``project_root``."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output

    mock_manager.return_value.init.assert_called_once()
    kwargs = mock_manager.return_value.init.call_args.kwargs
    # project_root is mandatory; default = current working directory.
    assert "project_root" in kwargs
    assert isinstance(kwargs["project_root"], Path)
    assert kwargs["project_root"] == Path.cwd()


@mock.patch(FEATURE_MANAGER)
def test_init_no_longer_accepts_no_scaffold_flag(mock_manager, runner):
    """``--no-scaffold`` is removed; passing it must error."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init", "--no-scaffold"])
    assert result.exit_code != 0, result.output
    assert "--no-scaffold" in result.output or "no such option" in result.output.lower()


@mock.patch(FEATURE_MANAGER)
def test_init_no_longer_accepts_from_flag(mock_manager, runner, tmp_path):
    """``--from`` is removed; passing it must error."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init", "--from", str(tmp_path)])
    assert result.exit_code != 0, result.output
    assert "--from" in result.output or "no such option" in result.output.lower()


@mock.patch(FEATURE_MANAGER)
def test_init_target_flag_passes_through(mock_manager, runner):
    """``--target STAGING`` propagates as ``target_name=STAGING``."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init", "--target", "STAGING"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.init.call_args.kwargs["target_name"] == "STAGING"


@mock.patch(FEATURE_MANAGER)
def test_init_default_target_is_none(mock_manager, runner):
    """``--target`` defaults to ``None`` so the manager picks the default."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.init.call_args.kwargs["target_name"] is None


@mock.patch(FEATURE_MANAGER)
def test_init_forwards_database_schema_kwargs(mock_manager, runner):
    """``snow feature init --database X --schema Y`` forwards both flag
    values to ``FeatureManager.init`` as ``database=`` / ``schema=``.

    Previously the Typer command captured the global ``--database`` /
    ``--schema`` flags into ``**options`` and then dropped them, which
    silently ignored the override (manifest was written with the
    connection profile's default schema).  The fix threads both values
    through to the manager kwarg so:

    * Fresh init writes the overrides into the new ``manifest.yml``.
    * Re-init can detect a mismatch against the resolved manifest
      target and raise ``CliError`` (manager-layer concern).
    """
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(
        [
            "feature",
            "init",
            "--database",
            "OVERRIDE_DB",
            "--schema",
            "OVERRIDE_SCHEMA",
        ]
    )
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.init.call_args.kwargs
    assert kwargs["database"] == "OVERRIDE_DB"
    assert kwargs["schema"] == "OVERRIDE_SCHEMA"


@mock.patch(FEATURE_MANAGER)
def test_init_omitted_database_schema_pass_none(mock_manager, runner):
    """When ``--database`` / ``--schema`` are omitted, the kwargs are
    forwarded as ``None`` so the manager falls back to the active
    connection's profile defaults.
    """
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.init.call_args.kwargs
    assert kwargs.get("database") is None
    assert kwargs.get("schema") is None


@mock.patch(FEATURE_MANAGER)
def test_init_target_with_database_schema_forwards_all_three(mock_manager, runner):
    """``--target NAME --database DB --schema SCH`` forwards every value
    on the same manager call (target + db + schema interact on a fresh
    init: target names the manifest target, db/schema populate its
    fields).
    """
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(
        [
            "feature",
            "init",
            "--target",
            "STAGING",
            "--database",
            "OVERRIDE_DB",
            "--schema",
            "OVERRIDE_SCHEMA",
        ]
    )
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.init.call_args.kwargs
    assert kwargs["target_name"] == "STAGING"
    assert kwargs["database"] == "OVERRIDE_DB"
    assert kwargs["schema"] == "OVERRIDE_SCHEMA"


# ---------------------------------------------------------------------------
# export — command no longer registered
# ---------------------------------------------------------------------------


def test_export_command_no_longer_registered(runner):
    """``snow feature export`` must be gone after init subsumes it."""
    result = runner.invoke(["feature", "export", "--help"])
    # Typer / click reports an unknown subcommand with a non-zero exit.
    assert result.exit_code != 0, result.output


def test_export_command_not_in_feature_help(runner):
    """``snow feature --help`` must not list the deleted ``export`` command."""
    result = runner.invoke(["feature", "--help"])
    assert result.exit_code == 0, result.output
    # Match a standalone ``export`` subcommand entry only — avoid
    # false-positive matches against unrelated text like
    # ``export-into`` in another command's help.
    lines = [line.strip() for line in result.output.splitlines()]
    assert not any(line.startswith("export ") or line == "export" for line in lines), (
        "'export' should no longer appear as a standalone subcommand in "
        "`snow feature --help`"
    )


def test_export_typer_command_function_no_longer_present():
    """Belt-and-suspenders: the ``export`` function is removed from commands."""
    from snowflake.cli._plugins.feature import commands

    assert not hasattr(
        commands, "export"
    ), "commands.export must be deleted; init now subsumes the export pipeline"
