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

"""Tests for 'snow feature' CLI commands."""

from unittest import mock

FEATURE_MANAGER = "snowflake.cli._plugins.feature.commands.FeatureManager"


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_apply_requires_at_least_one_file(mock_manager, runner):
    """apply with no files should exit with a usage error (code 2)."""
    result = runner.invoke(["feature", "apply"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_apply_single_file(mock_manager, runner):
    """apply with one file should call FeatureManager.apply."""
    mock_manager.return_value.apply.return_value = {"status": "ok"}
    result = runner.invoke(["feature", "apply", "my_specs.yaml"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.apply.assert_called_once()
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert "my_specs.yaml" in call_kwargs["input_files"]


@mock.patch(FEATURE_MANAGER)
def test_apply_dry_flag(mock_manager, runner):
    """apply --dry should pass dry_run=True to FeatureManager.apply."""
    mock_manager.return_value.apply.return_value = {"status": "dry"}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--dry"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["dry_run"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_dev_flag(mock_manager, runner):
    """apply --dev should pass dev_mode=True."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--dev"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["dev_mode"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_overwrite_flag(mock_manager, runner):
    """apply --overwrite should pass overwrite=True."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--overwrite"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["overwrite"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_allow_recreate_flag(mock_manager, runner):
    """apply --allow-recreate should pass allow_recreate=True."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--allow-recreate"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["allow_recreate"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_help_shows_all_options(mock_manager, runner):
    """apply --help must show all documented flags."""
    result = runner.invoke(["feature", "apply", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--dry" in output
    assert "--dev" in output
    assert "--overwrite" in output
    assert "--allow-recreate" in output
    assert "--config" in output
    assert "--verbose" in output


# ---------------------------------------------------------------------------
# Status: header (apply / plan)
# ---------------------------------------------------------------------------
#
# These tests pin the behaviour added when fixing the bug-bash step-6
# TODO: ``apply succeeded (rc=0) but output missing 'Status: success'``.
# The previous renderer emitted the ``Status: <status>`` line only on
# the empty-ops branch of ``_ops_result``, so a successful CREATE_FV
# (which renders as a non-empty ops table) left scripts and operators
# without a single canonical success indicator.  ``_print_status_header``
# now writes the line on stderr regardless of whether the payload
# renders as a table or a summary message, mirroring
# ``_print_mode_header`` / ``_print_target_header``.


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
    falls back to counting ops with ``status == "success"``.  Pin this
    so apply paths that don't bubble up an explicit ``executed`` count
    (e.g. ``status: validation_failed`` results) still report a sane
    number rather than a confusing ``executed: None``.
    """
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


@mock.patch(FEATURE_MANAGER)
def test_apply_calls_print_status_header_on_success(mock_manager, runner):
    """``snow feature apply`` must call ``_print_status_header`` so a
    successful CREATE_FV no longer looks silent on stderr.  This is the
    direct fix for bug-bash step 6's ``apply succeeded (rc=0) but output
    missing 'Status: success'`` finding."""
    mock_manager.return_value.apply.return_value = {
        "status": "applied",
        "ops": [{"operation": "CREATE_FV", "name": "X", "status": "success"}],
        "executed": 1,
    }
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(["feature", "apply", "specs.yaml"])
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    passed = mock_print_status.call_args.args[0]
    assert passed["status"] == "applied"


@mock.patch(FEATURE_MANAGER)
def test_apply_calls_print_status_header_on_validation_failed(mock_manager, runner):
    """Even on the validation-failed early-return branch, the header
    must fire so the operator sees ``Status: validation_failed`` rather
    than a silent zero exit code."""
    mock_manager.return_value.apply.return_value = {
        "status": "validation_failed",
        "ops": [],
        "errors": ["VERSION_CONFLICT: ..."],
    }
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(["feature", "apply", "specs.yaml"])
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "validation_failed"


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_print_status_header_on_success(mock_manager, runner, tmp_path):
    """``snow feature plan`` must also call ``_print_status_header`` so
    its output is symmetric with ``snow feature apply`` and shell scripts
    can grep for a single canonical success indicator across both."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.apply.return_value = {
        "status": "ready",
        "ops": [{"operation": "NO_CHANGE", "name": "X"}],
    }
    mock_manager.return_value.write_plan.return_value = str(out_path)
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(
            ["feature", "plan", "specs.yaml", "--out", str(out_path)]
        )
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "ready"


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_print_status_header_on_validation_failed(
    mock_manager, runner, tmp_path
):
    """``plan`` short-circuits on validation_failed and must still
    fire the header before returning, so the operator sees the failure
    on stderr rather than only inside the JSON-renderable result body."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.apply.return_value = {
        "status": "validation_failed",
        "ops": [],
        "errors": ["..."],
    }
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(
            ["feature", "plan", "specs.yaml", "--out", str(out_path)]
        )
    assert result.exit_code == 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "validation_failed"
    mock_manager.return_value.write_plan.assert_not_called()


def test_ops_result_message_body_no_longer_includes_status_line():
    """The empty-ops summary message must NOT include ``Status:``
    anymore.  The header is now emitted on stderr by
    ``_print_status_header`` and duplicating it on stdout would
    produce two ``Status:`` lines per invocation.  This test pins
    that the body collapses to just ``Operations: 0`` (plus warnings),
    preventing future regressions that re-add the duplicate."""
    from snowflake.cli._plugins.feature.commands import _ops_result

    cmd_result = _ops_result({"status": "applied", "ops": [], "warnings": []})
    rendered = cmd_result.message
    assert "Status:" not in rendered, rendered
    assert "Operations: 0" in rendered, rendered


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_plan_requires_at_least_one_file(mock_manager, runner):
    """plan with no files should exit with a usage error."""
    result = runner.invoke(["feature", "plan"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_apply_with_dry_run(mock_manager, runner):
    """plan should delegate to FeatureManager.apply(dry_run=True)."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "plan", "specs.yaml"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["dry_run"] is True


@mock.patch(FEATURE_MANAGER)
def test_plan_does_not_write_plan_file_on_validation_failed(
    mock_manager, runner, tmp_path
):
    """plan must NOT write a plan file when apply returns validation_failed.

    The previous flow wrote the plan file *before* running validation, so a
    failed plan still left a stale ``feature_plan_*.json`` on disk that
    operators could mistake for a successful run.  The fix runs validation
    first and short-circuits before ``write_plan`` is invoked.
    """
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.apply.return_value = {
        "status": "validation_failed",
        "ops": [],
        "errors": ["VERSION_CONFLICT: ..."],
    }
    result = runner.invoke(["feature", "plan", "specs.yaml", "--out", str(out_path)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.write_plan.assert_not_called()
    assert not out_path.exists()


@mock.patch(FEATURE_MANAGER)
def test_plan_writes_plan_file_on_success(mock_manager, runner, tmp_path):
    """plan must invoke write_plan when apply reports a non-failed status."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.apply.return_value = {
        "status": "ready",
        "ops": [{"operation": "NO_CHANGE", "name": "x"}],
    }
    mock_manager.return_value.write_plan.return_value = str(out_path)
    result = runner.invoke(["feature", "plan", "specs.yaml", "--out", str(out_path)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.write_plan.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_bare_directory_triggers_full_sync_mode(mock_manager, runner, tmp_path):
    """Bare directory arguments are auto-expanded to ``<dir>/...`` and run full-sync.

    Previously a bare directory silently loaded zero files because the loader's
    ``glob.glob`` branch returned the directory path itself and ``process_file``
    then crashed with ``IsADirectoryError``, swallowed by the loader's bare
    ``except``.  The fix auto-expands bare directories to ``<dir>/...``; this
    test pins the CLI mode header and ``no_delete=False`` propagation so the
    mode shown to the user matches what the loader actually does.
    """
    real_dir = tmp_path / "specs"
    real_dir.mkdir()
    (real_dir / "fv.yaml").write_text("kind: StreamingFeatureView\nname: x\n")

    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", str(real_dir)])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["no_delete"] is False, call_kwargs


@mock.patch(FEATURE_MANAGER)
def test_specific_file_stays_incremental(mock_manager, runner, tmp_path):
    """A specific file argument runs in INCREMENTAL mode (``no_delete=True``)."""
    p = tmp_path / "fv.yaml"
    p.write_text("kind: StreamingFeatureView\nname: x\n")

    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", str(p)])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["no_delete"] is True, call_kwargs


@mock.patch(FEATURE_MANAGER)
def test_plan_help_does_not_show_overwrite(mock_manager, runner):
    """plan --help must NOT show --overwrite or --allow-recreate."""
    result = runner.invoke(["feature", "plan", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--overwrite" not in output
    assert "--allow-recreate" not in output


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_list_no_files_lists_deployed(mock_manager, runner):
    """list with no file args should call list_specs with empty file list."""
    mock_manager.return_value.list_specs.return_value = {}
    result = runner.invoke(["feature", "list"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.list_specs.call_args[1]
    assert call_kwargs["input_files"] == ()


@mock.patch(FEATURE_MANAGER)
def test_list_with_file_passes_files(mock_manager, runner):
    """list with a file arg should pass that file to list_specs."""
    mock_manager.return_value.list_specs.return_value = {}
    result = runner.invoke(["feature", "list", "my_specs.yaml"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.list_specs.call_args[1]
    assert "my_specs.yaml" in call_kwargs["input_files"]


def test_list_table_display_columns_include_type():
    """The `type` column must be present so multi-kind rows can be
    distinguished (FeatureView / Entity / Datasource)."""
    from snowflake.cli._plugins.feature.commands import _TABLE_DISPLAY_COLUMNS

    assert "type" in _TABLE_DISPLAY_COLUMNS


def test_list_table_display_columns_omits_scheduling_state():
    """``scheduling_state`` is intentionally excluded from the table
    display columns: it is duplicated inside the ``details`` cell for
    FeatureView rows and is empty for Entity / Datasource rows, so
    surfacing it as its own column was pure noise.  The runtime status
    the user actually cares about is in ``details.scheduling_state``
    (FeatureView) and ``snow feature online-service status``."""
    from snowflake.cli._plugins.feature.commands import _TABLE_DISPLAY_COLUMNS

    assert "scheduling_state" not in _TABLE_DISPLAY_COLUMNS


def test_list_table_display_columns_omits_database_and_schema():
    """``database_name`` and ``schema_name`` are uniform across every
    row of a single ``snow feature list`` invocation (the connection
    has one current database/schema), so duplicating them in every
    table row was wasted width.  They are now surfaced once, above the
    table, by the ``Database: ... Schema: ...`` header line printed by
    ``_print_listing_scope_header``."""
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
    and schema_name (e.g. file-mode listings) signal that no header
    should be printed."""
    from snowflake.cli._plugins.feature.commands import _listing_scope

    assert _listing_scope([]) is None
    assert _listing_scope([{"name": "a"}, {"name": "b"}]) is None
    # Empty strings count as "missing" — they should not be rendered
    # as the scope value.
    assert _listing_scope([{"database_name": "", "schema_name": ""}]) is None


def test_project_columns_aligns_heterogeneous_rows():
    """Every projected row must carry **all** display columns in the
    canonical ``_TABLE_DISPLAY_COLUMNS`` order, with empty strings for
    fields a particular row does not populate.

    Without this guarantee the underlying table renderer (which uses
    each row's dict iteration order to position values) shifts the
    Entity / Datasource type and name values into columns that belong
    to FeatureView-only fields like ``created_on``.  The user-visible
    symptom is "the type shows up in the first column, ``created_on``"
    — see the column-alignment fix that introduced this test.
    """
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
        # Every row carries the full display column set …
        assert list(row.keys()) == _TABLE_DISPLAY_COLUMNS, (
            f"Expected canonical column order {_TABLE_DISPLAY_COLUMNS}, "
            f"got {list(row.keys())}"
        )
        # … and the columns we deliberately moved to header / details
        # are *not* projected per-row.
        assert "scheduling_state" not in row
        assert "database_name" not in row
        assert "schema_name" not in row

    fv_proj, entity_proj, ds_proj = projected

    # FV columns survive untouched (the upstream scheduling_state value
    # is preserved inside ``details`` for the FeatureView row).
    assert fv_proj["type"] == "FeatureView"
    assert fv_proj["name"] == "click_fv"
    assert fv_proj["version"] == "v1"
    assert fv_proj["entities"] == "user_id"
    assert fv_proj["created_on"] == "2024-01-01"
    assert fv_proj["details"] == {"scheduling_state": "ACTIVE"}

    # Entity row's type/name land in the right keys, and FV-only
    # columns are empty strings — proving column-by-column alignment
    # against the canonical ``_TABLE_DISPLAY_COLUMNS`` order.
    assert entity_proj["type"] == "Entity"
    assert entity_proj["name"] == "user_id"
    assert entity_proj["entities"] == "USER_ID"
    assert entity_proj["version"] == ""
    assert entity_proj["created_on"] == ""
    assert entity_proj["details"] == {
        "join_keys": ["USER_ID"],
        "comment": "User identity entity",
    }

    # Datasource row: same alignment guarantees — type/name in the
    # right slots, FV-only columns blanked out, spec-derived details
    # carried through verbatim.  The ``type`` column surfaces the
    # specific source type (``OfflineTable``) instead of the generic
    # ``Datasource`` so operators can distinguish stream vs table
    # backings at a glance — see
    # ``test_project_columns_surfaces_datasource_source_type_in_type_column``.
    assert ds_proj["type"] == "OfflineTable"
    assert ds_proj["name"] == "click_events_offline"
    assert ds_proj["entities"] == ""
    assert ds_proj["version"] == ""
    assert ds_proj["created_on"] == ""
    assert ds_proj["details"] == {"source_type": "OfflineTable", "column_count": 7}


def test_project_columns_surfaces_datasource_source_type_in_type_column():
    """Datasource rows surface ``details.source_type`` in the rendered
    ``type`` column instead of the generic ``Datasource`` label.

    This mirrors how FeatureView rows already render the specific
    subkind (``StreamingFeatureView`` / ``RealtimeFeatureView`` /
    ``BatchFeatureView``) in the ``type`` column.  Operators reading
    ``snow feature list`` see the backing kind (``Stream`` vs
    ``OfflineTable``) at a glance without having to expand the
    ``details`` cell.

    Behavior:

    * ``details.source_type == "Stream"``      → ``type`` renders ``Stream``
    * ``details.source_type == "OfflineTable"``→ ``type`` renders ``OfflineTable``
    * missing / empty ``details.source_type``  → ``type`` falls back to
      the canonical ``Datasource`` so the row remains visibly a
      datasource even when the source type is unknown.

    Internal model (``AppliedObject.kind``) is unchanged — the swap is
    a display-time projection only.  Code paths that group by
    ``kind == "Datasource"`` continue to work untouched.
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

    # Non-datasource rows are never rewritten — the swap only fires
    # when the canonical ``Datasource`` value is present.
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
    # The table now displays only 6 columns (type, name, version,
    # entities, created_on, details); database/schema are surfaced
    # once above the table by the ``Database: ... Schema: ...`` header
    # line.  Check for prefixes short enough to land inside a single
    # column cell.
    assert "Entity" in result.output
    assert "Dataso" in result.output  # Datasource wraps as "Dataso\nurce"
    assert "Featur" in result.output  # FeatureView wraps as "Featur\neView"
    assert "click_" in result.output  # FV name appears (may wrap)
    assert "user_e" in result.output  # datasource name appears (may wrap)
    # ``scheduling_state`` is no longer a column header — it lives
    # inside ``details`` for FeatureView rows.
    header_block = result.output.split("|--")[0]
    assert "scheduling_state" not in header_block
    # ``database_name`` / ``schema_name`` are no longer column headers
    # either — but the header line above the table still surfaces
    # the uniform DB / schema once.
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
def test_describe_passes_name(mock_manager, runner):
    """describe MY_ENTITY should call FeatureManager.describe(name='MY_ENTITY')."""
    mock_manager.return_value.describe.return_value = {}
    result = runner.invoke(["feature", "describe", "MY_ENTITY"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.describe.call_args[1]
    assert call_kwargs["name"] == "MY_ENTITY"


# ---------------------------------------------------------------------------
# drop
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_no_flags_returns_status(mock_manager, runner):
    """online-service with no flags should show runtime status."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "compute_pool": "active",
        "postgres": "active",
        "service": "active",
        "endpoints": [],
    }
    result = runner.invoke(["feature", "online-service"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.get_status.assert_called_once()


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_already_running_is_noop(mock_manager, runner):
    """online-service --create should be a no-op when status is already RUNNING."""
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service already initialized",
    }
    result = runner.invoke(["feature", "online-service", "--create"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.initialize_service.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_and_polls(mock_manager, runner):
    """online-service --create should create the runtime and poll until RUNNING."""
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
    call_kwargs = mock_manager.return_value.ingest.call_args[1]
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
    call_kwargs = mock_manager.return_value.ingest.call_args[1]
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
    # Typer emits "Missing option '--version'" (or similar) on stderr;
    # asserting the option name appears in the help/error text catches
    # accidental renames.
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
    call_kwargs = mock_manager.return_value.query.call_args[1]
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
# init
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_init_help_shows_command(mock_manager, runner):
    """init --help should show the init command with --no-scaffold option."""
    result = runner.invoke(["feature", "init", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--no-scaffold" in output


@mock.patch(FEATURE_MANAGER)
def test_init_calls_manager_init(mock_manager, runner):
    """init should call FeatureManager.init()."""
    mock_manager.return_value.init.return_value = {
        "status": "initialized",
        "database": "DB",
        "schema": "SCH",
        "directories": ["entities", "datasources", "feature_views"],
    }
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.init.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_init_no_scaffold_flag(mock_manager, runner):
    """init --no-scaffold should pass no_scaffold=True to FeatureManager.init."""
    mock_manager.return_value.init.return_value = {
        "status": "initialized",
        "database": "DB",
        "schema": "SCH",
        "directories": [],
    }
    result = runner.invoke(["feature", "init", "--no-scaffold"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.init.call_args[1]
    assert call_kwargs["no_scaffold"] is True


@mock.patch(FEATURE_MANAGER)
def test_init_default_no_scaffold_is_false(mock_manager, runner):
    """init without --no-scaffold should pass no_scaffold=False."""
    mock_manager.return_value.init.return_value = {
        "status": "initialized",
        "database": "DB",
        "schema": "SCH",
        "directories": ["entities", "datasources", "feature_views"],
    }
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.init.call_args[1]
    assert call_kwargs["no_scaffold"] is False


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_export_calls_manager(mock_manager, runner, tmp_path):
    """export should call FeatureManager.export_specs with the given dir."""
    mock_manager.return_value.export_specs.return_value = {
        "status": "exported",
        "directory": str(tmp_path),
        "files": [str(tmp_path / "feature_views/my_fv.yaml")],
    }
    result = runner.invoke(["feature", "export", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.export_specs.assert_called_once_with(str(tmp_path))


@mock.patch(FEATURE_MANAGER)
def test_export_default_dir(mock_manager, runner):
    """export without --dir should call export_specs with '.'."""
    mock_manager.return_value.export_specs.return_value = {
        "status": "exported",
        "directory": ".",
        "files": [],
    }
    result = runner.invoke(["feature", "export"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.export_specs.assert_called_once_with(".")


@mock.patch(FEATURE_MANAGER)
def test_export_returns_file_list(mock_manager, runner, tmp_path):
    """export should render the list of written files."""
    files = [
        str(tmp_path / "feature_views/my_fv.yaml"),
        str(tmp_path / "entities/user_id.yaml"),
        str(tmp_path / "datasources/click_events.yaml"),
    ]
    mock_manager.return_value.export_specs.return_value = {
        "status": "exported",
        "directory": str(tmp_path),
        "files": files,
    }
    result = runner.invoke(["feature", "export", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "my_fv.yaml" in result.output
