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

"""Tests for ``snow feature`` Typer commands (manifest-driven surface).

The CLI surface:

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
  surface (``--config`` is gone), and it lives on ``plan`` only —
  templating is resolved at plan time and baked into the plan JSON,
  so ``apply`` / ``list`` / ``describe`` / ``ingest`` / ``query``
  do not accept it.
* ``init`` derives the manifest from the live connection and is
  fail-fast on a pre-existing ``manifest.yml`` (no ``--force``).
"""

import io
import json
import logging
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from snowflake.cli.api.secure_path import SecurePath

FEATURE_MANAGER = "snowflake.cli._plugins.feature.commands.FeatureManager"


def _json_from_output(output: str):
    """Parse the JSON payload out of a ``--format json`` invocation.

    The plugin preview ``WARNING:`` preamble (and, with the runner's
    default ``mix_stderr=True``, any stderr) is prepended to the JSON
    on stdout.  The structured payload is the first ``{`` / ``[`` to
    end-of-string, so slice from whichever delimiter appears first and
    ``json.loads`` the remainder.  This asserts, by construction, that
    no free-form text trails the JSON in structured mode.
    """
    starts = [i for i in (output.find("{"), output.find("[")) if i != -1]
    assert starts, f"no JSON payload found in output: {output!r}"
    return json.loads(output[min(starts) :])


def _leading_json_object(output: str):
    """Parse the first JSON object in ``output``, ignoring trailing text.

    On a terminal-failure the command prints the structured envelope to
    stdout and *then* raises ``CliError`` — whose ``Error: ...`` banner the
    mixed-stream runner appends after the JSON.  ``raw_decode`` stops at the
    end of that first object so the trailing banner does not break parsing.
    """
    start = output.find("{")
    assert start != -1, f"no JSON object found in output: {output!r}"
    obj, _ = json.JSONDecoder().raw_decode(output[start:])
    return obj


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_apply_no_positional_args_runs_with_defaults(mock_manager, runner):
    """``apply`` accepts zero positional arguments — the new surface
    is fully flag-driven (``--from``, ``--target``, ``--plan``).

    The legacy contract was the opposite: ``apply`` required at
    least one ``INPUT_FILE`` positional and exited with usage code 2
    otherwise.  The positional surface is deleted entirely,
    so re-running the bare command must succeed and delegate
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
def test_apply_destructive_flag(mock_manager, runner):
    """``apply --destructive`` propagates ``destructive=True``."""
    mock_manager.return_value.apply.return_value = {"status": "applied", "ops": []}
    result = runner.invoke(["feature", "apply", "--destructive"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.apply.call_args.kwargs["destructive"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_rejects_overwrite_flag(mock_manager, runner):
    """``--overwrite`` is not part of the command surface.

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
    """``--config`` is not part of the command surface — ``-D key=value``
    is the only template-variable surface now."""
    result = runner.invoke(["feature", "apply", "--config", "vars.yaml"])
    assert result.exit_code != 0, result.output


@mock.patch(FEATURE_MANAGER)
def test_apply_help_shows_dcm_strict_surface(mock_manager, runner):
    """``apply --help`` must surface the new flag set and prove the
    deleted flags are gone.

    ``--from`` and ``--target`` appear on every relevant command's
    ``--help``. ``--variable`` is plan-only (templating is resolved at
    plan time and baked into ``out/plan/``); ``apply`` is a pure
    plan-file consumer, so it must NOT surface the flag. The deleted
    flags (``--overwrite``, ``--config``, ``--dry``, ``--dev``) must NOT
    appear or a regression has slipped a legacy code path back in.
    """
    result = runner.invoke(["feature", "apply", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--from" in output
    assert "--target" in output
    assert "--variable" not in output
    assert "--destructive" in output
    assert "--plan" in output
    assert "--dry" not in output
    assert "--overwrite" not in output
    assert "--config" not in output
    assert "--dev" not in output


@mock.patch(FEATURE_MANAGER)
def test_apply_help_documents_destructive_overwrite_effect(mock_manager, runner):
    """``--destructive`` also sets ``PlanOptions.overwrite``
    (manager.py ``_apply_from_plan_file``), so CREATE_FV re-registers over a
    live name/version. Pin that the help text states BOTH effects — the
    destructive gate and the in-place overwrite — so the flag cannot drift
    back to documenting only recreation.
    """
    result = runner.invoke(["feature", "apply", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "destructive" in output
    assert "overwrite" in output


@mock.patch(FEATURE_MANAGER)
def test_apply_rejects_variable_flag(mock_manager, runner):
    """``apply`` no longer accepts ``--variable`` / ``-D``.

    Templating is resolved at plan time and baked into the plan JSON that
    ``apply`` consumes; a runtime override on ``apply`` could never be
    honored, so silently discarding it is worse than rejecting it.
    """
    mock_manager.return_value.apply.return_value = {"status": "applied", "ops": []}
    result = runner.invoke(["feature", "apply", "-D", "env=prod"])
    assert result.exit_code != 0, result.output


# ---------------------------------------------------------------------------
# Status: header (apply / plan)
# ---------------------------------------------------------------------------
#
# These tests pin the behaviour that guarantees a parseable status line:
# ``apply succeeded (rc=0) but output missing 'Status: success'`` must not
# recur.


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
    # NO_CHANGE is omitted from the operator-facing count, so only the
    # single CREATE_FV op is tallied.
    assert "Operations: 1" in captured.err
    assert "executed: 1" in captured.err
    assert captured.out == ""


def test_print_status_header_empty_ops_still_emits_status(capsys):
    """Empty ops list still emits the header — even ``no_plan`` must
    surface a parseable status line on stderr."""
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
    in the rendered header — the target-name surface is what lets
    operators distinguish multiple manifest profiles in a single
    shell scrollback.
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
    successful CREATE_FV.
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
    # ``validation_failed`` is a terminal failure: the status header still
    # fires, but the command now exits non-zero (CliError).
    assert result.exit_code != 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "validation_failed"


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_print_status_header_on_success(mock_manager, runner, tmp_path):
    """``snow feature plan`` also fires ``_print_status_header`` so
    its output is symmetric with ``snow feature apply``."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = (
        {
            "status": "ready",
            "ops": [{"operation": "NO_CHANGE", "name": "X"}],
        },
        mock.MagicMock(name="plan"),
    )
    mock_manager.return_value.write_plan_object.return_value = str(out_path)
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
    mock_manager.return_value.plan.return_value = (
        {
            "status": "validation_failed",
            "ops": [],
            "errors": ["..."],
        },
        None,
    )
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_status_header"
    ) as mock_print_status:
        result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    # ``validation_failed`` is terminal: header fires, exit is non-zero, and
    # no plan file is written.
    assert result.exit_code != 0, result.output
    mock_print_status.assert_called_once()
    assert mock_print_status.call_args.args[0]["status"] == "validation_failed"
    mock_manager.return_value.write_plan_object.assert_not_called()


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


def test_ops_result_omits_no_change():
    """``NO_CHANGE`` ops are cosmetic noise and must not appear in the
    rendered CLI table — only actionable ops are shown."""
    from snowflake.cli._plugins.feature.commands import _ops_result

    cmd_result = _ops_result(
        {
            "status": "ready",
            "ops": [
                {"operation": "CREATE_FV", "name": "A"},
                {"operation": "NO_CHANGE", "name": "B"},
                {"operation": "UPDATE_FV", "name": "C"},
            ],
        }
    )
    rows = list(cmd_result.result)
    operations = [r["operation"] for r in rows]
    assert operations == ["CREATE_FV", "UPDATE_FV"]
    assert "NO_CHANGE" not in operations


def test_ops_result_all_no_change_is_empty_message():
    """When every op is ``NO_CHANGE`` the table collapses to the
    ``Operations: 0`` summary message rather than a table of skips."""
    from snowflake.cli._plugins.feature.commands import _ops_result

    cmd_result = _ops_result(
        {
            "status": "ready",
            "ops": [
                {"operation": "NO_CHANGE", "name": "A"},
                {"operation": "NO_CHANGE", "name": "B"},
            ],
        }
    )
    assert "Operations: 0" in cmd_result.message


def test_ops_result_orders_type_before_name():
    """The plan/apply ops table must lead with ``type`` then ``name``,
    matching ``snow feature list``.  Column order follows the first row's
    dict key order, so the projection must place ``type`` first."""
    from snowflake.cli._plugins.feature.commands import _ops_result

    cmd_result = _ops_result(
        {
            "status": "ready",
            "ops": [
                {
                    "type": "BatchFeatureView",
                    "name": "MY_BFV",
                    "operation": "CREATE_FV",
                    "reason": "new",
                    "destructive": False,
                },
                {
                    "type": "Entity",
                    "name": "USER_ID",
                    "operation": "CREATE_ENTITY",
                    "reason": "new",
                    "destructive": False,
                },
            ],
        }
    )
    rows = list(cmd_result.result)
    assert list(rows[0].keys())[:2] == ["type", "name"]
    assert rows[0]["type"] == "BatchFeatureView"
    assert rows[1]["type"] == "Entity"


def test_project_ops_columns_orders_and_preserves_error():
    """Known columns are ordered ``type``-first; extra keys such as
    ``error`` are preserved after the known columns."""
    from snowflake.cli._plugins.feature.commands import _project_ops_columns

    rows = _project_ops_columns(
        [
            {
                "status": "error",
                "operation": "CREATE_FV",
                "error": "boom",
                "name": "X",
                "type": "StreamingFeatureView",
                "reason": "new",
                "destructive": False,
            }
        ]
    )
    assert list(rows[0].keys()) == [
        "type",
        "name",
        "operation",
        "reason",
        "destructive",
        "status",
        "error",
    ]


def test_project_ops_columns_places_version_after_name():
    """When an op row carries a ``version`` it renders right after ``name``.

    Object identity is (name, version); the ops table surfaces the version so
    the operator can tell which of two same-named FVs an op targets.
    """
    from snowflake.cli._plugins.feature.commands import _project_ops_columns

    rows = _project_ops_columns(
        [
            {
                "type": "BatchFeatureView",
                "name": "MY_BFV",
                "version": "V2",
                "operation": "CREATE_FV",
                "reason": "new",
                "destructive": False,
            }
        ]
    )
    assert list(rows[0].keys())[:3] == ["type", "name", "version"]
    assert rows[0]["version"] == "V2"


def test_project_ops_columns_tolerates_missing_type():
    """An older decl build may omit ``type``; the projection must not
    inject empty columns and simply orders the keys that exist."""
    from snowflake.cli._plugins.feature.commands import _project_ops_columns

    rows = _project_ops_columns([{"operation": "CREATE_FV", "name": "X"}])
    assert "type" not in rows[0]
    assert list(rows[0].keys()) == ["name", "operation"]


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_plan_no_positional_args_runs_with_defaults(mock_manager, runner):
    """``plan`` accepts zero positional arguments — the legacy
    ``INPUT_FILES`` surface is gone.  Bare ``snow feature plan`` runs
    against the project rooted at the current working directory.
    """
    mock_manager.return_value.plan.return_value = (
        {"status": "ready", "ops": []},
        mock.MagicMock(name="plan"),
    )
    mock_manager.return_value.write_plan_object.return_value = (
        "out/plan/feature_plan_x.json"
    )
    result = runner.invoke(["feature", "plan"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.plan.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_manager_plan(mock_manager, runner):
    """``plan`` delegates to ``FeatureManager.plan`` (validate +
    generate_plan, no SQL).  ``apply`` MUST NOT be called by the
    plan command — the two commands are now disjoint code paths.
    """
    mock_manager.return_value.plan.return_value = (
        {"status": "ready", "ops": []},
        mock.MagicMock(name="plan"),
    )
    mock_manager.return_value.write_plan_object.return_value = (
        "out/plan/feature_plan_x.json"
    )
    result = runner.invoke(["feature", "plan"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.plan.assert_called_once()
    mock_manager.return_value.apply.assert_not_called()


def _wire_plan_success(mock_manager):
    """Give ``manager.plan`` / ``write_plan_object`` benign return values so
    the plan command reaches its success path and we can inspect the
    ``no_delete`` kwarg threaded into ``manager.plan``."""
    mock_manager.return_value.plan.return_value = (
        {"status": "ready", "ops": []},
        mock.MagicMock(name="plan"),
    )
    mock_manager.return_value.write_plan_object.return_value = (
        "out/plan/feature_plan_x.json"
    )


@mock.patch(FEATURE_MANAGER)
def test_plan_defaults_to_no_delete(mock_manager, runner):
    """Deletion detection is OFF by default: bare ``snow feature plan``
    threads ``no_delete=True`` into ``FeatureManager.plan`` so orphaned
    objects are left untouched."""
    _wire_plan_success(mock_manager)
    result = runner.invoke(["feature", "plan"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.plan.call_args.kwargs["no_delete"] is True


@mock.patch(FEATURE_MANAGER)
def test_plan_delete_flag_enables_deletion(mock_manager, runner):
    """``--delete`` flips the default: the user can turn deletion detection
    back on, threading ``no_delete=False`` into ``FeatureManager.plan``."""
    _wire_plan_success(mock_manager)
    result = runner.invoke(["feature", "plan", "--delete"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.plan.call_args.kwargs["no_delete"] is False


@mock.patch(FEATURE_MANAGER)
def test_plan_no_delete_flag_still_accepted(mock_manager, runner):
    """The explicit ``--no-delete`` flag remains accepted and keeps deletion
    detection OFF (``no_delete=True``)."""
    _wire_plan_success(mock_manager)
    result = runner.invoke(["feature", "plan", "--no-delete"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.plan.call_args.kwargs["no_delete"] is True


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
    mock_manager.return_value.plan.return_value = (
        {
            "status": "validation_failed",
            "ops": [],
            "errors": ["VERSION_CONFLICT: ..."],
        },
        None,
    )
    result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    # Terminal validation failure exits non-zero and writes no plan file.
    assert result.exit_code != 0, result.output
    mock_manager.return_value.write_plan_object.assert_not_called()
    assert not out_path.exists()


@mock.patch(FEATURE_MANAGER)
def test_plan_writes_plan_file_on_success(mock_manager, runner, tmp_path):
    """``plan`` invokes ``write_plan`` when ``manager.plan`` reports
    a non-failed status."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = (
        {
            "status": "ready",
            "ops": [{"operation": "NO_CHANGE", "name": "x"}],
        },
        mock.MagicMock(name="plan"),
    )
    mock_manager.return_value.write_plan_object.return_value = str(out_path)
    result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.write_plan_object.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_plan_serializes_plan_object_from_manager_plan(mock_manager, runner, tmp_path):
    """Single generate: the command threads the *same* ``Plan`` object
    ``manager.plan`` returned into ``write_plan_object`` (no regenerate, no
    refetch) and must NOT call the self-generating ``write_plan`` convenience.
    Closes the double-``DESCRIBE`` / TOCTOU gap from PR review r3918618799."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    plan_sentinel = mock.MagicMock(name="plan")
    mock_manager.return_value.plan.return_value = (
        {"status": "ready", "ops": [{"operation": "CREATE_FV", "name": "X"}]},
        plan_sentinel,
    )
    mock_manager.return_value.write_plan_object.return_value = str(out_path)

    result = runner.invoke(["feature", "plan", "--out", str(out_path)])

    assert result.exit_code == 0, result.output
    mock_manager.return_value.plan.assert_called_once()
    mock_manager.return_value.write_plan_object.assert_called_once()
    # The generated Plan object is threaded straight into serialization.
    assert (
        mock_manager.return_value.write_plan_object.call_args.args[0] is plan_sentinel
    )
    # The self-generating convenience must not run on the command path.
    mock_manager.return_value.write_plan.assert_not_called()


@mock.patch(FEATURE_MANAGER)
def test_plan_passes_variables_via_dash_d_flag(mock_manager, runner):
    """``-D key=value`` (and the long form ``--variable``) are the
    only template-variable surface.  The list of values is
    forwarded verbatim to the manager so the underlying
    ``decl_api.parse_variables`` sees the same string the operator
    typed.
    """
    mock_manager.return_value.plan.return_value = (
        {"status": "ready", "ops": []},
        mock.MagicMock(name="plan"),
    )
    mock_manager.return_value.write_plan_object.return_value = (
        "out/plan/feature_plan_x.json"
    )
    result = runner.invoke(
        ["feature", "plan", "-D", "env=prod", "--variable", "region=us-west-2"]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.plan.call_args.kwargs
    assert call_kwargs["variables"] == ["env=prod", "region=us-west-2"]


@mock.patch(FEATURE_MANAGER)
def test_plan_passes_target_name(mock_manager, runner, tmp_path):
    """``plan --from <dir> --target NAME`` propagates both flags."""
    mock_manager.return_value.plan.return_value = (
        {"status": "ready", "ops": []},
        mock.MagicMock(name="plan"),
    )
    mock_manager.return_value.write_plan_object.return_value = (
        "out/plan/feature_plan_x.json"
    )
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
    assert "--destructive" not in output
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
    """``list`` no longer accepts positional spec paths."""
    result = runner.invoke(["feature", "list", "my_specs.yaml"])
    assert result.exit_code != 0, result.output


@mock.patch(FEATURE_MANAGER)
def test_list_help_omits_variable_flag(mock_manager, runner):
    """``list --help`` surfaces ``--from`` / ``--target`` but not
    ``--variable``: ``list`` reads deployed state and never loads or
    renders local specs, so a templating override is meaningless here."""
    result = runner.invoke(["feature", "list", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--from" in output
    assert "--target" in output
    assert "--variable" not in output


@mock.patch(FEATURE_MANAGER)
def test_list_rejects_variable_flag(mock_manager, runner):
    """``list -D env=prod`` fails rather than silently discarding the
    override — ``list`` cannot honor templating variables."""
    mock_manager.return_value.list_specs.return_value = {"specs": []}
    result = runner.invoke(["feature", "list", "-D", "env=prod"])
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


def test_list_table_display_columns_omits_details():
    """``details`` is intentionally excluded from the table display
    columns.  The kind-specific ``details`` dict is verbose and its one
    load-bearing field (``source_type`` for Datasource rows) is already
    surfaced in the ``type`` column, so rendering the raw dict as its
    own column was pure noise.  It is still carried on the raw row so
    ``_project_columns`` can derive the Datasource ``type`` label."""
    from snowflake.cli._plugins.feature.commands import _TABLE_DISPLAY_COLUMNS

    assert "details" not in _TABLE_DISPLAY_COLUMNS


def test_listing_scope_uniform_rows_returns_single_value_pair():
    """When every row has the same database_name and schema_name, the
    helper returns those values verbatim so the header can render
    ``Database: <db>  Schema: <sch>``."""
    from snowflake.cli._plugins.feature.commands import _listing_scope

    rows = [
        {"name": "a", "database_name": "TEST_DB", "schema_name": "TEST_SCHEMA"},
        {"name": "b", "database_name": "TEST_DB", "schema_name": "TEST_SCHEMA"},
        {"name": "c", "database_name": "TEST_DB", "schema_name": "TEST_SCHEMA"},
    ]
    assert _listing_scope(rows) == ("TEST_DB", "TEST_SCHEMA")


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
        assert "details" not in row

    fv_proj, entity_proj, ds_proj = projected

    assert fv_proj["type"] == "FeatureView"
    assert fv_proj["name"] == "click_fv"
    assert fv_proj["version"] == "v1"
    assert fv_proj["entities"] == "user_id"
    assert fv_proj["created_on"] == "2024-01-01"

    assert entity_proj["type"] == "Entity"
    assert entity_proj["name"] == "user_id"
    assert entity_proj["entities"] == "USER_ID"
    assert entity_proj["version"] == ""
    assert entity_proj["created_on"] == ""

    assert ds_proj["type"] == "OfflineTable"
    assert ds_proj["name"] == "click_events_offline"
    assert ds_proj["entities"] == ""
    assert ds_proj["version"] == ""
    assert ds_proj["created_on"] == ""


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

    # ``details`` drives the ``type`` derivation but is not itself a
    # display column, so it must not leak into the projected rows.
    for proj in (stream_proj, offline_proj, no_st_proj, no_det_proj):
        assert "details" not in proj

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
    assert "details" not in header_block
    assert "Database: DB" in result.output
    assert "Schema: SCH" in result.output


@mock.patch(FEATURE_MANAGER)
def test_list_json_omits_schema_header(mock_manager, runner):
    """``list --format json`` emits a JSON array and suppresses the
    ``Database: … Schema: …`` free-form header (which previously
    polluted machine-readable stdout)."""
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
        ],
    }
    result = runner.invoke(["feature", "list", "--format", "json"])
    assert result.exit_code == 0, result.output

    # Free-form scope header must not appear in structured mode.
    assert "Database: DB" not in result.output
    assert "Schema: SCH" not in result.output

    payload = _json_from_output(result.output)
    assert isinstance(payload, list)
    assert payload[0]["type"] == "FeatureView"
    assert payload[0]["name"] == "click_fv"
    # Projected display columns only — verbose row fields stay out.
    assert "database_name" not in payload[0]
    assert "details" not in payload[0]


@mock.patch(FEATURE_MANAGER)
def test_list_json_has_no_progress_phase_text(mock_manager, runner):
    """The state-fetch progress bar is TABLE-only.  In structured mode
    (``--format json``) none of the sequential phase labels may leak into
    the machine-readable output — the bar renders on a stderr Console
    that is suppressed when ``get_cli_context().silent`` is True.
    """
    mock_manager.return_value.list_specs.return_value = {
        "source": "snowflake",
        "specs": [],
    }
    result = runner.invoke(["feature", "list", "--format", "json"])
    assert result.exit_code == 0, result.output

    for phrase in (
        "Loading online feature tables",
        "Loading feature views",
        "Loading entities",
        "Loading feature groups",
    ):
        assert phrase not in result.output, (
            f"progress phase {phrase!r} must not appear in --format json "
            f"output; got: {result.output!r}"
        )

    # Structured stdout still parses cleanly (no progress text prepended).
    payload = _json_from_output(result.output)
    assert isinstance(payload, (list, dict))


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


@mock.patch(FEATURE_MANAGER)
def test_describe_help_omits_variable_flag(mock_manager, runner):
    """``describe --help`` surfaces ``--from`` / ``--target`` / ``--version``
    but not ``--variable``: describe reads deployed state (and local YAML
    only for example enrichment), so a templating override is meaningless."""
    result = runner.invoke(["feature", "describe", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--from" in output
    assert "--target" in output
    assert "--version" in output
    assert "--variable" not in output


@mock.patch(FEATURE_MANAGER)
def test_describe_rejects_variable_flag(mock_manager, runner):
    """``describe NAME -D env=prod`` fails rather than silently discarding
    the override."""
    mock_manager.return_value.describe.return_value = {}
    result = runner.invoke(["feature", "describe", "MY_ENTITY", "-D", "env=prod"])
    assert result.exit_code != 0, result.output


@mock.patch(FEATURE_MANAGER)
def test_describe_version_defaults_to_none(mock_manager, runner, tmp_path):
    """``describe NAME`` (no ``--version``) forwards ``version=None``."""
    mock_manager.return_value.describe.return_value = {}
    result = runner.invoke(
        ["feature", "describe", "MY_ENTITY", "--from", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.describe.call_args.kwargs
    assert call_kwargs["version"] is None


@mock.patch(FEATURE_MANAGER)
def test_describe_forwards_version(mock_manager, runner, tmp_path):
    """``describe NAME --version V2`` forwards ``version='V2'`` to the manager."""
    mock_manager.return_value.describe.return_value = {}
    result = runner.invoke(
        [
            "feature",
            "describe",
            "USER_CLICKS",
            "--from",
            str(tmp_path),
            "--version",
            "V2",
        ]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.describe.call_args.kwargs
    assert call_kwargs["name"] == "USER_CLICKS"
    assert call_kwargs["version"] == "V2"


_DESCRIBE_ENVELOPE = {
    "name": "user_clicks",
    "feature_view": "user_clicks",
    "version": "v1",
    "database": "TEST_DB",
    "schema": "TEST_SCHEMA",
    "oft_name": "USER_CLICKS$V1$ONLINE",
    "entities": ["USER_ID"],
    "rows": [
        {"name": "USER_ID", "type": "NUMBER(38,0)", "primary key": "Y"},
        {"name": "CLICK_COUNT", "type": "NUMBER(38,0)", "primary key": "N"},
    ],
    "_display": "\n==========\n  Feature View: user_clicks\n==========\n",
}


@mock.patch(FEATURE_MANAGER)
def test_describe_json_returns_envelope(mock_manager, runner):
    """``describe --format json`` returns the full manager envelope as a
    nested JSON object, with the DESCRIBE ``rows`` intact and the rich
    ``_display`` banner suppressed.

    Pre-fix the success path wrote ``_display`` to stderr and returned
    ``_to_collection(rows)`` — which projected the OFT column metadata
    onto the *list* display columns (``type`` / ``version`` / ...),
    producing a useless mostly-empty array under ``--format json``.
    """
    mock_manager.return_value.describe.return_value = dict(_DESCRIBE_ENVELOPE)
    result = runner.invoke(["feature", "describe", "USER_CLICKS", "--format", "json"])
    assert result.exit_code == 0, result.output

    # Rich banner suppressed in structured mode.
    assert "Feature View: user_clicks" not in result.output

    payload = _json_from_output(result.output)
    assert payload["name"] == "user_clicks"
    assert payload["oft_name"] == "USER_CLICKS$V1$ONLINE"
    assert isinstance(payload["rows"], list)
    # Rows are the authoritative DESCRIBE column metadata, not the list
    # projection.
    assert payload["rows"][0]["name"] == "USER_ID"
    assert payload["rows"][0]["primary key"] == "Y"
    # Display-only key must not leak into structured stdout.
    assert "_display" not in payload


@mock.patch(FEATURE_MANAGER)
def test_describe_table_still_writes_display(mock_manager, runner):
    """Default (TABLE) ``describe`` still writes the rich banner."""
    mock_manager.return_value.describe.return_value = dict(_DESCRIBE_ENVELOPE)
    result = runner.invoke(["feature", "describe", "USER_CLICKS"])
    assert result.exit_code == 0, result.output
    assert "Feature View: user_clicks" in result.output


@mock.patch(FEATURE_MANAGER)
def test_describe_error_json_returns_object(mock_manager, runner):
    """A ``describe`` error envelope surfaces as a nested JSON object
    with no free-form display text."""
    mock_manager.return_value.describe.return_value = {
        "status": "error",
        "name": "CLICKSTREAM_EVENTS",
        "error": "CLICKSTREAM_EVENTS: not found in deployed feature views",
    }
    result = runner.invoke(
        ["feature", "describe", "CLICKSTREAM_EVENTS", "--format", "json"]
    )
    # A not-found describe is a terminal failure: non-zero exit, envelope
    # (with ``status`` / ``error``) still on stdout.
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "error"
    assert payload["name"] == "CLICKSTREAM_EVENTS"
    assert "not found" in payload["error"]


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_subcommand_returns_status(mock_manager, runner):
    """``online-service status`` should show runtime status."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "compute_pool": {"status": "ACTIVE", "name": "POOL"},
        "postgres": {"status": "READY", "name": "PG"},
        "service": {"status": "RUNNING", "name": "SVC"},
        "endpoints": [],
    }
    result = runner.invoke(["feature", "online-service", "status"])
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

    The banner now arrives on ``_display`` (the manager built it via
    ``decl_api.format_status_display``); the command only routes it.
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
        "_display": ("\n" + "=" * 80 + "\n  Feature Store — Online Service Status\n"),
    }
    result = runner.invoke(["feature", "online-service", "status"])
    assert result.exit_code == 0, result.output

    # The rich display banner is the only payload we expect.
    assert "Feature Store — Online Service Status" in result.output

    # The duplicate key/value table is recognisable by the explicit
    # column separators and the raw timestamp values it surfaced.
    assert "| status" not in result.output
    assert "| message" not in result.output
    assert "| created_at" not in result.output
    assert "| updated_at" not in result.output
    # The endpoints column previously rendered the raw list-of-dicts;
    # check the JSON-ish form does not leak through.
    assert "[{'name'" not in result.output


_FULL_STATUS_DICT = {
    "status": "RUNNING",
    "message": "running",
    "runtime_id": "rt-x",
    "endpoints": [
        {"name": "ingest", "url": "https://ingest.example.snowflakecomputing.app"},
    ],
    "compute_pool": {
        "status": "ACTIVE",
        "name": "POOL_X",
        "instance_family": "CPU_X64_XS",
        "active_nodes": 1,
        "total_nodes": 1,
        "min_nodes": 1,
        "max_nodes": 2,
        "auto_suspend_secs": 300,
    },
    "postgres": {
        "status": "READY",
        "name": "PG_X",
        "compute_family": "PG_S",
        "postgres_version": "15.4",
        "storage_gb": 50,
        "host": "pg.host",
    },
    "service": {
        "status": "RUNNING",
        "name": "SVC_X",
        "image_version": "1.2.3",
        "compute_pool_name": "POOL_X",
        "current_instances": 1,
        "min_instances": 1,
        "max_instances": 2,
    },
    "network_rules": [
        {
            "name": "RULE_X",
            "mode": "EGRESS",
            "type": "HOST_PORT",
            "purpose": "DEMO",
            "value_list": "example.com:443",
        },
    ],
    "secret": {"name": "SECRET_X", "username": "user_x", "password": "abcdef1234"},
}


# Compact / verbose banners the manager (``format_status_display``) builds and
# hands back on ``_display``. The command only routes them, so the command-level
# tests source the asserted substrings from these fakes; the real compact-vs-
# verbose layout is pinned on the manager's ``verbose=`` forwarding test.
_COMPACT_DISPLAY = (
    "\n" + "=" * 80 + "\n  Feature Store — Online Service Status\n"
    "  + Online Service\n"
    "  + Compute Pool                ACTIVE\n"
    "  + Postgres                    READY\n"
    "  + Service                     RUNNING\n"
    "  Endpoints:\n"
    "     ingest: https://ingest.example.snowflakecomputing.app\n"
)
_VERBOSE_DISPLAY = _COMPACT_DISPLAY + (
    "     Family:            CPU_X64_XS\n"
    "     Postgres:          PG 15.4\n"
    "     Image:             1.2.3\n"
    "  Network Rules:\n"
    "     RULE_X\n"
)


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_default_omits_verbose_detail(mock_manager, runner):
    """Bare ``online-service`` routes the manager's default-compact banner —
    heading summaries plus endpoints, no per-component detail rows, no
    Network Rules / Secret blocks, no masked Postgres connection string.

    The layout itself is the manager's job (``format_status_display(...,
    verbose=False)``); here we assert the command prints that banner
    verbatim and injects nothing of its own.
    """
    status = dict(_FULL_STATUS_DICT)
    status["_display"] = _COMPACT_DISPLAY
    mock_manager.return_value.get_status.return_value = status
    result = runner.invoke(["feature", "online-service", "status"])
    assert result.exit_code == 0, result.output

    # Heading-only summary rows stay; banner stays; endpoints stay.
    assert "Compute Pool" in result.output
    assert "Postgres" in result.output
    assert "Service" in result.output
    assert "https://ingest.example.snowflakecomputing.app" in result.output

    # Verbose-only fields and blocks must NOT appear.
    assert "CPU_X64_XS" not in result.output
    assert "PG 15.4" not in result.output
    assert "1.2.3" not in result.output
    assert "RULE_X" not in result.output
    assert "SECRET_X" not in result.output
    assert "postgresql://" not in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_short_verbose_flag_renders_detail(mock_manager, runner):
    """``online-service -v`` routes the manager's verbose banner: per-
    component detail rows and Network Rules. The Secret / masked Postgres
    string are never emitted.
    """
    status = dict(_FULL_STATUS_DICT)
    status["_display"] = _VERBOSE_DISPLAY
    mock_manager.return_value.get_status.return_value = status
    result = runner.invoke(["feature", "online-service", "status", "-v"])
    assert result.exit_code == 0, result.output

    assert "CPU_X64_XS" in result.output
    assert "PG 15.4" in result.output
    assert "1.2.3" in result.output
    assert "RULE_X" in result.output
    assert "SECRET_X" not in result.output
    assert "postgresql://" not in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_long_verbose_flag_renders_detail(mock_manager, runner):
    """``--verbose`` is an alias for ``-v``."""
    status = dict(_FULL_STATUS_DICT)
    status["_display"] = _VERBOSE_DISPLAY
    mock_manager.return_value.get_status.return_value = status
    result = runner.invoke(["feature", "online-service", "status", "--verbose"])
    assert result.exit_code == 0, result.output
    assert "RULE_X" in result.output
    assert "SECRET_X" not in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_error_exits_nonzero(mock_manager, runner):
    """When ``get_status`` returns an ``error`` envelope (unreachable
    manifest / failed status query) the bare status read is a terminal
    failure: it exits non-zero and still surfaces the error text (the
    ``CliError`` summary the mixed-stream runner captures).
    """
    mock_manager.return_value.get_status.return_value = {
        "status": "error",
        "error": "Something went wrong",
    }
    result = runner.invoke(["feature", "online-service", "status"])
    assert result.exit_code != 0, result.output
    assert "Something went wrong" in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_json_returns_nested_status(mock_manager, runner):
    """``online-service --format json`` returns the parsed status as a
    nested JSON object on stdout and suppresses the rich banner.

    Pre-fix the success path wrote the banner to stderr and returned an
    empty ``MessageResult`` — so ``--format json`` produced
    ``{"message": ""}`` while the banner still printed.  The fix returns
    an ``ObjectResult`` of the parsed status (nested ``endpoints`` /
    ``compute_pool`` intact) and skips the free-form banner entirely
    when a structured format is active.
    """
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "message": "running",
        "endpoints": [
            {"name": "ingest", "url": "https://ingest.example.snowflakecomputing.app"},
        ],
        "compute_pool": {"status": "ACTIVE", "name": "POOL"},
        "postgres": {"status": "READY", "name": "PG"},
        "service": {"status": "RUNNING", "name": "SVC"},
        "_display": "SENTINEL_STATUS_BANNER",
        "_user": "test_user",
        "_database": "TEST_DB",
        "_schema": "TEST_SCHEMA",
    }
    result = runner.invoke(["feature", "online-service", "status", "--format", "json"])
    assert result.exit_code == 0, result.output

    # No free-form banner anywhere.
    assert "Feature Store — Online Service Status" not in result.output
    assert "SENTINEL_STATUS_BANNER" not in result.output

    payload = _json_from_output(result.output)
    assert payload["status"] == "RUNNING"
    assert isinstance(payload["endpoints"], list)
    assert payload["endpoints"][0]["url"].startswith("https://")
    assert isinstance(payload["compute_pool"], dict)
    # Private display-only keys must never leak into structured stdout.
    assert "_display" not in payload
    assert "_user" not in payload
    assert "_database" not in payload
    assert "_schema" not in payload


@mock.patch(FEATURE_MANAGER)
def test_online_service_csv_suppresses_banner(mock_manager, runner):
    """``online-service --format csv`` also suppresses the rich banner
    (any structured format skips the free-form text)."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "message": "running",
        "endpoints": [],
        "compute_pool": {"status": "ACTIVE", "name": "POOL"},
        "postgres": {"status": "READY", "name": "PG"},
        "service": {"status": "RUNNING", "name": "SVC"},
        "_display": "SENTINEL_STATUS_BANNER",
        "_user": "test_user",
        "_database": "TEST_DB",
        "_schema": "TEST_SCHEMA",
    }
    result = runner.invoke(["feature", "online-service", "status", "--format", "csv"])
    assert result.exit_code == 0, result.output
    assert "Feature Store — Online Service Status" not in result.output
    assert "SENTINEL_STATUS_BANNER" not in result.output
    assert "status" in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_error_json_returns_object(mock_manager, runner):
    """The error envelope surfaces as a nested JSON object under
    ``--format json`` (not an empty message), and the bare-status failure
    exits non-zero per the terminal-failure contract."""
    mock_manager.return_value.get_status.return_value = {
        "status": "error",
        "error": "Something went wrong",
    }
    result = runner.invoke(["feature", "online-service", "status", "--format", "json"])
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "error"
    assert payload["error"] == "Something went wrong"


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_writes_manager_display_in_table(mock_manager, runner):
    """TABLE ``online-service`` prints the banner the manager built on
    ``result['_display']`` verbatim; the command no longer formats status
    itself. Mirrors the ``describe`` adapter shape."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "_display": "SENTINEL_STATUS_BANNER",
    }
    result = runner.invoke(["feature", "online-service", "status"])
    assert result.exit_code == 0, result.output
    assert "SENTINEL_STATUS_BANNER" in result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_status_json_suppresses_manager_display(mock_manager, runner):
    """``--format json`` drops the display-only ``_display`` banner so it
    never leaks into structured stdout."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "_display": "SENTINEL_STATUS_BANNER",
    }
    result = runner.invoke(["feature", "online-service", "status", "--format", "json"])
    assert result.exit_code == 0, result.output
    assert "SENTINEL_STATUS_BANNER" not in result.output
    payload = _json_from_output(result.output)
    assert payload["status"] == "RUNNING"
    assert "_display" not in payload


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_already_running_is_noop(mock_manager, runner):
    """online-service --create should be a no-op when status is already RUNNING."""
    mock_manager.return_value.get_status.return_value = {"status": "RUNNING"}
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service already initialized",
    }
    result = runner.invoke(["feature", "online-service", "create"])
    assert result.exit_code == 0, result.output


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_and_polls(mock_manager, runner):
    """online-service --create should create the runtime and poll until RUNNING."""
    mock_manager.return_value.get_status.return_value = {"status": "STOPPED"}
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service initialized successfully",
    }
    result = runner.invoke(["feature", "online-service", "create"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.initialize_service.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_request_error_is_terminal_failure(mock_manager, runner):
    """A failed CREATE request (``initialize_service`` returns an ``error``
    envelope: bad role, missing privileges, warehouse unavailable) exits
    non-zero, keeps the envelope on structured stdout, and never enters the
    poll loop so ``--create && apply`` cannot proceed against a runtime that
    was never created."""
    mock_manager.return_value.get_status.return_value = {"status": "STOPPED"}
    mock_manager.return_value.initialize_service.return_value = {
        "status": "error",
        "error": "Insufficient privileges to operate on schema",
    }
    result = runner.invoke(["feature", "online-service", "create", "--format", "json"])
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "error"
    assert "Insufficient privileges" in payload["error"]
    # The wait loop never starts: ``get_status`` is only the pre-create probe.
    mock_manager.return_value.get_status.assert_called_once()


# --- --create wait loop: terminal statuses / swallowed errors / timeout ---
#
# The wait after ``initialize_service`` must break on a terminal runtime
# status (RUNNING success; FAILED / SUSPENDED / uppercase ERROR failure),
# debug-log swallowed poll errors with a consecutive-failure cap, and fail
# the command (non-zero exit) on timeout instead of silently exiting 0.


@contextmanager
def _dummy_service_wait_progress(*args, **kwargs):
    """Stand-in for the Rich wait bar so tests don't pull in Rich's own
    ``time`` usage (which would fight the patched clock)."""
    yield mock.Mock()


def _advancing_clock(step: float = 60.0, start: float = 0.0):
    """A monotonic() replacement that advances ``step`` seconds per call.

    Never exhausts, so any incidental caller keeps getting increasing
    values; with ``step=60`` the 600s wait budget yields ~9 poll
    iterations before the deadline.
    """
    state = {"t": start}

    def _now() -> float:
        v = state["t"]
        state["t"] += step
        return v

    return _now


_WAIT_PATCH_PROGRESS = mock.patch(
    "snowflake.cli._plugins.feature.commands._service_wait_progress",
    new=_dummy_service_wait_progress,
)


@mock.patch("time.sleep")
@mock.patch(FEATURE_MANAGER)
def test_online_service_create_pending_then_running_succeeds(
    mock_manager, mock_sleep, runner
):
    """A ramp-up (PENDING) that resolves to RUNNING is a success (exit 0)."""
    mock_manager.return_value.get_status.side_effect = [
        {"status": "STOPPED"},  # pre-create probe
        {"status": "PENDING", "message": "starting"},
        {"status": "RUNNING", "message": "up"},
    ]
    mock_manager.return_value.initialize_service.return_value = {
        "status": "CREATING",
        "message": "Create requested",
    }
    with _WAIT_PATCH_PROGRESS, mock.patch("time.monotonic", new=_advancing_clock()):
        result = runner.invoke(["feature", "online-service", "create"])
    assert result.exit_code == 0, result.output
    # pre-probe + two poll iterations (PENDING, RUNNING).
    assert mock_manager.return_value.get_status.call_count == 3


@pytest.mark.parametrize("terminal", ["FAILED", "SUSPENDED", "ERROR"])
@mock.patch("time.sleep")
@mock.patch(FEATURE_MANAGER)
def test_online_service_create_breaks_on_terminal_status(
    mock_manager, mock_sleep, runner, terminal
):
    """A terminal runtime status aborts the wait immediately (non-zero
    exit, envelope carries the observed status) rather than burning the
    full 600s budget down to a generic timeout."""
    mock_manager.return_value.get_status.side_effect = [
        {"status": "STOPPED"},  # pre-create probe
        {"status": terminal, "message": "boom"},
    ]
    mock_manager.return_value.initialize_service.return_value = {
        "status": "CREATING",
    }
    with _WAIT_PATCH_PROGRESS, mock.patch("time.monotonic", new=_advancing_clock()):
        result = runner.invoke(
            ["feature", "online-service", "create", "--format", "json"]
        )
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == terminal
    # Broke on the first poll — did not sleep through the whole budget.
    assert mock_sleep.call_count == 1


@mock.patch("time.sleep")
@mock.patch(FEATURE_MANAGER)
def test_online_service_create_transient_exception_then_running(
    mock_manager, mock_sleep, runner
):
    """A raised poll (swallowed + debug-logged) that then recovers to
    RUNNING still succeeds — a single blip does not abort."""
    mock_manager.return_value.get_status.side_effect = [
        {"status": "STOPPED"},  # pre-create probe
        RuntimeError("transient network blip"),
        {"status": "RUNNING", "message": "up"},
    ]
    mock_manager.return_value.initialize_service.return_value = {"status": "CREATING"}
    with _WAIT_PATCH_PROGRESS, mock.patch("time.monotonic", new=_advancing_clock()):
        result = runner.invoke(["feature", "online-service", "create"])
    assert result.exit_code == 0, result.output


@mock.patch("time.sleep")
@mock.patch(FEATURE_MANAGER)
def test_online_service_create_consecutive_exceptions_abort_as_error(
    mock_manager, mock_sleep, runner
):
    """Hitting the consecutive-failure cap aborts with ``error`` (not a
    fake ``timeout``) well before the deadline."""
    mock_manager.return_value.get_status.side_effect = [
        {"status": "STOPPED"},  # pre-create probe
        RuntimeError("blip 1"),
        RuntimeError("blip 2"),
        RuntimeError("blip 3"),
        RuntimeError("blip 4"),
        RuntimeError("blip 5"),
    ]
    mock_manager.return_value.initialize_service.return_value = {"status": "CREATING"}
    with _WAIT_PATCH_PROGRESS, mock.patch("time.monotonic", new=_advancing_clock()):
        result = runner.invoke(
            ["feature", "online-service", "create", "--format", "json"]
        )
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "error"


@mock.patch("time.sleep")
@mock.patch(FEATURE_MANAGER)
def test_online_service_create_consecutive_error_envelopes_abort(
    mock_manager, mock_sleep, runner
):
    """A run of manager query-failure envelopes (lowercase ``error``)
    also counts toward the cap and aborts as ``error``."""
    mock_manager.return_value.get_status.return_value = {
        "status": "error",
        "error": "insufficient privileges",
    }
    mock_manager.return_value.initialize_service.return_value = {"status": "CREATING"}
    with _WAIT_PATCH_PROGRESS, mock.patch("time.monotonic", new=_advancing_clock()):
        result = runner.invoke(
            ["feature", "online-service", "create", "--format", "json"]
        )
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "error"


@mock.patch("time.sleep")
@mock.patch(FEATURE_MANAGER)
def test_online_service_create_timeout_is_terminal_failure(
    mock_manager, mock_sleep, runner
):
    """A service stuck in-flight until the deadline yields ``timeout`` AND
    a non-zero exit (was exit 0 before the fix)."""
    mock_manager.return_value.get_status.return_value = {"status": "PENDING"}
    mock_manager.return_value.initialize_service.return_value = {"status": "CREATING"}
    with _WAIT_PATCH_PROGRESS, mock.patch("time.monotonic", new=_advancing_clock()):
        result = runner.invoke(
            ["feature", "online-service", "create", "--format", "json"]
        )
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "timeout"


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop(mock_manager, runner):
    """online-service --drop should drop OFTs then call FeatureManager.destroy_service."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "destroyed",
        "dropped_ofts": ["TABLE_A", "TABLE_B"],
    }
    result = runner.invoke(["feature", "online-service", "drop", "--force"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.destroy_service.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_partial_failure_is_terminal(mock_manager, runner):
    """``--drop`` returning ``partial_failure`` (some OFT dropped, some errored)
    must exit non-zero and keep the full envelope on structured stdout."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "partial_failure",
        "dropped_ofts": ["TABLE_B"],
        "errors": ["TABLE_A: boom"],
    }
    result = runner.invoke(
        ["feature", "online-service", "drop", "--force", "--format", "json"]
    )
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "partial_failure"
    assert payload["dropped_ofts"] == ["TABLE_B"]
    assert payload["errors"] == ["TABLE_A: boom"]


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_failed_is_terminal(mock_manager, runner):
    """``--drop`` returning ``failed`` (nothing dropped, all errored) must
    exit non-zero and keep the full envelope on structured stdout."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "failed",
        "dropped_ofts": [],
        "errors": ["show OFTs: boom", "drop_runtime: boom"],
    }
    result = runner.invoke(
        ["feature", "online-service", "drop", "--force", "--format", "json"]
    )
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "failed"
    assert payload["dropped_ofts"] == []
    assert payload["errors"] == ["show OFTs: boom", "drop_runtime: boom"]


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_no_manifest_is_hard_error(mock_manager, runner):
    """``--drop --force`` from a manifest-less directory must fail: the strict
    ``destroy_service`` raises a ``CliError`` rather than resolving to the
    connection, so no runtime is torn down at whatever the connection points
    at."""
    from snowflake.cli.api.exceptions import CliError

    mock_manager.return_value.destroy_service.side_effect = CliError(
        "Could not locate manifest.yml starting from /tmp/nowhere"
    )
    result = runner.invoke(["feature", "online-service", "drop", "--force"])
    assert result.exit_code != 0, result.output
    assert "manifest.yml" in result.output


# --- --drop confirmation gate (DCM purge parity) ----------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_no_interactive_without_force_refuses(mock_manager, runner):
    """``--drop --no-interactive`` without ``--force`` must refuse before any
    teardown (``destroy_service`` is never called), matching ``dcm purge``."""
    result = runner.invoke(["feature", "online-service", "drop", "--no-interactive"])
    assert result.exit_code != 0, result.output
    assert (
        "Cannot drop the online service non-interactively without --force"
        in result.output
    )
    mock_manager.return_value.destroy_service.assert_not_called()


@mock.patch("snowflake.cli.api.commands.flags.is_tty_interactive", return_value=False)
@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_default_non_tty_without_force_refuses(
    mock_manager, mock_is_tty, runner
):
    """A bare ``--drop`` in a non-TTY (CI / pipe) without ``--force`` refuses,
    just like ``dcm purge`` defaults to non-interactive off a terminal."""
    result = runner.invoke(["feature", "online-service", "drop"])
    assert result.exit_code != 0, result.output
    assert (
        "Cannot drop the online service non-interactively without --force"
        in result.output
    )
    mock_manager.return_value.destroy_service.assert_not_called()


@mock.patch("snowflake.cli._plugins.feature.commands._confirm_drop")
@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_force_skips_confirmation(
    mock_manager, mock_confirm_drop, runner
):
    """``--drop --force`` (even ``--no-interactive``) proceeds without prompting."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "destroyed",
        "dropped_ofts": [],
    }
    result = runner.invoke(
        ["feature", "online-service", "drop", "--force", "--no-interactive"]
    )
    assert result.exit_code == 0, result.output
    mock_confirm_drop.assert_not_called()
    mock_manager.return_value.destroy_service.assert_called_once()


@mock.patch("snowflake.cli.api.commands.flags.is_tty_interactive", return_value=True)
@mock.patch("snowflake.cli._plugins.feature.commands.typer.prompt")
@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_interactive_confirm_proceeds(
    mock_manager, mock_prompt, mock_is_tty, runner
):
    """Interactive default: a typed ``drop DATABASE.SCHEMA`` confirmation
    proceeds to ``destroy_service`` (location comes from the resolved target)."""
    mock_manager.return_value._resolve_service_target.return_value = (  # noqa: SLF001
        "MYDB",
        "MYSCHEMA",
        None,
    )
    mock_manager.return_value.destroy_service.return_value = {
        "status": "destroyed",
        "dropped_ofts": [],
    }
    mock_prompt.return_value = "drop MYDB.MYSCHEMA"

    result = runner.invoke(["feature", "online-service", "drop"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.destroy_service.assert_called_once()


@mock.patch("snowflake.cli.api.commands.flags.is_tty_interactive", return_value=True)
@mock.patch("snowflake.cli._plugins.feature.commands.typer.prompt")
@mock.patch(FEATURE_MANAGER)
def test_online_service_drop_interactive_cancel_aborts(
    mock_manager, mock_prompt, mock_is_tty, runner
):
    """Typing ``cancel`` at the confirmation aborts before ``destroy_service``."""
    mock_manager.return_value._resolve_service_target.return_value = (  # noqa: SLF001
        "MYDB",
        "MYSCHEMA",
        None,
    )
    mock_prompt.return_value = "cancel"

    result = runner.invoke(["feature", "online-service", "drop"])
    assert result.exit_code != 0, result.output
    mock_manager.return_value.destroy_service.assert_not_called()


def test_online_service_bare_group_lists_subcommands(runner):
    """Bare ``online-service`` prints group help listing the subcommands.

    ``--create`` / ``--drop`` are now alternate verbs (``create`` /
    ``drop``), not flags on one command, so the group is a nested
    sub-typer and a bare invocation shows the subcommand list.
    """
    result = runner.invoke(["feature", "online-service"])
    assert result.exit_code == 0, result.output
    assert "create" in result.output
    assert "drop" in result.output
    assert "status" in result.output


def test_online_service_removed_create_flag_rejected(runner):
    """The old ``--create`` flag is gone: ``online-service`` is a group now,
    so a bare ``--create`` is an unknown option / subcommand (non-zero)."""
    result = runner.invoke(["feature", "online-service", "--create"])
    assert result.exit_code != 0, result.output


def test_online_service_status_help_works_without_library(runner, monkeypatch):
    """``online-service status --help`` works even when the ML library is
    absent: ``--help`` is eager and short-circuits before the per-command
    preflight guard runs."""
    monkeypatch.setattr(
        "snowflake.cli._plugins.feature.manager.decl_api", None, raising=False
    )
    monkeypatch.setattr(
        "snowflake.cli._plugins.feature.manager._SNOWML_IMPORT_ERROR",
        ImportError("No module named 'snowflake.ml.feature_store'"),
    )
    result = runner.invoke(["feature", "online-service", "status", "--help"])
    assert result.exit_code == 0, result.output
    assert "--from" in result.output


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
            "status",
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
    """Bare ``online-service`` (no flags) passes ``from_dir=SecurePath.cwd()``
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
    result = runner.invoke(["feature", "online-service", "status"])
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
            "create",
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
            "drop",
            "--force",
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


@mock.patch(FEATURE_MANAGER)
def test_ingest_help_omits_variable_flag(mock_manager, runner):
    """``ingest --help`` does not surface ``--variable``: ingest streams
    records through the Online Service and never loads or renders local
    specs, so a templating override is meaningless here."""
    result = runner.invoke(["feature", "ingest", "--help"])
    assert result.exit_code == 0, result.output
    assert "--variable" not in result.output.lower()


@mock.patch(FEATURE_MANAGER)
def test_ingest_rejects_variable_flag(mock_manager, runner):
    """``ingest SOURCE -D env=prod`` fails rather than silently discarding
    the override."""
    result = runner.invoke(
        ["feature", "ingest", "my_source", "-D", "env=prod"],
        input="[]",
    )
    assert result.exit_code != 0, result.output


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

    The library's ``FeatureStore.get_feature_view(name, version)``
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


@mock.patch(FEATURE_MANAGER)
def test_query_help_omits_variable_flag(mock_manager, runner):
    """``query --help`` does not surface ``--variable``: query performs an
    online lookup and never loads or renders local specs, so a templating
    override is meaningless here."""
    result = runner.invoke(["feature", "query", "--help"])
    assert result.exit_code == 0, result.output
    assert "--variable" not in result.output.lower()


@mock.patch(FEATURE_MANAGER)
def test_query_rejects_variable_flag(mock_manager, runner):
    """``query VIEW -D env=prod`` fails rather than silently discarding the
    override."""
    result = runner.invoke(
        [
            "feature",
            "query",
            "my_view",
            "--version",
            "V1",
            "--keys",
            '[{"id": "1"}]',
            "-D",
            "env=prod",
        ]
    )
    assert result.exit_code != 0, result.output


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
    assert "--python" in text
    assert "--yaml" in text

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
def test_init_default_python_is_true(mock_manager, runner):
    """``snow feature init`` defaults to the Python export form (``python=True``)."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.init.call_args.kwargs["python"] is True


@mock.patch(FEATURE_MANAGER)
def test_init_python_flag(mock_manager, runner):
    """``--python`` (explicit alias for the default) threads ``python=True``."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init", "--python"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.init.call_args.kwargs["python"] is True


@mock.patch(FEATURE_MANAGER)
def test_init_yaml_flag(mock_manager, runner):
    """``--yaml`` threads ``python=False`` to the manager."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init", "--yaml"])
    assert result.exit_code == 0, result.output
    assert mock_manager.return_value.init.call_args.kwargs["python"] is False


@mock.patch(FEATURE_MANAGER)
def test_init_python_and_yaml_conflict(mock_manager, runner):
    """Passing both ``--python`` and ``--yaml`` is a bad-flag error.

    Standardized on ``CliArgumentError``: exit 1 by default (the
    ``BaseCliError`` default when ``--enhanced-exit-codes`` is off).
    """
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(["feature", "init", "--python", "--yaml"])
    assert result.exit_code == 1, result.output
    assert "--python" in result.output and "--yaml" in result.output
    mock_manager.return_value.init.assert_not_called()


@mock.patch(FEATURE_MANAGER)
def test_init_python_and_yaml_conflict_enhanced_exit_code(mock_manager, runner):
    """With ``--enhanced-exit-codes`` the bad-flag error is exit 2."""
    mock_manager.return_value.init.return_value = _INIT_RESULT_STUB
    result = runner.invoke(
        ["feature", "init", "--python", "--yaml", "--enhanced-exit-codes"]
    )
    assert result.exit_code == 2, result.output
    mock_manager.return_value.init.assert_not_called()


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


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------

_SYNC_RESULT_STUB = {
    "status": "synced",
    "directory": "/tmp/proj/sources",
    "files": [],
    "target_database": "DB",
    "target_schema": "SCH",
}


@mock.patch(FEATURE_MANAGER)
def test_sync_default_flags(mock_manager, runner):
    """``snow feature sync`` with no extra flags calls ``FeatureManager().sync()``
    with ``from_dir=SecurePath.cwd()``, ``target_name=None``, ``name_filter=None``,
    and ``python=True`` (Python is the default export form).
    """
    mock_manager.return_value.sync.return_value = _SYNC_RESULT_STUB
    result = runner.invoke(["feature", "sync"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.sync.assert_called_once()
    kwargs = mock_manager.return_value.sync.call_args.kwargs
    assert isinstance(kwargs["from_dir"], SecurePath)
    assert kwargs["from_dir"] == Path.cwd()
    assert kwargs["target_name"] is None
    assert kwargs["name_filter"] is None
    assert kwargs["python"] is True


@mock.patch(FEATURE_MANAGER)
def test_sync_name_flag(mock_manager, runner):
    """``--name MY_FV`` threads ``name_filter="MY_FV"`` to the manager."""
    mock_manager.return_value.sync.return_value = _SYNC_RESULT_STUB
    result = runner.invoke(["feature", "sync", "--name", "MY_FV"])
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.sync.call_args.kwargs
    assert kwargs["name_filter"] == "MY_FV"


@mock.patch(FEATURE_MANAGER)
def test_sync_python_flag(mock_manager, runner):
    """``--python`` (explicit alias for the default) threads ``python=True``."""
    mock_manager.return_value.sync.return_value = _SYNC_RESULT_STUB
    result = runner.invoke(["feature", "sync", "--python"])
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.sync.call_args.kwargs
    assert kwargs["python"] is True


@mock.patch(FEATURE_MANAGER)
def test_sync_yaml_flag(mock_manager, runner):
    """``--yaml`` threads ``python=False`` to the manager."""
    mock_manager.return_value.sync.return_value = _SYNC_RESULT_STUB
    result = runner.invoke(["feature", "sync", "--yaml"])
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.sync.call_args.kwargs
    assert kwargs["python"] is False


@mock.patch(FEATURE_MANAGER)
def test_sync_python_and_yaml_conflict(mock_manager, runner):
    """Passing both ``--python`` and ``--yaml`` is a bad-flag error (exit 1)."""
    mock_manager.return_value.sync.return_value = _SYNC_RESULT_STUB
    result = runner.invoke(["feature", "sync", "--python", "--yaml"])
    assert result.exit_code == 1, result.output
    assert "--python" in result.output and "--yaml" in result.output
    mock_manager.return_value.sync.assert_not_called()


@mock.patch(FEATURE_MANAGER)
def test_sync_from_flag(mock_manager, runner, tmp_path):
    """``--from /some/path`` threads ``from_dir`` as a ``SecurePath`` to the manager."""
    mock_manager.return_value.sync.return_value = _SYNC_RESULT_STUB
    result = runner.invoke(["feature", "sync", "--from", str(tmp_path)])
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.sync.call_args.kwargs
    assert kwargs["from_dir"] == tmp_path


@mock.patch(FEATURE_MANAGER)
def test_sync_target_flag(mock_manager, runner):
    """``--target PROD`` threads ``target_name="PROD"`` to the manager."""
    mock_manager.return_value.sync.return_value = _SYNC_RESULT_STUB
    result = runner.invoke(["feature", "sync", "--target", "PROD"])
    assert result.exit_code == 0, result.output
    kwargs = mock_manager.return_value.sync.call_args.kwargs
    assert kwargs["target_name"] == "PROD"


@mock.patch(FEATURE_MANAGER)
def test_sync_help_text(mock_manager, runner):
    """``--name``, ``--python``, ``--yaml``, ``--target``, and ``--from`` appear in help."""
    result = runner.invoke(["feature", "sync", "--help"])
    assert result.exit_code == 0, result.output
    text = result.output
    assert "--name" in text
    assert "--python" in text
    assert "--yaml" in text
    assert "--target" in text
    assert "--from" in text


# ---------------------------------------------------------------------------
# Readable diagnostics (errors / warnings) — plan validation_failed + apply
# ---------------------------------------------------------------------------
#
# On ``validation_failed`` the plan envelope carries ``errors`` /
# ``warnings`` as lists of ``ValidationResult`` objects.  Rendering
# them into a single key-value TABLE cell wrapped an unreadable blob;
# the CLI now lifts them onto stderr as one readable finding per line
# and compacts the stdout payload.  JSON / CSV callers keep the full
# arrays (sanitized to plain dicts) untouched.


def _tiling_error():
    """A ``ValidationResult``-shaped finding for the diagnostics tests."""
    return SimpleNamespace(
        severity="ERROR",
        code="STREAM_FV_TILING_REFRESH",
        message=(
            "USER_CLICK_BACKFILL_DECL: tiled StreamingFeatureView requires "
            "``refresh_freq`` so the offline tile Dynamic Table has a refresh "
            "cadence. It drives the Online Feature Table's ingest-path lag."
        ),
        object_name="USER_CLICK_BACKFILL_DECL",
    )


def test_print_diagnostics_reformats_findings_readably(capsys):
    """``_print_diagnostics`` renders ``OBJECT  [CODE]`` + wrapped
    message on stderr from the finding's fields."""
    from snowflake.cli._plugins.feature.commands import _print_diagnostics

    _print_diagnostics(
        {
            "errors": [_tiling_error()],
            "warnings": [],
        }
    )
    captured = capsys.readouterr()
    assert "Errors (1):" in captured.err
    assert "USER_CLICK_BACKFILL_DECL" in captured.err
    assert "[STREAM_FV_TILING_REFRESH]" in captured.err
    # The reformatted output must drop any raw repr scaffolding.
    assert "severity='ERROR'" not in captured.err
    assert "object_name=" not in captured.err
    assert captured.out == ""


def test_print_diagnostics_plain_string_fallback(capsys):
    """Non-finding strings (e.g. init/sync warnings) fall back to a
    plain ``- <text>`` bullet."""
    from snowflake.cli._plugins.feature.commands import _print_diagnostics

    _print_diagnostics({"warnings": ["orphaned OFT skipped during export"]})
    captured = capsys.readouterr()
    assert "Warnings (1):" in captured.err
    assert "  - orphaned OFT skipped during export" in captured.err


def test_print_diagnostics_silent_when_empty(capsys):
    """No errors and no warnings → no stderr output at all."""
    from snowflake.cli._plugins.feature.commands import _print_diagnostics

    _print_diagnostics({"errors": [], "warnings": []})
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


# ---------------------------------------------------------------------------
# Terminal-output sanitization (conventions.md): server- / config-sourced
# object names, statuses, and error strings are stripped of ANSI / control
# sequences at the shared stderr printers and CliError summaries, not at each
# call site (cf. DCM's ``_check_account_identifier``).
# ---------------------------------------------------------------------------


def test_print_diagnostics_sanitizes_finding_fields(capsys):
    """A finding whose object_name / code / message carry ANSI escapes is
    stripped before hitting stderr; the raw CSI bytes never appear."""
    from snowflake.cli._plugins.feature.commands import _print_diagnostics

    _print_diagnostics(
        {
            "errors": [
                SimpleNamespace(
                    severity="ERROR",
                    code="BAD\x1b[31mCODE",
                    message="msg\x1b[0m body",
                    object_name="OBJ\x1b[1;31m",
                )
            ],
            "warnings": [],
        }
    )
    captured = capsys.readouterr()
    assert "\x1b" not in captured.err
    assert "OBJ" in captured.err
    assert "BADCODE" in captured.err
    assert "msg body" in captured.err


def test_print_diagnostics_sanitizes_plain_string_fallback(capsys):
    """Non-finding warning strings are sanitized on the ``- <text>`` bullet."""
    from snowflake.cli._plugins.feature.commands import _print_diagnostics

    _print_diagnostics({"warnings": ["orphaned OFT \x1b[2Jskipped"]})
    captured = capsys.readouterr()
    assert "\x1b" not in captured.err
    assert "orphaned OFT skipped" in captured.err


def test_print_warnings_sanitizes_bullets(capsys):
    """``_print_warnings`` (init/sync) strips ANSI from each warning bullet."""
    from snowflake.cli._plugins.feature.commands import _print_warnings

    _print_warnings({"warnings": ["stale \x1b[31mexport"]})
    captured = capsys.readouterr()
    assert "\x1b" not in captured.err
    assert "stale export" in captured.err


def test_envelope_error_messages_sanitizes(capsys):
    """The errors join used for the CliError summary is sanitized: both
    finding-shaped and plain-string errors lose their escape sequences."""
    from snowflake.cli._plugins.feature.commands import _envelope_error_messages

    messages = _envelope_error_messages(
        {
            "errors": [
                SimpleNamespace(
                    severity="ERROR",
                    code="C\x1b[31m1",
                    message="boom\x1b[0m",
                    object_name="OBJ\x1b[1m",
                ),
                "plain \x1b[2Jerror",
            ]
        }
    )
    joined = "\n".join(str(m) for m in messages)
    assert "\x1b" not in joined
    assert "OBJ" in joined
    assert "C1" in joined
    assert "boom" in joined
    assert "plain error" in joined


def test_terminal_failure_error_sanitizes_status_and_errors():
    """``_terminal_failure_error`` builds a CliError whose ``Status:`` line
    and error body are free of ANSI escapes."""
    from snowflake.cli._plugins.feature.commands import _terminal_failure_error

    err = _terminal_failure_error(
        {
            "status": "refused\x1b[31m",
            "error": "denied \x1b[0mhard",
        }
    )
    message = str(err)
    assert "\x1b" not in message
    assert "Status: refused" in message
    assert "denied hard" in message


def test_is_structured_output_defaults_true_when_context_unresolvable(caplog):
    """A failed CLI-context lookup must not fall back to TABLE: JSON
    consumers would then get free-form stdout instead of an envelope."""
    from snowflake.cli._plugins.feature import commands as cmds

    caplog.set_level(logging.DEBUG, logger=cmds.log.name)
    with mock.patch(
        "snowflake.cli.api.cli_global_context.get_cli_context",
        side_effect=RuntimeError("no cli context"),
    ):
        assert cmds._is_structured_output() is True  # noqa: SLF001
    assert "could not resolve CLI output format" in caplog.text
    assert "no cli context" in caplog.text


def test_print_status_header_sanitizes_status(capsys):
    """ANSI in a server-provided ``status`` never reaches stderr."""
    from snowflake.cli._plugins.feature.commands import _print_status_header

    _print_status_header({"status": "applied\x1b[31m", "ops": [], "executed": 0})
    captured = capsys.readouterr()
    assert "\x1b" not in captured.err
    assert "Status: applied" in captured.err


def test_sanitize_dict_findings_become_plain_dicts():
    """JSON / CSV callers get ``{severity, code, message, object_name}``
    dicts, not a pydantic repr string, for envelope findings."""
    from snowflake.cli._plugins.feature.commands import _sanitize_dict

    sanitized = _sanitize_dict(
        {"status": "validation_failed", "errors": [_tiling_error()], "warnings": []}
    )
    assert isinstance(sanitized["errors"], list)
    finding = sanitized["errors"][0]
    assert isinstance(finding, dict)
    assert finding["code"] == "STREAM_FV_TILING_REFRESH"
    assert finding["object_name"] == "USER_CLICK_BACKFILL_DECL"
    assert finding["severity"] == "ERROR"
    # The whole envelope must be JSON-serialisable (no pydantic reprs).
    json.dumps(sanitized)


def test_compact_failure_envelope_drops_list_cells():
    """The TABLE payload swaps the verbose ``errors`` / ``warnings`` /
    ``ops`` lists for ``error_count`` / ``warning_count`` scalars while
    keeping status + target fields."""
    from snowflake.cli._plugins.feature.commands import _compact_failure_envelope

    compact = _compact_failure_envelope(
        {
            "status": "validation_failed",
            "target_name": "PROD",
            "ops": [],
            "errors": [_tiling_error(), _tiling_error()],
            "warnings": ["w1"],
        }
    )
    assert "errors" not in compact
    assert "warnings" not in compact
    assert "ops" not in compact
    assert compact["status"] == "validation_failed"
    assert compact["target_name"] == "PROD"
    assert compact["error_count"] == 2
    assert compact["warning_count"] == 1


@mock.patch(FEATURE_MANAGER)
def test_plan_validation_failed_table_shows_readable_diagnostics(
    mock_manager, runner, tmp_path
):
    """In TABLE mode, ``plan`` prints the readable diagnostics and must
    not leak the raw ``severity='ERROR'`` list into the output."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = (
        {
            "status": "validation_failed",
            "target_name": "PROD",
            "ops": [],
            "errors": [_tiling_error()],
            "warnings": [],
        },
        None,
    )
    result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    # Terminal validation failure now exits non-zero.
    assert result.exit_code != 0, result.output
    # Readable form present; ugly repr blob absent.
    assert "Errors (1):" in result.output
    assert "[STREAM_FV_TILING_REFRESH]" in result.output
    assert "severity='ERROR'" not in result.output
    # A failed plan still must not write a plan file.
    mock_manager.return_value.write_plan_object.assert_not_called()


@mock.patch("snowflake.cli._plugins.feature.commands._is_structured_output")
@mock.patch(FEATURE_MANAGER)
def test_plan_validation_failed_structured_keeps_error_list(
    mock_manager, mock_structured, runner, tmp_path
):
    """In JSON / CSV mode, ``plan`` skips the stderr diagnostics and
    returns the untouched envelope so the ``errors`` array reaches
    machine consumers on stdout."""
    mock_structured.return_value = True
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = (
        {
            "status": "validation_failed",
            "ops": [],
            "errors": [_tiling_error()],
            "warnings": [],
        },
        None,
    )
    with mock.patch(
        "snowflake.cli._plugins.feature.commands._print_diagnostics"
    ) as mock_diag:
        result = runner.invoke(["feature", "plan", "--out", str(out_path)])
    # Terminal validation failure exits non-zero; structured mode skips the
    # stderr diagnostics (the envelope carries the arrays on stdout instead).
    assert result.exit_code != 0, result.output
    mock_diag.assert_not_called()
    mock_manager.return_value.write_plan_object.assert_not_called()


@mock.patch(FEATURE_MANAGER)
def test_plan_json_validation_failed_emits_envelope_and_fails(
    mock_manager, runner, tmp_path
):
    """``plan --format json`` on ``validation_failed`` exits non-zero and the
    stdout payload keeps ``status`` + the populated ``errors`` array (findings
    sanitized to plain dicts), never collapsing to ``Operations: 0``."""
    out_path = tmp_path / "plans" / "feature_plan_test.json"
    mock_manager.return_value.plan.return_value = (
        {
            "status": "validation_failed",
            "target_name": "PROD",
            "ops": [],
            "errors": [_tiling_error()],
            "warnings": [],
        },
        None,
    )
    result = runner.invoke(
        ["feature", "plan", "--out", str(out_path), "--format", "json"]
    )
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "validation_failed"
    assert isinstance(payload["errors"], list) and payload["errors"]
    assert payload["errors"][0]["code"] == "STREAM_FV_TILING_REFRESH"
    # No plan file is written for a failed plan.
    mock_manager.return_value.write_plan_object.assert_not_called()
    assert not out_path.exists()


@mock.patch("snowflake.cli._plugins.feature.commands._is_structured_output")
@mock.patch(FEATURE_MANAGER)
def test_apply_prints_diagnostics_when_errors_present(
    mock_manager, mock_structured, runner
):
    """``apply`` surfaces envelope ``errors`` via the readable
    diagnostics in TABLE mode (refused / validation_failed / partial)."""
    mock_structured.return_value = False
    mock_manager.return_value.apply.return_value = {
        "status": "refused",
        "ops": [
            {"operation": "RECREATE_FV", "name": "X", "status": "refused"},
        ],
        "errors": [
            "Apply refused: 1 destructive operation(s) require --allow-recreate."
        ],
        "warnings": [],
    }
    result = runner.invoke(["feature", "apply"])
    assert result.exit_code != 0, result.output
    assert "Errors (1):" in result.output
    assert "--allow-recreate" in result.output


# ---------------------------------------------------------------------------
# Terminal-failure envelopes reach the caller (status + errors + exit code)
# ---------------------------------------------------------------------------
#
# Regression guard for the ``snow feature apply --format json`` blocker: a
# terminal status (``target_mismatch`` / ``partial_failure`` / ``refused`` /
# ``validation_failed``) must (a) exit non-zero like DCM and (b) still carry
# the populated ``status`` / ``errors`` payload on structured stdout — never
# collapse to ``{"message": "Operations: 0"}`` or a bare ops array.


@mock.patch(FEATURE_MANAGER)
def test_apply_json_target_mismatch_emits_envelope_and_fails(mock_manager, runner):
    """``apply --format json`` on ``target_mismatch`` exits non-zero and the
    stdout payload keeps ``status`` + the populated ``errors`` array."""
    mock_manager.return_value.apply.return_value = {
        "target_database": "",
        "target_schema": "",
        "target_warehouse": "",
        "target_name": "PROD",
        "status": "target_mismatch",
        "ops": [],
        "executed": 0,
        "warnings": [],
        "errors": [
            "Account mismatch resolving manifest target PROD; "
            "connection reports OTHER-ACCT."
        ],
    }
    result = runner.invoke(["feature", "apply", "--format", "json"])
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "target_mismatch"
    assert isinstance(payload["errors"], list) and payload["errors"]
    assert "Account mismatch" in payload["errors"][0]
    # The old envelope-dropping behaviour must be gone.
    assert payload.get("message") != "Operations: 0"


@mock.patch(FEATURE_MANAGER)
def test_apply_json_partial_failure_emits_envelope_and_fails(mock_manager, runner):
    """``partial_failure`` (ops present) must not collapse to a bare ops
    array — the envelope with ``status`` + ``errors`` reaches the caller and
    the process exits non-zero."""
    mock_manager.return_value.apply.return_value = {
        "target_database": "DB",
        "target_schema": "SCH",
        "target_warehouse": "WH",
        "target_name": "PROD",
        "status": "partial_failure",
        "ops": [
            {"operation": "CREATE_FV", "name": "A", "status": "success"},
            {"operation": "CREATE_FV", "name": "B", "status": "failed"},
        ],
        "executed": 1,
        "warnings": [],
        "errors": ["B: TABLE collision during CREATE_FV."],
    }
    result = runner.invoke(["feature", "apply", "--format", "json"])
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert isinstance(payload, dict), payload
    assert payload["status"] == "partial_failure"
    assert "TABLE collision" in payload["errors"][0]
    # Per-op detail is still present in the structured envelope.
    assert isinstance(payload["ops"], list) and len(payload["ops"]) == 2


@mock.patch(FEATURE_MANAGER)
def test_apply_json_refused_emits_envelope_and_fails(mock_manager, runner):
    """``refused`` exits non-zero with the ``errors`` array intact."""
    mock_manager.return_value.apply.return_value = {
        "target_database": "DB",
        "target_schema": "SCH",
        "target_warehouse": "WH",
        "target_name": "PROD",
        "status": "refused",
        "ops": [{"operation": "RECREATE_FV", "name": "X", "status": "refused"}],
        "executed": 0,
        "warnings": [],
        "errors": [
            "Apply refused: 1 destructive operation(s) require --allow-recreate."
        ],
    }
    result = runner.invoke(["feature", "apply", "--format", "json"])
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "refused"
    assert "--allow-recreate" in payload["errors"][0]


@mock.patch(FEATURE_MANAGER)
def test_apply_json_no_plan_keeps_envelope_but_exits_zero(mock_manager, runner):
    """``no_plan`` stays an idle *success* (exit 0), but the JSON payload is
    the full envelope (``status`` + ``errors``) rather than the old
    ``{"message": "Operations: 0"}`` collapse."""
    mock_manager.return_value.apply.return_value = {
        "target_database": "DB",
        "target_schema": "SCH",
        "target_warehouse": "WH",
        "target_name": "PROD",
        "status": "no_plan",
        "ops": [],
        "executed": 0,
        "warnings": [],
        "errors": ["No unapplied plan file found under 'out/plan/'. Run plan first."],
    }
    result = runner.invoke(["feature", "apply", "--format", "json"])
    assert result.exit_code == 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "no_plan"
    assert payload.get("message") != "Operations: 0"
    assert isinstance(payload["errors"], list) and payload["errors"]


@mock.patch(FEATURE_MANAGER)
def test_apply_table_target_mismatch_exits_nonzero(mock_manager, runner):
    """In TABLE mode a terminal ``target_mismatch`` also exits non-zero so
    exit-code-only callers stop treating it as success."""
    mock_manager.return_value.apply.return_value = {
        "target_database": "DB",
        "target_schema": "SCH",
        "target_warehouse": "WH",
        "target_name": "PROD",
        "status": "target_mismatch",
        "ops": [],
        "executed": 0,
        "warnings": [],
        "errors": ["Plan was generated for target 'DEV' but apply used 'PROD'."],
    }
    result = runner.invoke(["feature", "apply"])
    assert result.exit_code != 0, result.output
    assert "target_mismatch" in result.output


@mock.patch(FEATURE_MANAGER)
def test_list_json_error_exits_nonzero(mock_manager, runner):
    """``list --format json`` on a backend SQL error exits non-zero while
    keeping the ``status`` / ``error`` envelope on stdout."""
    mock_manager.return_value.list_specs.return_value = {
        "status": "error",
        "error": "SQL compilation error: object does not exist",
    }
    result = runner.invoke(["feature", "list", "--format", "json"])
    assert result.exit_code != 0, result.output
    payload = _leading_json_object(result.output)
    assert payload["status"] == "error"
    assert "SQL compilation error" in payload["error"]


# ---------------------------------------------------------------------------
# Public-preview warning banner
# ---------------------------------------------------------------------------
#
# The banner is emitted by the group callback before every ``snow
# feature`` invocation.  These tests pin the exact copy, the yellow-only
# coloring, and the stderr-only surface so structured stdout stays clean.


class _FakeStderr(io.StringIO):
    """A StringIO that can pretend to be (or not be) a TTY."""

    def __init__(self, isatty: bool):
        super().__init__()
        self._isatty = isatty

    def isatty(self) -> bool:  # noqa: D401 - trivial shim
        return self._isatty


_PREVIEW_TEXT = "WARNING: 'snow feature' is in public preview."
_YELLOW = "\x1b[33m"


def test_preview_warning_exact_text_no_color_when_not_tty(monkeypatch):
    """Non-TTY stderr gets the exact banner text with no ANSI color."""
    from snowflake.cli._plugins.feature.commands import _emit_preview_warning

    fake = _FakeStderr(isatty=False)
    monkeypatch.setattr("sys.stderr", fake)
    monkeypatch.delenv("NO_COLOR", raising=False)

    _emit_preview_warning()

    assert fake.getvalue() == _PREVIEW_TEXT + "\n"


def test_preview_warning_is_yellow_on_a_color_tty(monkeypatch):
    """A color-capable TTY wraps the banner in the yellow ANSI code."""
    from snowflake.cli._plugins.feature.commands import _emit_preview_warning

    fake = _FakeStderr(isatty=True)
    monkeypatch.setattr("sys.stderr", fake)
    monkeypatch.delenv("NO_COLOR", raising=False)

    _emit_preview_warning()

    out = fake.getvalue()
    assert out.startswith(_YELLOW)
    assert _PREVIEW_TEXT in out
    assert out.endswith("\x1b[0m\n")


def test_preview_warning_respects_no_color_on_tty(monkeypatch):
    """``NO_COLOR`` suppresses the ANSI color even on a TTY."""
    from snowflake.cli._plugins.feature.commands import _emit_preview_warning

    fake = _FakeStderr(isatty=True)
    monkeypatch.setattr("sys.stderr", fake)
    monkeypatch.setenv("NO_COLOR", "1")

    _emit_preview_warning()

    assert fake.getvalue() == _PREVIEW_TEXT + "\n"


@mock.patch(FEATURE_MANAGER)
def test_preview_warning_kept_off_structured_stdout(mock_manager, runner):
    """The banner rides on stderr; ``--format json`` stdout stays parseable."""
    mock_manager.return_value.apply.return_value = {
        "status": "applied",
        "ops": [],
        "executed": 0,
    }
    result = runner.invoke(["feature", "apply", "--format", "json"])
    assert result.exit_code == 0, result.output
    assert _PREVIEW_TEXT in result.output
    # The JSON payload must still be recoverable despite the stderr preamble.
    assert _json_from_output(result.output) is not None


# ---------------------------------------------------------------------------
# Missing snowflake-ml-python[feature_store] dependency guard
# ---------------------------------------------------------------------------


_MISSING_SNOWML_TEXT = (
    "Error: 'snow feature' requires the snowflake-ml-python[feature_store] library"
)


def test_feature_command_errors_when_snowml_missing(runner, monkeypatch):
    """A subcommand fails with the exact message when the library is absent.

    The missing-library signal is owned by ``manager`` (the module that
    binds ``decl_api``); ``commands`` consults it via
    ``manager.snowml_import_error()``.  Patch the manager-side source of
    truth so both halves stay coupled.
    """
    monkeypatch.setattr(
        "snowflake.cli._plugins.feature.manager.decl_api", None, raising=False
    )
    monkeypatch.setattr(
        "snowflake.cli._plugins.feature.manager._SNOWML_IMPORT_ERROR",
        ImportError("No module named 'snowflake.ml.feature_store'"),
    )
    result = runner.invoke(["feature", "apply"])
    assert result.exit_code != 0
    # The error renders inside a Rich box that wraps the message across
    # lines; strip the box borders and collapse whitespace before matching
    # the exact copy.
    normalized = " ".join(result.output.replace("|", " ").split())
    assert _MISSING_SNOWML_TEXT in normalized


def test_feature_command_errors_when_decl_api_import_diverges(runner, monkeypatch):
    """Preflight must fail on a partial import (``decl.api`` unavailable).

    Regression for the split-brain guard: ``decl.api`` is heavier than
    ``decl.errors`` (executor / types / enums), so an api-only
    ``ImportError`` (partial install, version skew) previously slipped
    past a preflight that only probed the errors import.  The command
    then dereferenced a ``None`` ``decl_api`` and raised
    ``AttributeError: 'NoneType' object has no attribute 'load_project'``
    instead of the actionable library-required message.

    With the guard unified on the manager's ``decl_api`` binding, this
    surfaces the ``CliError`` regardless of which submodule failed.
    """
    monkeypatch.setattr(
        "snowflake.cli._plugins.feature.manager.decl_api", None, raising=False
    )
    monkeypatch.setattr(
        "snowflake.cli._plugins.feature.manager._SNOWML_IMPORT_ERROR",
        ImportError("cannot import name 'api' from 'snowflake.ml.feature_store.decl'"),
    )
    result = runner.invoke(["feature", "apply"])
    assert result.exit_code != 0
    normalized = " ".join(result.output.replace("|", " ").split())
    assert _MISSING_SNOWML_TEXT in normalized
    # The unactionable dereference symptom must never reach the operator.
    assert "AttributeError" not in result.output
    assert "load_project" not in result.output


def test_snowml_import_error_tracks_decl_api_binding():
    """``snowml_import_error()`` is ``None`` iff ``decl_api`` imported.

    Both come from the single manager try/except, so they can never
    disagree — that coupling is the whole point of the unified guard.
    """
    from snowflake.cli._plugins.feature import manager

    assert (manager.snowml_import_error() is None) == (manager.decl_api is not None)


def test_require_snowflake_ml_is_noop_when_present(monkeypatch):
    """The guard does nothing when the import succeeded (error is ``None``)."""
    from snowflake.cli._plugins.feature.commands import _require_snowflake_ml

    monkeypatch.setattr(
        "snowflake.cli._plugins.feature.manager._SNOWML_IMPORT_ERROR", None
    )
    _require_snowflake_ml()  # must not raise
