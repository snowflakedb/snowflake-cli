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

"""Exhaustive contract test for ``snow feature apply`` status surface.

The five terminal ``ApplyResult.status`` tokens — ``applied``,
``partial_failure``, ``refused``, ``target_mismatch``, ``no_plan`` —
are the complete surface for the apply path.  Phase 3+4 widens
``target_mismatch`` to fire on **either** of:

* the active connection's ``account_identifier`` ≠ the manifest
  target's ``account_identifier`` (D4 / L6-extension), or
* a ``--plan <file>`` whose envelope ``target_name`` ≠ the requested
  ``--target`` (D4-ext / L6 widened).

The plan-file lifecycle now lives under
``<project_root>/out/plan/`` (D8 relocated from
``<cwd>/.snowflake/plans/``); the discovery branch that produces
``status="no_plan"`` walks the new directory.
"""

from __future__ import annotations

import io
import json
import re
import textwrap
from pathlib import Path
from unittest import mock

import pytest

# Re-use the autouse fixtures from the main manager test module so this
# file inherits the same ``mock_cli_context`` / ``mock_decl`` /
# ``mock_build_session`` / ``mock_account_identifier`` wiring.
from tests.feature.test_manager import (  # noqa: F401
    mock_account_identifier,
    mock_build_session,
    mock_cli_context,
    mock_decl,
    mock_execute_query,
)

# The full set of terminal ``ApplyResult.status`` tokens the apply path
# can surface.  See ``docs/ARCHITECTURE.md`` § "Apply Lifecycle (L1–L7)"
# + "Apply-time --allow-recreate gate".
APPLY_STATUSES = (
    "applied",
    "partial_failure",
    "refused",
    "target_mismatch",
    "no_plan",
)

# Regex the verifier (and any other downstream consumer) can rely on.
_STATUS_LINE_RE = re.compile(
    r"^Status:[ ]+([a-z_]+)[ ]+Operations:[ ]+\d+[ ]+\(executed:[ ]+\d+\)$"
)


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
    """
)


_TWO_TARGET_MANIFEST_YAML = textwrap.dedent(
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
)


def _write_manifest(project_root: Path, *, yaml_text=_DEFAULT_MANIFEST_YAML) -> Path:
    project_root.mkdir(parents=True, exist_ok=True)
    p = project_root / "manifest.yml"
    p.write_text(yaml_text)
    return p


def _make_plan_json(
    target_database: str = "TEST_DB",
    target_schema: str = "TEST_SCHEMA",
    target_name: str = "DEFAULT",
) -> str:
    """Return a minimal valid PlanFile JSON envelope (D4-ext shape)."""
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
    """Create ``<project_root>/out/plan/`` (D8 relocated)."""
    plans_dir = project_root / "out" / "plan"
    plans_dir.mkdir(parents=True, exist_ok=True)
    return plans_dir


def _wire_plan_file(mock_decl, *, target_name="DEFAULT"):
    """Wire ``decl_api.deserialize_plan`` to return a usable PlanFile mock."""
    plan_file_obj = mock_decl.deserialize_plan.return_value
    plan_file_obj.plan = mock_decl.generate_plan.return_value
    plan_file_obj.target_database = "TEST_DB"
    plan_file_obj.target_schema = "TEST_SCHEMA"
    plan_file_obj.target_name = target_name
    return plan_file_obj


def _make_apply_result(status, *, ops=None, warnings=None, errors=None):
    result = mock.MagicMock()
    result.status = status
    result.ops = ops if ops is not None else []
    result.warnings = warnings if warnings is not None else []
    result.errors = errors if errors is not None else []
    return result


# ---------------------------------------------------------------------------
# Manager-level: every status branch must surface its token to the caller.
# ---------------------------------------------------------------------------


class TestFeatureManagerApplyStatusSet:
    """``FeatureManager.apply`` must surface every member of
    ``APPLY_STATUSES``.  Three statuses come from ``execute_plan``;
    two are manager-level shortcuts (``no_plan`` from L1,
    ``target_mismatch`` from L6 / D4-ext)."""

    @pytest.mark.parametrize("status", ("applied", "partial_failure", "refused"))
    def test_execute_plan_status_threaded_through_apply(
        self, mock_execute_query, mock_decl, tmp_path, status
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        plans_dir = _make_plans_dir(tmp_path)
        (plans_dir / "feature_plan_20260510T000000.json").write_text(_make_plan_json())
        _wire_plan_file(mock_decl)
        mock_decl.execute_plan.return_value = _make_apply_result(status)

        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["status"] == status

    def test_no_plan_status_when_out_plan_directory_empty(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """L1 + D8: empty ``<project_root>/out/plan/`` → ``status='no_plan'``."""
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
        mock_decl.execute_plan.assert_not_called()

    def test_target_mismatch_status_when_account_identifier_mismatches(
        self,
        mock_execute_query,
        mock_decl,
        mock_account_identifier,
        tmp_path,
    ):
        """L6 / D4: connection account ≠ manifest target.account_identifier
        → ``status='target_mismatch'`` *before* execute_plan runs."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.identifiers import AccountIdentifier

        _write_manifest(tmp_path)
        plans_dir = _make_plans_dir(tmp_path)
        (plans_dir / "feature_plan_20260510T000000.json").write_text(_make_plan_json())
        _wire_plan_file(mock_decl)
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
        mock_decl.execute_plan.assert_not_called()

    def test_target_mismatch_status_when_plan_target_name_disagrees(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """D4-ext: plan envelope ``target_name`` ≠ requested ``--target``
        → ``status='target_mismatch'``."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path, yaml_text=_TWO_TARGET_MANIFEST_YAML)
        plan_path = tmp_path / "prod_plan.json"
        plan_path.write_text(_make_plan_json(target_name="PROD"))
        _wire_plan_file(mock_decl, target_name="PROD")

        result = FeatureManager().apply(
            from_dir=tmp_path,
            target_name="DEV",
            plan_file=str(plan_path),
            dev_mode=False,
            allow_recreate=False,
        )
        assert result["status"] == "target_mismatch"
        mock_decl.execute_plan.assert_not_called()

    def test_apply_status_set_is_complete(
        self, mock_execute_query, mock_decl, mock_account_identifier, tmp_path
    ):
        """The five status tokens enumerated above are the complete contract.
        Adding a sixth without updating ``verify_bug_bash.sh`` would silently
        regress to a "missing output" TODO again."""
        from snowflake.cli._plugins.feature.manager import FeatureManager
        from snowflake.cli.api.identifiers import AccountIdentifier

        _write_manifest(tmp_path)
        observed: set[str] = set()

        # Three statuses come from execute_plan.
        for status in ("applied", "partial_failure", "refused"):
            plans_dir = _make_plans_dir(tmp_path)
            plan_path = (
                plans_dir / f"feature_plan_20260510T0000{len(observed):02d}.json"
            )
            plan_path.write_text(_make_plan_json())
            _wire_plan_file(mock_decl)
            mock_decl.execute_plan.return_value = _make_apply_result(status)
            r = FeatureManager().apply(
                from_dir=tmp_path,
                target_name=None,
                plan_file=None,
                dev_mode=False,
                allow_recreate=False,
            )
            observed.add(r.get("status", ""))
            for f in plans_dir.glob("feature_plan_*.json*"):
                f.unlink()

        # no_plan: empty plans dir.
        r = FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )
        observed.add(r.get("status", ""))

        # target_mismatch: account mismatch.
        plans_dir = _make_plans_dir(tmp_path)
        (plans_dir / "feature_plan_20260510T999999.json").write_text(_make_plan_json())
        _wire_plan_file(mock_decl)
        mock_account_identifier.return_value = AccountIdentifier(
            "OTHER_ORG", "OTHER_ACCT"
        )
        r = FeatureManager().apply(
            from_dir=tmp_path,
            target_name=None,
            plan_file=None,
            dev_mode=False,
            allow_recreate=False,
        )
        observed.add(r.get("status", ""))

        assert observed == set(APPLY_STATUSES), (
            f"Expected to observe exactly {set(APPLY_STATUSES)!r}; "
            f"got {observed!r}."
        )


# ---------------------------------------------------------------------------
# CLI-level: ``_print_status_header`` must emit the canonical line for
# every status, including the manager-shortcut branches.
# ---------------------------------------------------------------------------


class TestPrintStatusHeader:
    @pytest.mark.parametrize("status", APPLY_STATUSES)
    def test_emits_canonical_line_for_every_apply_status(self, status, capsys):
        from snowflake.cli._plugins.feature.commands import _print_status_header

        result = {"status": status, "ops": [{"status": "skipped"}], "executed": 0}
        _print_status_header(result)
        captured = capsys.readouterr()
        line = captured.err.strip()
        match = _STATUS_LINE_RE.match(line)
        assert match is not None
        assert match.group(1) == status

    def test_defaults_executed_to_zero_when_missing(self, capsys):
        from snowflake.cli._plugins.feature.commands import _print_status_header

        result = {"status": "no_plan", "ops": []}
        _print_status_header(result)
        line = capsys.readouterr().err.strip()
        match = _STATUS_LINE_RE.match(line)
        assert match is not None
        assert "Operations: 0" in line
        assert "executed: 0" in line

    def test_executed_counts_success_status_when_omitted(self, capsys):
        from snowflake.cli._plugins.feature.commands import _print_status_header

        result = {
            "status": "partial_failure",
            "ops": [
                {"status": "success"},
                {"status": "error"},
                {"status": "success"},
            ],
        }
        _print_status_header(result)
        line = capsys.readouterr().err.strip()
        match = _STATUS_LINE_RE.match(line)
        assert match is not None
        assert "Operations: 3" in line
        assert "executed: 2" in line

    def test_silent_when_status_missing(self, capsys):
        from snowflake.cli._plugins.feature.commands import _print_status_header

        _print_status_header({"ops": [{"status": "success"}]})
        captured = capsys.readouterr()
        assert captured.err == ""

        _print_status_header({"status": "", "ops": []})
        captured = capsys.readouterr()
        assert captured.err == ""


# ---------------------------------------------------------------------------
# Documentation contract
# ---------------------------------------------------------------------------


def test_apply_statuses_matches_architecture_doc():
    """``docs/ARCHITECTURE.md`` enumerates the apply statuses; every
    ``APPLY_STATUSES`` member must be mentioned at least once."""
    here = Path(__file__).resolve()
    doc_path = None
    for parent in [here, *here.parents][:10]:
        candidate = parent / "docs" / "ARCHITECTURE.md"
        if candidate.exists():
            doc_path = candidate
            break
    if doc_path is None:
        pytest.skip("docs/ARCHITECTURE.md not reachable from test file")

    text = doc_path.read_text()
    missing = [
        s for s in APPLY_STATUSES if f'"{s}"' not in text and f"`{s}`" not in text
    ]
    assert not missing, (
        f"APPLY_STATUSES members not mentioned in docs/ARCHITECTURE.md: " f"{missing!r}"
    )


def test_status_line_regex_matches_canonical_format():
    line = "Status: refused  Operations: 7 (executed: 0)"
    match = _STATUS_LINE_RE.match(line)
    assert match is not None
    assert match.group(1) == "refused"


def test_status_line_regex_accepts_every_apply_status():
    for status in APPLY_STATUSES:
        line = f"Status: {status}  Operations: 0 (executed: 0)"
        assert _STATUS_LINE_RE.match(line)


def test_print_status_header_writes_to_real_stderr(monkeypatch):
    import sys

    from snowflake.cli._plugins.feature.commands import _print_status_header

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stderr", buf)
    _print_status_header({"status": "applied", "ops": [], "executed": 0})
    out = buf.getvalue()
    assert _STATUS_LINE_RE.match(out.strip()) is not None
