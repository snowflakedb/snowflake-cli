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

"""Tests for the wheel-staleness preflight helper used by
``scripts/verify_bug_bash.sh``.

These tests pin the BUG_BASH §9 / §11 / §14 drift-class trap that
surfaced during 2026-05-10 bug-bash runs:

- The `snowcli_fs_test_2` conda env had a stale install of
  ``snowflake_ml_feature_store_decl`` whose ``invariants.py`` was
  dated *before* the H1 / H1-extended / H3 fix commits.
- ``verify_bug_bash.sh`` re-ran the script every time and the same
  TODOs (step 9 ``VERSION_CONFLICT`` + step 9/11/14 ``COLUMN_ADDED``)
  re-injected into ``docs/BUG_BASH.md`` because the script did not
  know it was running against a stale binary.

The preflight helper compares the mtime of the active env's
installed ``decl/invariants.py`` against the most recent
``build/snowflake_ml_feature_store_decl-*.whl`` and prints a
remediation block when the install is older than the wheel — so the
next person running ``verify_bug_bash.sh`` lands at step 1 with a
clear "rebuild your install" message instead of cascading into the
misleading planner_revalidate_identical_spec.plan.md attribution.

The helper lives at ``scripts/_check_wheel_freshness.py`` and is
both directly importable (these tests) and runnable as a script
(``python3 scripts/_check_wheel_freshness.py <install> <wheel-dir>``)
so the bash script can call it without re-implementing the mtime
comparison in shell.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Repo-root resolution — walk up from this test file to find the
# scripts/ dir.  This avoids hard-coding the developer's checkout layout.
# ---------------------------------------------------------------------------


def _repo_root() -> Path:
    """Locate the repo root by searching upward for ``scripts/verify_bug_bash.sh``.

    Returns:
        The repo root path containing ``scripts/`` and ``build/``.

    Raises:
        FileNotFoundError: If no parent contains the verify script.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "scripts" / "verify_bug_bash.sh").exists():
            return parent
    raise FileNotFoundError(
        f"Could not locate scripts/verify_bug_bash.sh upward from {here}"
    )


_REPO = _repo_root()
_PREFLIGHT = _REPO / "scripts" / "_check_wheel_freshness.py"
_PLAN_ERROR_EXTRACTOR = _REPO / "scripts" / "_extract_plan_error_code.py"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_install_and_wheel(tmp_path):
    """Build a fake ``installed/.../invariants.py`` + ``build/*.whl`` pair.

    The pair shares a parent directory so each test can set the mtimes
    independently to drive the preflight into either branch.

    Yields:
        ``(install_path, wheel_dir, wheel_path)`` triple — install_path
        is the simulated installed-package file the preflight stats;
        wheel_dir is the dir the preflight scans for ``*.whl``;
        wheel_path is the single wheel file inside ``wheel_dir``.
    """
    install_root = tmp_path / "site-packages"
    install_root.mkdir()
    install = install_root / "invariants.py"
    install.write_text("# fake installed module\n")

    wheel_dir = tmp_path / "build"
    wheel_dir.mkdir()
    wheel = wheel_dir / "snowflake_ml_feature_store_decl-0.1.0-py3-none-any.whl"
    wheel.write_text("PK\x03\x04 fake wheel bytes\n")

    yield install, wheel_dir, wheel


def _set_mtime(path: Path, ts: float) -> None:
    """Pin a file's mtime (and atime) to *ts*.

    Args:
        path: Target file.
        ts: Unix epoch seconds.
    """
    os.utime(path, (ts, ts))


def _run_preflight(*args: str) -> subprocess.CompletedProcess:
    """Invoke ``scripts/_check_wheel_freshness.py`` as a subprocess.

    Args:
        *args: Positional CLI args (install path, wheel dir).

    Returns:
        The subprocess result (rc + stdout + stderr).
    """
    return subprocess.run(
        [sys.executable, str(_PREFLIGHT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


# ---------------------------------------------------------------------------
# Acceptance criteria
# ---------------------------------------------------------------------------


class TestVerifyScriptPreflight:
    """Pin the wheel-staleness preflight contract used by
    ``scripts/verify_bug_bash.sh``.
    """

    def test_preflight_passes_when_install_is_fresher_than_wheel(
        self, fake_install_and_wheel
    ):
        """An install mtime > wheel mtime must exit 0 (no remediation
        printed).

        This is the steady-state bug-bash environment: operator
        rebuilt the wheel via ``build_wheels.sh`` and reinstalled into
        the active env immediately, so the install file's mtime is
        strictly newer than every wheel under ``build/``.
        """
        install, wheel_dir, wheel = fake_install_and_wheel
        _set_mtime(wheel, 1_000_000_000.0)
        _set_mtime(install, 1_000_000_100.0)

        result = _run_preflight(str(install), str(wheel_dir))

        assert result.returncode == 0, (
            f"expected exit 0 when install is fresher; got "
            f"rc={result.returncode!r} stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )

    def test_preflight_fails_when_install_is_stale(self, fake_install_and_wheel):
        """An install mtime < wheel mtime must exit non-zero AND emit
        the ``Installed wheel ... is older than build/`` message.

        This is the trap state — the operator built a fresh wheel but
        forgot to ``pip install --force-reinstall`` it into the active
        env, so the live ``snow`` invocation runs against the stale
        binary that produced the BUG_BASH §9 / §11 / §14 cascade.
        """
        install, wheel_dir, wheel = fake_install_and_wheel
        _set_mtime(install, 1_000_000_000.0)
        _set_mtime(wheel, 1_000_000_100.0)

        result = _run_preflight(str(install), str(wheel_dir))

        assert result.returncode != 0, (
            f"expected non-zero exit when install is stale; got "
            f"rc={result.returncode!r} stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )
        combined = result.stdout + result.stderr
        assert "Installed wheel" in combined and "is older than build/" in combined, (
            "expected 'Installed wheel ... is older than build/' message in "
            f"output; got stdout={result.stdout!r} stderr={result.stderr!r}"
        )

    def test_preflight_remediation_command_is_correct(self, fake_install_and_wheel):
        """The stale-state remediation block must print the exact
        ``pip install --force-reinstall build/*.whl build/*.whl``
        shape.

        Pinning the literal command shape catches a future typo in the
        script (e.g. dropping ``--force-reinstall``, hard-coding a
        version, or losing one of the two wheels) at unit-test time
        rather than at the next bug-bash session.
        """
        install, wheel_dir, wheel = fake_install_and_wheel
        _set_mtime(install, 1_000_000_000.0)
        _set_mtime(wheel, 1_000_000_100.0)

        result = _run_preflight(str(install), str(wheel_dir))

        combined = result.stdout + result.stderr
        assert "pip install --force-reinstall" in combined, (
            "expected 'pip install --force-reinstall' remediation in output; "
            f"got stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        assert "build/snowflake_ml_feature_store_decl-" in combined, (
            "expected the snowflake_ml_feature_store_decl wheel in the "
            f"remediation; got stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        assert "build/snowflake_cli-" in combined, (
            "expected the snowflake_cli wheel in the remediation; got "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )


# ---------------------------------------------------------------------------
# scripts/_extract_plan_error_code.py — pinning the step-6 ERROR-code
# extractor used by verify_bug_bash.sh.
# ---------------------------------------------------------------------------


def _run_extractor(*args: str) -> subprocess.CompletedProcess:
    """Invoke ``scripts/_extract_plan_error_code.py`` as a subprocess.

    Args:
        *args: Positional CLI args (the plan output file path).

    Returns:
        The subprocess result (rc + stdout + stderr).
    """
    return subprocess.run(
        [sys.executable, str(_PLAN_ERROR_EXTRACTOR), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _plan_out_with(
    warnings: list[tuple[str, str]], errors: list[tuple[str, str]]
) -> str:
    """Build a fake ``step6_plan.out`` body with the given warnings/errors.

    Each tuple is ``(code, object_name)``.  The rendered shape mirrors the
    actual ``snow feature plan`` table output that
    ``scripts/verify_bug_bash.sh`` captures into ``$WORK_ROOT/step6_plan.out``.
    """

    def _render(severity: str, code: str, name: str) -> str:
        return (
            f"\"severity='{severity}' code='{code}' "
            f"message='{name}: synthetic test fixture' "
            f"object_name='{name}'\""
        )

    warning_cell = ", ".join(_render("WARNING", c, n) for c, n in warnings)
    error_cell = ", ".join(_render("ERROR", c, n) for c, n in errors)
    return (
        "Status: validation_failed  Operations: 0 (executed: 0)\n"
        f"| warnings | [{warning_cell}] |\n"
        f"| errors   | [{error_cell}] |\n"
    )


class TestExtractPlanErrorCode:
    """Pin ``_extract_plan_error_code.py`` against the BUG_BASH step-6
    extraction contract.

    The legacy ``grep -oE "code='[A-Z_]+'" | head -1`` pattern matched the
    first ``code=`` token regardless of severity — so when WARNING
    entries (``NO_CHANGE``) preceded ERROR entries in the rendered cell,
    the verify script's TODO marker reported the benign warning and the
    actual blocker (e.g. ``BATCH_FV_TILING_TIMESTAMP``) never reached
    ``docs/BUG_BASH.md``.  These tests pin the new extractor against
    the same plan-output shape so a regression never silently masks an
    ERROR again.
    """

    def test_extractor_returns_first_error_code(self, tmp_path):
        """When warnings precede errors, only the ERROR code is returned."""
        plan = tmp_path / "step6_plan.out"
        plan.write_text(
            _plan_out_with(
                warnings=[
                    ("NO_CHANGE", "USER_ID"),
                    ("NO_CHANGE", "CLICKSTREAM_EVENTS"),
                ],
                errors=[
                    ("BATCH_FV_TILING_TIMESTAMP", "MY_BATCH_FV_BATCH_DECL"),
                    ("BATCH_FV_TILING_AGG_METHOD", "MY_BATCH_FV_BATCH_DECL"),
                ],
            )
        )

        result = _run_extractor(str(plan))

        assert result.returncode == 0, (
            f"expected rc=0 when an ERROR code is present; got "
            f"rc={result.returncode!r} stderr={result.stderr!r}"
        )
        assert result.stdout.strip() == "BATCH_FV_TILING_TIMESTAMP", (
            "expected the FIRST ERROR code to be returned (skipping the "
            "NO_CHANGE warnings); got stdout=%r" % result.stdout
        )

    def test_extractor_skips_warning_codes(self, tmp_path):
        """A plan with only WARNING entries must return rc=1 and no code.

        Confirms the ``severity='ERROR'`` anchor — without it, the legacy
        regex would happily return ``NO_CHANGE`` (a WARNING) as the
        ``ERR_SUMMARY``.
        """
        plan = tmp_path / "step6_plan.out"
        plan.write_text(
            _plan_out_with(
                warnings=[
                    ("NO_CHANGE", "USER_ID"),
                    ("NO_CHANGE", "CLICKSTREAM_EVENTS"),
                ],
                errors=[],
            )
        )

        result = _run_extractor(str(plan))

        assert result.returncode == 1, (
            f"expected rc=1 when only WARNING codes are present; got "
            f"rc={result.returncode!r} stdout={result.stdout!r}"
        )
        assert result.stdout.strip() == "", (
            "expected no stdout output when no ERROR codes exist; got "
            f"stdout={result.stdout!r}"
        )

    def test_extractor_handles_escaped_quotes(self, tmp_path):
        """Plan render may pass through a layer that escapes the inner
        quotes (``severity=\\'ERROR\\' code=\\'X\\'``).  The extractor
        must still recover the code.

        Mirrors the defensive ``\\?`` in the legacy bash regex — the
        verify script never knows which rendering layer landed in its
        captured stdout.
        """
        plan = tmp_path / "step6_plan.out"
        plan.write_text(
            "Status: validation_failed\n"
            "| errors | [\"severity=\\'ERROR\\' code=\\'STATE_DRIFT\\' "
            "message=\\'FV1\\' object_name=\\'FV1\\'\"] |\n"
        )

        result = _run_extractor(str(plan))

        assert result.returncode == 0, (
            f"expected rc=0 with escape-quoted shape; got "
            f"rc={result.returncode!r} stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )
        assert result.stdout.strip() == "STATE_DRIFT", (
            "expected STATE_DRIFT to be recovered from the escape-quoted "
            f"plan render; got stdout={result.stdout!r}"
        )

    def test_extractor_returns_rc1_for_missing_file(self, tmp_path):
        """A missing plan output file must exit 1 with empty stdout.

        Mirrors the bash side's defensive ``2>/dev/null || true`` so the
        verify script never crashes on a missing artefact.
        """
        result = _run_extractor(str(tmp_path / "does-not-exist.out"))

        assert result.returncode == 1
        assert result.stdout.strip() == ""
