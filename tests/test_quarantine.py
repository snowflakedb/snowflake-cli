# Copyright (c) 2026 Snowflake Inc.
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
import importlib.util
from pathlib import Path

import pytest

from tests_common.quarantine import (
    UNSET,
    load_entries,
    lookup_reason,
    quarantine_reason,
)

pytest_plugins = ["pytester"]

NODE_ID = "test_sample.py::test_always_fails"


def test_load_entries_ignores_comments_and_keeps_the_first_line(tmp_path):
    path = tmp_path / "quarantine.txt"
    path.write_text(
        "\n".join(
            [
                "# header",
                "",
                "tests/test_a.py::test_a  # first reason",
                "tests/test_a.py::test_a  # second reason",
                "tests/test_b.py::test_b",
            ]
        ),
        encoding="utf-8",
    )
    entries = load_entries(path)
    assert entries == {
        "tests/test_a.py::test_a": "first reason",
        "tests/test_b.py::test_b": "listed in tests_common/quarantine.txt",
    }


def test_lookup_reason_matches_parameters_only():
    entries = {"tests/test_a.py::test_foo": "reason"}
    assert lookup_reason("tests/test_a.py::test_foo", entries) == "reason"
    assert lookup_reason("tests/test_a.py::test_foo[1]", entries) == "reason"
    assert lookup_reason("tests/test_a.py::test_foo_bar", entries) is None
    assert lookup_reason("tests/test_a.py::test_foo_bar[1]", entries) is None


def test_quarantine_reason_gates_on_event_diff_and_edited_file():
    entries = {NODE_ID: "already red"}
    kwargs = {"entries": entries, "event_name": "pull_request", "changed_raw": ""}
    assert quarantine_reason(NODE_ID, **kwargs) == "already red"
    # A newline is the same empty diff. Windows deletes an empty env value
    # before an xdist worker starts, so subprocess callers pass this instead.
    assert (
        quarantine_reason(
            NODE_ID, entries=entries, event_name="pull_request", changed_raw="\n"
        )
        == "already red"
    )
    assert (
        quarantine_reason(NODE_ID, entries=entries, event_name="push", changed_raw="")
        is None
    )
    assert (
        quarantine_reason(
            NODE_ID, entries=entries, event_name="pull_request", changed_raw=None
        )
        is None
    )
    assert (
        quarantine_reason(
            NODE_ID, entries=entries, event_name="pull_request", changed_raw=UNSET
        )
        is None
    )
    assert (
        quarantine_reason(
            NODE_ID,
            entries=entries,
            event_name="pull_request",
            changed_raw="test_sample.py\ntests_common/quarantine.txt\n",
        )
        is None
    )
    assert (
        quarantine_reason(
            NODE_ID,
            entries=entries,
            event_name="pull_request",
            changed_raw="tests_common/quarantine.txt\n",
        )
        == "already red"
    )


def _enable(monkeypatch, pytester, changed=""):
    listing = pytester.path / "quarantine.txt"
    listing.write_text(f"{NODE_ID}  # already red on main\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("SNOWFLAKE_CLI_CHANGED_FILES", changed)
    monkeypatch.setenv("SNOWFLAKE_CLI_QUARANTINE_FILE", str(listing))
    monkeypatch.delenv("GH_TOKEN", raising=False)


def _failing_test(pytester, counter=None):
    pytester.makeini("[pytest]\naddopts =\n")
    counter_line = ""
    if counter is not None:
        counter_line = (
            "    from pathlib import Path\n"
            f"    p = Path({str(counter)!r})\n"
            "    n = int(p.read_text()) if p.exists() else 0\n"
            "    p.write_text(str(n + 1))\n"
        )
    pytester.makepyfile(
        test_sample="def test_always_fails():\n" + counter_line + "    assert False\n"
    )


def _run(pytester, *extra):
    return pytester.runpytest(
        "--noconftest",
        "-p",
        "tests_common.quarantine",
        "-p",
        "no:cacheprovider",
        "-q",
        *extra,
    )


def test_listed_failure_is_xfailed_on_a_pull_request(pytester, monkeypatch):
    _failing_test(pytester)
    _enable(monkeypatch, pytester)
    result = _run(pytester)
    result.assert_outcomes(xfailed=1)
    assert result.ret == 0


def test_unlisted_failure_still_fails(pytester, monkeypatch):
    _failing_test(pytester)
    _enable(monkeypatch, pytester)
    listing = pytester.path / "quarantine.txt"
    listing.write_text("# empty\n", encoding="utf-8")
    result = _run(pytester)
    result.assert_outcomes(failed=1)
    assert result.ret == 1


def test_edited_test_file_is_not_forgiven(pytester, monkeypatch):
    _failing_test(pytester)
    _enable(monkeypatch, pytester, changed="test_sample.py\n")
    result = _run(pytester)
    result.assert_outcomes(failed=1)
    assert result.ret == 1


def test_unset_changed_files_stays_strict(pytester, monkeypatch):
    _failing_test(pytester)
    _enable(monkeypatch, pytester, changed=UNSET)
    result = _run(pytester)
    result.assert_outcomes(failed=1)


def test_push_event_stays_strict(pytester, monkeypatch):
    _failing_test(pytester)
    _enable(monkeypatch, pytester)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "push")
    result = _run(pytester)
    result.assert_outcomes(failed=1)


def test_passing_listed_test_reports_xpass(pytester, monkeypatch, tmp_path):
    pytester.makeini("[pytest]\naddopts =\n")
    pytester.makepyfile(test_sample="def test_always_fails():\n    assert True\n")
    _enable(monkeypatch, pytester)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    result = _run(pytester)
    result.assert_outcomes(passed=1)
    assert result.ret == 0
    output = "\n".join(line for line in result.outlines)
    assert "XPASS" in output
    assert NODE_ID in summary.read_text(encoding="utf-8")


def test_quarantine_runs_after_deflake_retry(pytester, monkeypatch, tmp_path):
    counter = tmp_path / "runs"
    _failing_test(pytester, counter)
    _enable(monkeypatch, pytester)
    result = pytester.runpytest(
        "--noconftest",
        "-p",
        "tests_common.deflake",
        "-p",
        "tests_common.quarantine",
        "-p",
        "no:cacheprovider",
        "-q",
    )
    assert counter.read_text(encoding="utf-8") == "2"
    result.assert_outcomes(xfailed=1)
    assert result.ret == 0


@pytest.mark.skipif(
    importlib.util.find_spec("xdist") is None, reason="pytest-xdist is not installed"
)
def test_xdist_controller_reports_the_quarantine(pytester, monkeypatch):
    _failing_test(pytester)
    # "" is deleted by Windows putenv, so the worker sees the variable as unset.
    _enable(monkeypatch, pytester, changed="\n")
    # xdist freezes sys.path when its plugin is imported, before pytest adds
    # the repo root. Nested workers then cannot import tests_common.
    import xdist.plugin

    repo_root = str(Path(__file__).resolve().parents[1])
    frozen_path = xdist.plugin._sys_path  # noqa: SLF001
    if repo_root not in frozen_path:
        frozen_path.insert(0, repo_root)
    result = _run(pytester, "-n2", "-p", "xdist")
    result.assert_outcomes(xfailed=1)
    assert result.ret == 0
    assert "snowflake-cli quarantine" in "\n".join(result.outlines)
