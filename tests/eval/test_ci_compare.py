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

"""Tests for ci_compare.py — the CI orchestration wrapper.

All tests are offline: compare.main and the gh CLI are mocked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

_EVAL_DIR = Path(__file__).parents[2] / ".github" / "scripts" / "eval"
sys.path.insert(0, str(_EVAL_DIR))

import ci_compare  # noqa: E402

# ---------------------------------------------------------------------------
# trim_to_overall
# ---------------------------------------------------------------------------

_FULL_REPORT = """\
# E2E reviewer eval — A/B comparison

## Regressions

None. 🎉

## Overall

| metric | baseline | candidate | Δ |
|---|---|---|---|
| verdict_accuracy | 100% | 100% | ±0 |

## By expected_verdict

### FAIL

| metric | baseline | candidate | Δ |
|---|---|---|---|

## By difficulty

### easy

| metric | baseline | candidate | Δ |
|---|---|---|---|
"""


def test_trim_removes_slice_sections():
    trimmed = ci_compare.trim_to_overall(_FULL_REPORT)
    assert "## Overall" in trimmed
    assert "## Regressions" in trimmed
    assert "## By expected_verdict" not in trimmed
    assert "## By difficulty" not in trimmed


def test_trim_no_slice_sections_unchanged():
    md = "# Report\n\n## Regressions\n\nNone.\n\n## Overall\n\n| metric |"
    assert ci_compare.trim_to_overall(md) == md


# ---------------------------------------------------------------------------
# build_comment_body
# ---------------------------------------------------------------------------


def test_build_body_regressions_adds_warning(tmp_path):
    md = tmp_path / "comparison.md"
    md.write_text("# Report")
    body = ci_compare.build_comment_body(md, 2, "https://example.com/run")
    assert ci_compare.COMMENT_MARKER in body
    assert "Regressions detected" in body


def test_build_body_missing_md_links_to_logs(tmp_path):
    body = ci_compare.build_comment_body(
        tmp_path / "missing.md", 1, "https://example.com/run"
    )
    assert "could not produce results" in body
    assert "https://example.com/run" in body


# ---------------------------------------------------------------------------
# post_comment — truncation guard
# ---------------------------------------------------------------------------


def test_post_comment_truncates_at_limit():
    long_body = "x" * (ci_compare.MAX_COMMENT_LEN + 500)
    posted = []

    def capture(cmd, **_):
        if "--body" in cmd:
            posted.append(cmd[cmd.index("--body") + 1])

    with mock.patch("subprocess.run", side_effect=capture):
        ci_compare.post_comment("owner/repo", 1, long_body)

    assert len(posted[0]) <= ci_compare.MAX_COMMENT_LEN + 50
    assert "truncated" in posted[0]


# ---------------------------------------------------------------------------
# delete_previous_comment — re-run clean-up
# ---------------------------------------------------------------------------


def test_delete_previous_comment_issues_delete_per_id():
    class FakeResult:
        stdout = "111\n222\n"

    calls = []

    def capture(cmd, **_):
        calls.append(cmd)
        return FakeResult()

    with mock.patch("subprocess.run", side_effect=capture):
        ci_compare.delete_previous_comment("owner/repo", 5)

    joined = [" ".join(c) for c in calls]
    delete_calls = [s for s in joined if "DELETE" in s]
    assert len(delete_calls) == 2
    assert any("111" in s for s in delete_calls)
    assert any("222" in s for s in delete_calls)


# ---------------------------------------------------------------------------
# main() — orchestration
# ---------------------------------------------------------------------------


def _run_main(pr_number=None, compare_exit=0, extra_argv=None):
    """Run ci_compare.main with compare.main and gh CLI fully mocked."""
    argv = ["--connection", "e2ereviewer", "--results-dir", "/tmp/ci_test"]
    if pr_number is not None:
        argv += ["--pr-number", str(pr_number), "--repo", "owner/repo"]
    argv += extra_argv or []

    def fake_compare_main(args):
        rd = Path(args[args.index("--results-dir") + 1])
        rd.mkdir(parents=True, exist_ok=True)
        Path(args[args.index("--markdown-out") + 1]).write_text("# Report")
        Path(args[args.index("--json-out") + 1]).write_text("{}")
        return compare_exit

    gh_calls = []

    def fake_run(cmd, **_):
        gh_calls.append(list(cmd))

        class R:
            stdout = ""

        return R()

    env = {
        "GITHUB_REPOSITORY": "owner/repo",
        "GITHUB_RUN_ID": "1",
        "GITHUB_SERVER_URL": "https://github.com",
    }
    with (
        mock.patch.object(ci_compare.compare, "main", side_effect=fake_compare_main),
        mock.patch("subprocess.run", side_effect=fake_run),
        mock.patch.dict("os.environ", env, clear=False),
    ):
        return ci_compare.main(argv), gh_calls


def test_exit_code_propagated():
    assert _run_main(compare_exit=2)[0] == 2


def test_no_pr_number_skips_gh_calls():
    _, calls = _run_main(pr_number=None)
    assert calls == []


def test_pr_number_posts_comment_and_deletes_old():
    _, calls = _run_main(pr_number=99)
    joined = [" ".join(c) for c in calls]
    assert any("comment" in s for s in joined), "expected a pr comment call"
    assert any(
        "issues" in s and "DELETE" not in s for s in joined
    ), "expected a list-comments call for deletion"


def test_regression_comment_has_warning_banner():
    posted = []

    def fake_compare_main(args):
        rd = Path(args[args.index("--results-dir") + 1])
        rd.mkdir(parents=True, exist_ok=True)
        Path(args[args.index("--markdown-out") + 1]).write_text("# Report")
        Path(args[args.index("--json-out") + 1]).write_text("{}")
        return 2

    def capture_run(cmd, **_):
        if "--body" in cmd:
            posted.append(cmd[cmd.index("--body") + 1])

        class R:
            stdout = ""

        return R()

    env = {
        "GITHUB_REPOSITORY": "owner/repo",
        "GITHUB_RUN_ID": "1",
        "GITHUB_SERVER_URL": "https://github.com",
    }
    with (
        mock.patch.object(ci_compare.compare, "main", side_effect=fake_compare_main),
        mock.patch("subprocess.run", side_effect=capture_run),
        mock.patch.dict("os.environ", env, clear=False),
    ):
        ci_compare.main(
            [
                "--pr-number",
                "1",
                "--repo",
                "owner/repo",
                "--connection",
                "e2ereviewer",
                "--results-dir",
                "/tmp/ci_reg",
            ]
        )

    assert any("Regressions detected" in b for b in posted)
