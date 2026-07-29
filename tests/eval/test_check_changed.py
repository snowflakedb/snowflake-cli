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

"""Tests for check_changed.py — the CI prompt/model-change detector.

All tests are offline: git show and filesystem reads are mocked.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

_EVAL_DIR = Path(__file__).parents[2] / ".github" / "scripts" / "eval"
sys.path.insert(0, str(_EVAL_DIR))

import check_changed  # noqa: E402
import prompt_spec  # noqa: E402

_WORKFLOW = "          CORTEX_MODEL: claude-opus-4-6\n"
_REVIEWER = """\
import os
LOCAL_DIFF_INSTRUCTIONS = "run git diff origin/main"
AGENT_PROMPT_TEMPLATE = "You are a reviewer. {diff_instructions}"
def main():
    m = os.environ.get("CORTEX_MODEL", "claude-opus-4-6")
"""


def _run(
    argv=None,
    base_src=_REVIEWER,
    cand_src=_REVIEWER,
    base_wf=_WORKFLOW,
    cand_wf=_WORKFLOW,
    github_output=None,
):
    def fake_git_show(ref_path):
        return base_wf if prompt_spec.WORKFLOW_REL_PATH in ref_path else base_src

    with (
        mock.patch.object(check_changed, "_git_show", side_effect=fake_git_show),
        mock.patch.object(
            Path,
            "read_text",
            lambda self: cand_wf
            if prompt_spec.WORKFLOW_REL_PATH in Path(str(self)).as_posix()
            else cand_src,
        ),
        mock.patch.object(Path, "exists", return_value=True),
        mock.patch.dict(
            "os.environ",
            {"GITHUB_OUTPUT": github_output} if github_output else {},
            clear=False,
        ),
    ):
        return check_changed.main(argv or [])


def test_identical_specs_not_changed(capsys):
    assert _run() == 0
    assert "changed: False" in capsys.readouterr().out


def test_different_prompt_is_changed(capsys):
    cand = _REVIEWER.replace("You are a reviewer.", "You are an improved reviewer.")
    assert _run(cand_src=cand) == 0
    assert "changed: True" in capsys.readouterr().out


def test_different_model_is_changed(capsys):
    assert _run(cand_wf="          CORTEX_MODEL: claude-opus-4-8\n") == 0
    assert "changed: True" in capsys.readouterr().out


def test_writes_github_output(tmp_path):
    out_file = tmp_path / "gh_output"
    out_file.write_text("")
    cand = _REVIEWER.replace("You are a reviewer.", "Updated.")
    _run(cand_src=cand, github_output=str(out_file))
    assert "changed=true" in out_file.read_text()


def test_baseline_not_found_returns_1(capsys):
    with mock.patch.object(check_changed, "_git_show", return_value=None):
        assert check_changed.main([]) == 1
    assert "error" in capsys.readouterr().err
