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

"""Tests that each case's mutation is anchored, effective, and idempotent.

These guard against silent code drift: if an anchor disappears from the current
source, ``mutate`` raises ``AnchorNotFoundError`` and the corresponding test fails,
telling us the case needs updating. Offline — pure text edits on copies of the
real source files; no Snowflake, no cortex.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[2]
_EVAL_DIR = REPO_ROOT / ".github" / "scripts" / "eval"
sys.path.insert(0, str(_EVAL_DIR))

import cases as cases_mod  # noqa: E402

ALL_CASES = cases_mod.load_all_cases()
CASE_IDS = [c.name for c in ALL_CASES]


def _mini_repo(tmp_path: Path, targets: list[str]) -> Path:
    """Copy the real target files into a throwaway repo root."""
    for rel in targets:
        src = REPO_ROOT / rel
        dst = tmp_path / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
    return tmp_path


@pytest.mark.parametrize("case", ALL_CASES, ids=CASE_IDS)
def test_targets_exist_in_repo(case):
    for rel in case.targets:
        assert (REPO_ROOT / rel).exists(), f"{case.name}: target {rel} missing"


@pytest.mark.parametrize("case", ALL_CASES, ids=CASE_IDS)
def test_mutation_changes_files(tmp_path, case):
    """Applying to a fresh copy of current main changes the file(s).

    Also implicitly asserts the anchor still exists (else mutate raises).
    """
    repo = _mini_repo(tmp_path, case.targets)
    before = {t: (repo / t).read_text(encoding="utf-8") for t in case.targets}
    case.mutate(repo)
    after = {t: (repo / t).read_text(encoding="utf-8") for t in case.targets}
    assert any(
        before[t] != after[t] for t in case.targets
    ), f"{case.name}: mutation changed nothing (stale anchor?)"


@pytest.mark.parametrize("case", ALL_CASES, ids=CASE_IDS)
def test_mutation_idempotent(tmp_path, case):
    repo = _mini_repo(tmp_path, case.targets)
    case.mutate(repo)
    once = {t: (repo / t).read_text(encoding="utf-8") for t in case.targets}
    case.mutate(repo)  # second application must be a no-op
    twice = {t: (repo / t).read_text(encoding="utf-8") for t in case.targets}
    assert once == twice, f"{case.name}: mutation is not idempotent"


@pytest.mark.parametrize("case", ALL_CASES, ids=CASE_IDS)
def test_mutation_output_is_valid_python(tmp_path, case):
    """Mutated .py targets must still parse (no syntax errors introduced)."""
    import ast

    repo = _mini_repo(tmp_path, case.targets)
    case.mutate(repo)
    for rel in case.targets:
        if rel.endswith(".py"):
            ast.parse((repo / rel).read_text(encoding="utf-8"))
