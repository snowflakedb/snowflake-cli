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

"""Tests for the eval case loader (cases.py). Offline — no Snowflake, no cortex."""

from __future__ import annotations

import sys
from pathlib import Path

_EVAL_DIR = Path(__file__).parents[2] / ".github" / "scripts" / "eval"
sys.path.insert(0, str(_EVAL_DIR))

import cases as cases_mod  # noqa: E402

EXPECTED_IDS = {
    "sql-rename-query-flag",
    "object-list-noop-comment",
    "sql-add-query-alias",
    "connection-list-json-key",
    "like-default-match-all",
    "git-split-path-refactor",
    "object-list-reorder-options",
    "object-rename-internal-helper",
}

ALL_CASES = cases_mod.load_all_cases()


def test_all_expected_cases_present():
    ids = {c.name for c in ALL_CASES}
    assert ids == EXPECTED_IDS


def test_case_id_matches_directory_name():
    for c in ALL_CASES:
        assert c.name == c.path.name, f"metadata.name {c.name!r} != dir {c.path.name!r}"


def test_verdicts_and_difficulty_valid():
    for c in ALL_CASES:
        assert c.expected_verdict in {"PASS", "FAIL", "SKIP"}
        assert c.difficulty in {"easy", "hard"}


def test_ground_truth_fields_typed():
    for c in ALL_CASES:
        assert isinstance(c.expected_commands, list)
        assert all(isinstance(x, str) for x in c.expected_commands)
        assert isinstance(c.expected_breaking_changes, bool)
        assert isinstance(c.needs_live_snowflake, bool)
        assert isinstance(c.rubric, str)
        assert c.targets, f"{c.name}: mutate.py must declare TARGETS"
        assert callable(c.mutate)


def test_breaking_change_flag_consistent_with_verdict():
    # Only FAIL cases may claim a breaking change.
    for c in ALL_CASES:
        if c.expected_breaking_changes:
            assert (
                c.expected_verdict == "FAIL"
            ), f"{c.name}: breaking change but not FAIL"


def test_skip_case_has_no_expected_commands():
    # A non-behavioral change needs no investigative commands.
    noop = next(c for c in ALL_CASES if c.name == "object-list-noop-comment")
    assert noop.expected_verdict == "SKIP"
    assert noop.expected_commands == []


def test_specific_case_fields():
    by_id = {c.name: c for c in ALL_CASES}

    rename = by_id["sql-rename-query-flag"]
    assert rename.expected_verdict == "FAIL"
    assert rename.expected_breaking_changes is True

    json_key = by_id["connection-list-json-key"]
    assert json_key.expected_verdict == "FAIL"
    assert "snow connection list --format json" in json_key.expected_commands
    assert json_key.needs_live_snowflake is False

    like = by_id["like-default-match-all"]
    assert like.expected_verdict == "PASS"
    assert like.expected_breaking_changes is False
