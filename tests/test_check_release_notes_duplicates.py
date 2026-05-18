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

from __future__ import annotations

import importlib.util
import textwrap
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "check_release_notes_duplicates.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location(
        "check_release_notes_duplicates", _SCRIPT_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check = _load_script_module()


@pytest.fixture
def notes_file(tmp_path: Path):
    def _write(body: str) -> Path:
        path = tmp_path / "RELEASE-NOTES.md"
        path.write_text(textwrap.dedent(body).lstrip("\n"), encoding="utf-8")
        return path

    return _write


def test_clean_notes_have_no_duplicates(notes_file):
    path = notes_file(
        """
        # Unreleased version
        ## Fixes and improvements
        * Fixed something new.

        # v3.17.0
        ## Fixes and improvements
        * Fixed something else.
        """
    )
    assert check.find_duplicates(path) == []


def test_bullet_duplicated_across_sections_is_flagged(notes_file):
    path = notes_file(
        """
        # Unreleased version
        ## Fixes and improvements
        * Fixed the leaky connection pool.

        # v3.17.0
        ## Fixes and improvements
        * Fixed the leaky connection pool.
        """
    )
    errors = check.find_duplicates(path)
    assert len(errors) == 1
    assert "Unreleased version" in errors[0]
    assert "v3.17.0" in errors[0]
    assert "leaky connection pool" in errors[0]


def test_bullet_duplicated_inside_unreleased_is_flagged(notes_file):
    path = notes_file(
        """
        # Unreleased version
        ## Fixes and improvements
        * Fixed broken thing.
        * Fixed broken thing.
        """
    )
    errors = check.find_duplicates(path)
    assert len(errors) == 1
    assert "multiple times" in errors[0]
    assert "Unreleased version" in errors[0]


def test_near_duplicates_are_not_flagged(notes_file):
    path = notes_file(
        """
        # Unreleased version
        ## Fixes and improvements
        * Fixed the issue with connections.
        * Fixed the issue with authentication.
        """
    )
    assert check.find_duplicates(path) == []


def test_whitespace_differences_are_treated_as_duplicates(notes_file):
    path = notes_file(
        """
        # Unreleased version
        ## Fixes and improvements
        *   Fixed   something    weird.

        # v3.17.0
        ## Fixes and improvements
        * Fixed something weird.
        """
    )
    errors = check.find_duplicates(path)
    assert len(errors) == 1
    assert "Unreleased version" in errors[0]


def test_main_returns_zero_on_clean_file(notes_file):
    path = notes_file(
        """
        # Unreleased version
        ## Fixes and improvements
        * One bullet.
        """
    )
    assert check.main([str(path)]) == 0


def test_main_returns_one_on_dirty_file(notes_file, capsys):
    path = notes_file(
        """
        # Unreleased version
        ## Fixes and improvements
        * Duplicate bullet.

        # v3.17.0
        ## Fixes and improvements
        * Duplicate bullet.
        """
    )
    rc = check.main([str(path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "Duplicate bullet" in err
