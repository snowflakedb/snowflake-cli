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

import contextlib
import os
import tempfile
import unittest.mock as mock
from contextlib import contextmanager
from pathlib import PurePosixPath, Path
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture
def print_paths_as_posix():
    """
    Used to monkey-patch Path instances to always use POSIX-style separators ('/'). This is useful when using
    snapshot-based tests that would otherwise be platform-dependent. Note that using this fixture does introduce
    a small blind spot during testing, so use sparingly.
    """

    with mock.patch("pathlib.WindowsPath.__str__", autospec=True) as mock_str:
        mock_str.side_effect = lambda path: str(PurePosixPath(*path.parts))
        yield mock_str


@contextmanager
def pushd(directory: Path):
    cwd = os.getcwd()
    os.chdir(directory)
    try:
        yield directory
    finally:
        os.chdir(cwd)


@contextmanager
def _named_temporary_file(suffix=None, prefix=None):
    with tempfile.TemporaryDirectory() as tmp_dir:
        suffix = suffix or ""
        prefix = prefix or ""
        f = Path(tmp_dir) / f"{prefix}tmp_file{suffix}"
        f.touch()
        yield f


@pytest.fixture()
def named_temporary_file():
    return _named_temporary_file


class WorkingDirectoryChanger:
    def __init__(self):
        self._initial_working_directory = os.getcwd()

    @staticmethod
    def change_working_directory_to(directory: str | Path):
        os.chdir(directory)

    def restore_initial_working_directory(self):
        self.change_working_directory_to(self._initial_working_directory)


@pytest.fixture
def temporary_working_directory():
    working_directory_changer = WorkingDirectoryChanger()
    with TemporaryDirectory() as tmp_dir:
        working_directory_changer.change_working_directory_to(tmp_dir)
        yield Path(tmp_dir)
        working_directory_changer.restore_initial_working_directory()


# TODO: remove, alias for other fixture
temp_dir = temporary_working_directory


@pytest.fixture
def temporary_working_directory_ctx():
    @contextlib.contextmanager
    def _ctx_manager():
        working_directory_changer = WorkingDirectoryChanger()
        with TemporaryDirectory() as tmp_dir:
            working_directory_changer.change_working_directory_to(tmp_dir)
            yield Path(tmp_dir)
            working_directory_changer.restore_initial_working_directory()

    return _ctx_manager
