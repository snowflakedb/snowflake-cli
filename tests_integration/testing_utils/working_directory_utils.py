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

import contextlib
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest


class WorkingDirectoryChanger:
    def __init__(self):
        self._initial_working_directory = os.getcwd()

    @staticmethod
    def change_working_directory_to(directory: str):
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
