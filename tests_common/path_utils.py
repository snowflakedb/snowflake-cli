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

import unittest.mock as mock
from pathlib import PurePosixPath

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
