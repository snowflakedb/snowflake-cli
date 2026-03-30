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

import os
from unittest.mock import patch

import snowflake.cli.__about__ as about_module
from snowflake.cli.__about__ import get_display_version

ABOUT_DIR = os.path.dirname(about_module.__file__)


class TestGetDisplayVersion:
    @patch("snowflake.cli.__about__.subprocess.check_output", return_value=b"abc1234\n")
    @patch("snowflake.cli.__about__.VERSION", "1.0.0.dev0")
    def test_dev_version_appends_git_sha(self, mock_git):
        assert get_display_version() == "1.0.0.dev0 (abc1234)"
        mock_git.assert_called_once()
        _, kwargs = mock_git.call_args
        assert kwargs["cwd"] == ABOUT_DIR

    @patch(
        "snowflake.cli.__about__.subprocess.check_output",
        side_effect=FileNotFoundError("git not found"),
    )
    @patch("snowflake.cli.__about__.VERSION", "1.0.0.dev0")
    def test_dev_version_falls_back_when_git_unavailable(self, mock_git):
        assert get_display_version() == "1.0.0.dev0"

    @patch("snowflake.cli.__about__.VERSION", "1.0.0")
    def test_release_version_returned_unchanged(self):
        assert get_display_version() == "1.0.0"

    @patch("snowflake.cli.__about__.VERSION", "2.5.3")
    def test_release_version_with_no_dev_suffix(self):
        assert get_display_version() == "2.5.3"

    @patch("snowflake.cli.__about__.subprocess.check_output", return_value=b"abc1234\n")
    @patch("snowflake.cli.__about__.VERSION", "3.0.0.dev5")
    def test_dev_version_with_numeric_suffix(self, mock_git):
        assert get_display_version() == "3.0.0.dev5 (abc1234)"
