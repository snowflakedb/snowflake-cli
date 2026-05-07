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

from unittest import mock

from snowflake.cli.api.utils.tty import is_tty_interactive


def test_is_tty_interactive_returns_true_when_tty():
    with mock.patch("sys.stdin") as mock_stdin, mock.patch("sys.stdout") as mock_stdout:
        mock_stdin.isatty.return_value = True
        mock_stdout.isatty.return_value = True
        assert is_tty_interactive() is True


def test_is_tty_interactive_returns_false_when_not_tty():
    with mock.patch("sys.stdin") as mock_stdin, mock.patch("sys.stdout") as mock_stdout:
        mock_stdin.isatty.return_value = False
        mock_stdout.isatty.return_value = True
        assert is_tty_interactive() is False

    with mock.patch("sys.stdin") as mock_stdin, mock.patch("sys.stdout") as mock_stdout:
        mock_stdin.isatty.return_value = True
        mock_stdout.isatty.return_value = False
        assert is_tty_interactive() is False


def test_is_tty_interactive_returns_false_on_exception():
    with mock.patch("sys.stdin") as mock_stdin:
        mock_stdin.isatty.side_effect = Exception("No TTY available")
        assert is_tty_interactive() is False
