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
import json
from io import StringIO
from unittest import mock

from snowflake.cli.api.exceptions import CliError

CLI_CONSOLE_PATH = (
    "snowflake.cli._plugins.dcm.reporters.base.cli_console.styled_message"
)


class FakeCursor:
    """Fake cursor that returns JSON data like a real Snowflake cursor."""

    def __init__(self, data):
        self._data = data
        self._fetched = False

    def fetchone(self):
        if self._fetched:
            return None
        self._fetched = True
        if self._data is None:
            return None
        return (json.dumps(self._data) if isinstance(self._data, dict) else self._data,)


def capture_reporter_output(reporter, cursor, cli_console_path=""):
    """Capture the output from a reporter's process method."""
    output = StringIO()

    def mock_print(text, style=""):
        if hasattr(text, "plain"):
            output.write(text.plain)
        else:
            output.write(str(text))

    error_message = ""
    with mock.patch(
        CLI_CONSOLE_PATH,
        side_effect=mock_print,
    ):
        try:
            reporter.process(cursor)
        except CliError as e:
            error_message = e.message

    result = output.getvalue()
    if error_message:
        result += f"\n{error_message}\n"
    return result
