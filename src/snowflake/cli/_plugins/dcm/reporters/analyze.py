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
import logging
from typing import Any, Dict, Iterator, List

from rich.text import Text
from snowflake.cli._plugins.dcm.reporters.base import Reporter, cli_console
from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)


class AnalyzeReporter(Reporter[Dict[str, Any]]):
    _FILES_KEY = "files"

    def __init__(self):
        super().__init__()
        self.command_name = "analyze"
        self._error_count = 0

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        files = result_json.get(self._FILES_KEY, [])
        if not isinstance(files, list):
            log.debug(
                'Unexpected response format. Expected "files" to be a list: %s', files
            )
            raise CliError("Could not process response.")
        return files

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        for file_entry in data:
            self._error_count += len(file_entry.get("errors", []))
            for definition in file_entry.get("definitions", []):
                self._error_count += len(definition.get("errors", []))
            yield file_entry

    def print_renderables(self, data: Iterator[Dict[str, Any]]) -> None:
        for _ in data:
            pass
        if self.result_raw_data is not None:
            cli_console.styled_message(self.result_raw_data)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        if self._error_count == 0:
            return [Text("Analysis completed successfully.")]
        return [Text(f"Analysis found {self._error_count} error(s).")]

    def _is_success(self) -> bool:
        return self._error_count == 0
