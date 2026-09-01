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
from snowflake.cli._plugins.dcm.utils import RAW_ANALYZE_COMMAND_NAME
from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)


class AnalyzeReporter(Reporter[Dict[str, Any]]):
    _FILES_KEY = "files"

    def __init__(self, save_output: bool = False):
        super().__init__(save_output=save_output)
        self.command_name = RAW_ANALYZE_COMMAND_NAME
        self._issue_count = 0

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        files = result_json.get(self._FILES_KEY, [])
        if not isinstance(files, list):
            log.info(
                'Unexpected response format. Expected "files" to be a list: %s', files
            )
            raise CliError("Could not process response.")
        self._issue_count = self._count_issues(result_json, files)
        return files

    @staticmethod
    def _count_issues(result_json: Dict[str, Any], files: List[Dict[str, Any]]) -> int:
        count = len(result_json.get("issues", []))
        for file_entry in files:
            count += len(file_entry.get("issues", []))
            for definition in file_entry.get("definitions", []):
                count += len(definition.get("issues", []))
        return count

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        return iter(data)

    def print_renderables(self, data: Iterator[Dict[str, Any]]) -> None:
        if self.result_raw_data is not None:
            cli_console.styled_message(self.result_raw_data)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        if self._issue_count == 0:
            return [Text("Analysis completed successfully.")]
        return [Text(f"Analysis found {self._issue_count} error(s).")]

    def _is_success(self) -> bool:
        return self._issue_count == 0
