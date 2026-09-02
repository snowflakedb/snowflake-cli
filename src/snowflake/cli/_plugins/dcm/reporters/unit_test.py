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
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.base import Reporter, cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)


class UnitTestScriptStatus(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


@dataclass
class UnitTestScriptRow:
    script_name: str = "UNKNOWN"
    status: UnitTestScriptStatus = UnitTestScriptStatus.UNKNOWN
    error_message: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["UnitTestScriptRow"]:
        if not isinstance(data, dict):
            log.info("Unexpected script entry type: %s", type(data))
            return None

        script_name = data.get("script_name", "UNKNOWN")
        passed = data.get("passed")
        if passed is True:
            status = UnitTestScriptStatus.PASS
        elif passed is False:
            status = UnitTestScriptStatus.FAIL
        else:
            log.warning(
                "Unexpected 'passed' value for script %r: %r; treating as UNKNOWN.",
                script_name,
                passed,
            )
            status = UnitTestScriptStatus.UNKNOWN

        error_message = data.get("error_message")
        return cls(
            script_name=sanitize_for_terminal(str(script_name)),
            status=status,
            error_message=(
                sanitize_for_terminal(str(error_message)) if error_message else None
            ),
        )


class UnitTestReporter(Reporter[UnitTestScriptRow]):
    STATUS_WIDTH = 11
    _DATA_KEY = "scripts"

    @dataclass
    class Summary:
        passed: int = 0
        failed: int = 0

        @property
        def total(self):
            return self.passed + self.failed

    def __init__(self, save_output: bool = False):
        super().__init__(save_output=save_output)
        self.command_name = "unit_test"
        self._summary = self.Summary()

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(result_json, dict):
            log.info("Unexpected response type: %s, expected dict", type(result_json))
            raise CliError("Could not process response.")

        scripts = result_json.get(self._DATA_KEY, list())

        if not isinstance(scripts, list):
            log.warning(
                "Unexpected scripts type: %s, expected list",
                type(scripts),
            )
            raise CliError("Could not process response.")

        return scripts

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[UnitTestScriptRow]:
        for row in data:
            parsed = UnitTestScriptRow.from_dict(row)
            if parsed is not None:
                if parsed.status == UnitTestScriptStatus.PASS:
                    self._summary.passed += 1
                else:
                    self._summary.failed += 1
                yield parsed

    def print_renderables(self, data: Iterator[UnitTestScriptRow]) -> None:
        for row in data:
            if row.status == UnitTestScriptStatus.PASS:
                status_text = "✓ PASS"
                style = styles.PASS_STYLE
            else:
                status_text = "✗ FAIL"
                style = styles.FAIL_STYLE

            cli_console.styled_message(
                status_text.ljust(self.STATUS_WIDTH) + " ",
                style=style,
            )
            cli_console.styled_message(row.script_name, style=styles.DOMAIN_STYLE)
            cli_console.styled_message("\n")

            if row.status == UnitTestScriptStatus.FAIL and row.error_message:
                cli_console.styled_message(f"  └─ {row.error_message}\n")

    def _generate_summary_renderables(self) -> List[Text]:
        total = self._summary.total
        if total == 0:
            return [Text("No test scripts were run.")]

        return [
            Text(f"{self._summary.passed} passed", styles.PASS_STYLE),
            Text(", "),
            Text(f"{self._summary.failed} failed", styles.FAIL_STYLE),
            Text(" out of "),
            Text(f"{total}", styles.BOLD_STYLE),
            Text(" total."),
        ]

    def _is_success(self) -> bool:
        return self._summary.failed == 0
