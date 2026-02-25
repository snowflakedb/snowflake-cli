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


class TestStatus(Enum):
    __test__ = False  # Prevent pytest collection

    UNKNOWN = "UNKNOWN"
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass
class TestRow:
    __test__ = False  # Prevent pytest collection

    table_name: str = "UNKNOWN"
    expectation_name: str = "UNKNOWN"
    status: TestStatus = TestStatus.UNKNOWN
    expectation_expression: str = ""
    metric_name: str = ""
    actual_value: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["TestRow"]:
        def _get(key):
            return sanitize_for_terminal(str(data.get(key, "UNKNOWN")))

        if not isinstance(data, dict):
            log.debug("Unexpected test entry type: %s", type(data))
            return None

        row = cls(
            table_name=_get("table_name"),
            expectation_name=_get("expectation_name"),
            expectation_expression=_get("expectation_expression"),
            metric_name=_get("metric_name"),
            actual_value=_get("value"),
        )

        expectation_violated = data.get("expectation_violated")
        if expectation_violated is True:
            row.status = TestStatus.FAIL
        elif expectation_violated is False:
            row.status = TestStatus.PASS
        else:
            row.status = TestStatus.UNKNOWN
        return row


class TestReporter(Reporter[TestRow]):
    __test__ = False  # Prevent pytest collection

    STATUS_WIDTH = 11
    _DATA_KEY = "expectations"

    @dataclass
    class Summary:
        passed: int = 0
        failed: int = 0
        unknown: int = 0

        @property
        def total(self):
            return self.passed + self.failed + self.unknown

    def __init__(self):
        super().__init__()
        self.command_name = "test"
        self._summary = self.Summary()

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(result_json, dict):
            log.debug("Unexpected response type: %s, expected dict", type(result_json))
            raise CliError("Could not process response.")

        expectations = result_json.get(self._DATA_KEY, list())

        if not isinstance(expectations, list):
            log.warning(
                "Unexpected expectations type: %s, expected list",
                type(expectations),
            )
            raise CliError("Could not process response.")

        return expectations

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[TestRow]:
        for row in data:
            parsed = TestRow.from_dict(row)
            if parsed is not None:
                if parsed.status == TestStatus.PASS:
                    self._summary.passed += 1
                elif parsed.status == TestStatus.FAIL:
                    self._summary.failed += 1
                else:
                    self._summary.unknown += 1
                yield parsed

    def print_renderables(self, data: Iterator[TestRow]) -> None:
        for row in data:
            if row.status == TestStatus.PASS:
                status_text = "✓ PASS"
                style = styles.PASS_STYLE
            elif row.status == TestStatus.FAIL:
                status_text = "✗ FAIL"
                style = styles.FAIL_STYLE
            else:
                status_text = "? UNKNOWN"
                style = styles.STATUS_STYLE

            cli_console.styled_message(
                status_text.ljust(self.STATUS_WIDTH) + " ",
                style=style,
            )
            cli_console.styled_message(row.table_name, style=styles.DOMAIN_STYLE)
            cli_console.styled_message(f" ({row.expectation_name})")
            cli_console.styled_message("\n")

            if row.status == TestStatus.FAIL:
                cli_console.styled_message(
                    f"  └─ Expected: {row.expectation_expression}, "
                    f"Got: {row.actual_value} (Metric: {row.metric_name})\n"
                )

    def _generate_summary_renderables(self) -> List[Text]:
        total = self._summary.total
        if total == 0:
            return [Text("No expectations found in the project.")]

        result = [
            (Text(f"{self._summary.passed} passed", styles.PASS_STYLE)),
            (Text(", ")),
            (Text(f"{self._summary.failed} failed", styles.FAIL_STYLE)),
        ]
        if self._summary.unknown > 0:
            result.append(Text(", "))
            result.append(Text(f"{self._summary.unknown} unknown", styles.FAIL_STYLE))
        result.append(Text(" out of "))
        result.append(Text(f"{total}", styles.BOLD_STYLE))
        result.append(Text(" total."))
        return result

    def _is_success(self) -> bool:
        return self._summary.failed + self._summary.unknown == 0
