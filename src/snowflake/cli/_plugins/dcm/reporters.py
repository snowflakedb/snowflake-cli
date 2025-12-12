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
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Generic, Iterator, List, Optional, TypeVar

from snowflake.cli._plugins.dcm import styles
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)

T = TypeVar("T")


class Reporter(ABC, Generic[T]):
    def __init__(self) -> None:
        self.command_name = ""

    @abstractmethod
    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract the relevant data from the result JSON."""
        ...

    @abstractmethod
    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[T]:
        """Parse raw data into domain objects."""
        ...

    @abstractmethod
    def print_renderables(self, data: Iterator[T]) -> None:
        """Print Rich renderables for the parsed data."""
        ...

    @abstractmethod
    def print_summary(self) -> None:
        """Print a summary."""
        ...

    def process(self, cursor: SnowflakeCursor) -> bool:
        """Process cursor data and print results.

        Returns:
            True if there were errors (e.g., failed tests), False otherwise.
        """
        row = cursor.fetchone()
        if not row:
            cli_console.styled_message("No data.\n")
            return False

        try:
            result_data = row[0]
            result_json = (
                json.loads(result_data) if isinstance(result_data, str) else result_data
            )
        except IndexError:
            log.debug("Unexpected response format: %s", row)
            raise CliError("Could not process response.")
        except json.JSONDecodeError as e:
            log.debug("Could not decode response: %s", e)
            raise CliError("Could not process response.")

        raw_data = self.extract_data(result_json)
        parsed_data: Iterator[T] = self.parse_data(raw_data)
        self.print_renderables(parsed_data)
        self.print_summary()
        return self.check_for_errors()

    def check_for_errors(self) -> bool:
        return False


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

    def print_summary(self) -> None:
        cli_console.styled_message("\n")
        total = self._summary.total
        if total == 0:
            cli_console.styled_message("No expectations found in the project.\n")
            return

        cli_console.styled_message(f"{self._summary.passed} passed", styles.PASS_STYLE)
        cli_console.styled_message(", ")
        cli_console.styled_message(f"{self._summary.failed} failed", styles.FAIL_STYLE)
        if self._summary.unknown > 0:
            cli_console.styled_message(", ")
            cli_console.styled_message(
                f"{self._summary.unknown} unknown", styles.FAIL_STYLE
            )
        cli_console.styled_message(" out of ")
        cli_console.styled_message(f"{total}", styles.BOLD_STYLE)
        cli_console.styled_message(" total.\n")

    def check_for_errors(self) -> bool:
        return self._summary.failed + self._summary.unknown > 0


class RefreshStatus(Enum):
    UNKNOWN = "UNKNOWN"
    UP_TO_DATE = "UP-TO-DATE"
    REFRESHED = "REFRESHED"


@dataclass
class RefreshRow:
    dt_name: str = "UNKNOWN"
    status: RefreshStatus = RefreshStatus.UNKNOWN
    _inserted: int = field(default=0, repr=False)
    _deleted: int = field(default=0, repr=False)

    _EMPTY_STAT = "No new data"
    _STATISTICS_KEY = "statistics"
    _DYNAMIC_TABLE_KEY = "dt_name"
    _INSERTED_KEY = "insertedRows"
    _DELETED_KEY = "deletedRows"

    @staticmethod
    def _safe_int(value: Any) -> int:
        if value is None:
            return 0
        try:
            return int(value)
        except (ValueError, TypeError):
            log.debug("Could not convert value to int: %r", value)
            return 0

    @staticmethod
    def _format_number(num: int) -> str:
        abs_num = abs(num)

        units = [
            (1_000_000_000_000_000_000, "E"),  # Quintillions (10^18)
            (1_000_000_000_000_000, "P"),  # Quadrillions (10^15)
            (1_000_000_000_000, "T"),  # Trillions
            (1_000_000_000, "B"),  # Billions
            (1_000_000, "M"),  # Millions
            (1_000, "k"),  # Thousands
        ]

        for threshold, suffix in units:
            if abs_num >= threshold:
                value = abs_num / threshold
                if round(value, 1) >= 1000:
                    formatted = f"{int(value)}{suffix}"
                else:
                    formatted = f"{value:.1f}{suffix}".replace(".0", "")
                return formatted

        return str(num)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["RefreshRow"]:
        if not isinstance(data, dict):
            log.debug("Unexpected table entry type: %s", type(data))
            return None

        raw_dt_name = data.get(cls._DYNAMIC_TABLE_KEY, "UNKNOWN")
        dt_name = sanitize_for_terminal(str(raw_dt_name))
        row = cls(dt_name=dt_name)

        statistics = data.get(cls._STATISTICS_KEY)
        if statistics is None:
            return row

        if isinstance(statistics, dict):
            row.inserted = statistics.get(cls._INSERTED_KEY, 0)
            row.deleted = statistics.get(cls._DELETED_KEY, 0)
        elif isinstance(statistics, str):
            if statistics == cls._EMPTY_STAT:
                row.inserted = 0
                row.deleted = 0
            elif statistics.startswith("{"):
                try:
                    stats_data = json.loads(statistics)
                    row.inserted = stats_data.get(cls._INSERTED_KEY, 0)
                    row.deleted = stats_data.get(cls._DELETED_KEY, 0)
                except json.JSONDecodeError:
                    log.debug("Failed to parse statistics JSON: %r", statistics)
                    return row
            else:
                log.debug("Unexpected statistics format: %r", statistics)
                return row

        if row.inserted == 0 and row.deleted == 0:
            row.status = RefreshStatus.UP_TO_DATE
        else:
            row.status = RefreshStatus.REFRESHED

        return row

    @property
    def inserted(self) -> int:
        return self._inserted

    @inserted.setter
    def inserted(self, value: Any) -> None:
        self._inserted = self._safe_int(value)

    @property
    def deleted(self) -> int:
        return self._deleted

    @deleted.setter
    def deleted(self, value: Any) -> None:
        self._deleted = self._safe_int(value)

    @property
    def formatted_inserted(self) -> str:
        if self.status == RefreshStatus.UNKNOWN:
            return ""
        formatted = self._format_number(self._inserted)
        if formatted != "0":
            return "+" + formatted
        return formatted

    @property
    def formatted_deleted(self) -> str:
        if self.status == RefreshStatus.UNKNOWN:
            return ""
        formatted = self._format_number(self._deleted)
        if formatted != "0":
            return "-" + formatted
        return formatted


class RefreshReporter(Reporter[RefreshRow]):
    STATUS_WIDTH = 11
    STATS_WIDTH = 7
    _DATA_KEY = "refreshed_tables"

    @dataclass
    class Summary:
        up_to_date: int = 0
        refreshed: int = 0
        unknown: int = 0

        @property
        def total(self):
            return self.up_to_date + self.refreshed + self.unknown

    def __init__(self):
        super().__init__()
        self.command_name = "refresh"
        self._summary = self.Summary()

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(result_json, dict):
            log.debug("Unexpected response type: %s, expected dict", type(result_json))
            raise CliError("Could not process response.")

        refreshed_tables = result_json.get(self._DATA_KEY, list())

        if not isinstance(refreshed_tables, list):
            log.warning(
                "Unexpected refreshed_tables type: %s, expected list",
                type(refreshed_tables),
            )
            raise CliError("Could not process response.")

        return refreshed_tables

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[RefreshRow]:
        for row in data:
            parsed = RefreshRow.from_dict(row)
            if parsed is None:
                self._summary.unknown += 1
                continue

            if parsed.status == RefreshStatus.UP_TO_DATE:
                self._summary.up_to_date += 1
            elif parsed.status == RefreshStatus.REFRESHED:
                self._summary.refreshed += 1
            else:
                self._summary.unknown += 1
            yield parsed

    def print_renderables(self, data: Iterator[RefreshRow]) -> None:
        for row in data:
            cli_console.styled_message(
                row.status.value.ljust(self.STATUS_WIDTH) + " ",
                style=styles.STATUS_STYLE,
            )
            cli_console.styled_message(
                row.formatted_inserted.rjust(self.STATS_WIDTH) + " ",
                style=styles.INSERTED_STYLE,
            )
            cli_console.styled_message(
                row.formatted_deleted.rjust(self.STATS_WIDTH) + " ",
                style=styles.REMOVED_STYLE,
            )
            cli_console.styled_message(row.dt_name, style=styles.DOMAIN_STYLE)
            cli_console.styled_message("\n")

    def print_summary(self) -> None:
        cli_console.styled_message("\n")
        total = self._summary.total
        if total == 0:
            return cli_console.styled_message(
                "No dynamic tables found in the project.\n"
            )

        parts = []
        if (refreshed := self._summary.refreshed) > 0:
            parts.append(f"{refreshed} refreshed")
        if (up_to_date := self._summary.up_to_date) > 0:
            parts.append(f"{up_to_date} up-to-date")
        if (unknown := self._summary.unknown) > 0:
            parts.append(f"{unknown} unknown")

        summary = ""
        for i, part in enumerate(parts):
            if i > 0:
                summary += ", "
            summary += part
        summary += ".\n"

        cli_console.styled_message(summary)
