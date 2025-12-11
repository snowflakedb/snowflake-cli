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
from typing import Any, Dict, Generic, Iterator, List, Optional, TypeVar, Union

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

    def process(self, cursor: SnowflakeCursor) -> None:
        row = cursor.fetchone()
        if not row:
            cli_console.styled_message("No data.\n")
            return

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
    _EMPTY_STAT = "No new data"
    _DATA_KEY = "refreshed_tables"
    _STATISTICS_KEY = "statistics"
    _DYNAMIC_TABLE_KEY = "dt_name"
    _INSERTED_KEY = "insertedRows"
    _DELETED_KEY = "deletedRows"

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

    def _parse_statistics(
        self, row: Union[Dict[str, Any], Any]
    ) -> Optional[RefreshRow]:
        """Parse statistics from various formats into a normalized dict."""
        if not isinstance(row, dict):
            log.debug("Unexpected table entry type: %s", type(row))
            self._summary.unknown += 1
            return None

        raw_dt_name = row.get(self._DYNAMIC_TABLE_KEY, "UNKNOWN")
        dt_name = sanitize_for_terminal(raw_dt_name)
        new_row = RefreshRow(dt_name=dt_name)
        statistics = row.get(self._STATISTICS_KEY, None)

        if statistics is None:
            self._summary.unknown += 1
            return RefreshRow(dt_name=dt_name)

        if isinstance(statistics, dict):
            new_row.inserted = statistics.get(self._INSERTED_KEY, 0)
            new_row.deleted = statistics.get(self._DELETED_KEY, 0)

        if isinstance(statistics, str):
            if statistics == self._EMPTY_STAT:
                new_row.inserted = 0
                new_row.deleted = 0
            elif statistics.startswith("{"):
                try:
                    data = json.loads(statistics)
                    new_row.inserted = data.get(self._INSERTED_KEY, 0)
                    new_row.deleted = data.get(self._DELETED_KEY, 0)
                except json.JSONDecodeError:
                    log.debug("Failed to parse statistics JSON: %r", statistics)
                    self._summary.unknown += 1
                    return RefreshRow(dt_name=dt_name)
            else:
                log.debug("Unexpected statistics format: %r", statistics)
                self._summary.unknown += 1
                return RefreshRow(dt_name=dt_name)

        if new_row.inserted == 0 and new_row.deleted == 0:
            new_row.status = RefreshStatus.UP_TO_DATE
            self._summary.up_to_date += 1
        else:
            new_row.status = RefreshStatus.REFRESHED
            self._summary.refreshed += 1
        return new_row

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[RefreshRow]:
        for row in data:
            parsed = self._parse_statistics(row)
            if parsed is not None:
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
