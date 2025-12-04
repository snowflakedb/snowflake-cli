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
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


class Reporter(ABC):
    def __init__(self) -> None:
        self.command_name = ""

    @abstractmethod
    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract the relevant data from the result JSON."""
        pass

    @abstractmethod
    def generate_renderables(self, data: List[Dict[str, Any]]) -> Iterator[Text]:
        """Generate Rich renderables for the data."""
        pass

    @abstractmethod
    def generate_summary(self) -> Text:
        """Generate a summary Text object."""
        pass

    def process(self, cursor: SnowflakeCursor) -> None:
        row = cursor.fetchone()
        if not row:
            cli_console.safe_print("No data.")
            return

        result_data = row[0]
        result_json = (
            json.loads(result_data) if isinstance(result_data, str) else result_data
        )

        data = self.extract_data(result_json)
        for renderable in self.generate_renderables(data):
            cli_console.safe_print(renderable)

        summary = self.generate_summary()
        cli_console.safe_print(Text("\n") + summary)


class RefreshReporter(Reporter):
    STATUS_WIDTH = 11
    STATS_WIDTH = 7
    _EMPTY_STAT = "No new data"

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
            return []

        refreshed_tables = result_json.get("refreshed_tables", list())

        if not isinstance(refreshed_tables, list):
            log.warning(
                "Unexpected refreshed_tables type: %s, expected list",
                type(refreshed_tables),
            )
            return []

        return refreshed_tables

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

    def _build_row(self, status: str, inserted: str, deleted: str, name: str) -> Text:
        row = Text()
        row.append(status.ljust(self.STATUS_WIDTH) + " ", style=styles.STATUS_STYLE)
        row.append(inserted.rjust(self.STATS_WIDTH) + " ", style=styles.INSERTED_STYLE)
        row.append(deleted.rjust(self.STATS_WIDTH) + " ", style=styles.REMOVED_STYLE)
        row.append(name, style=styles.DOMAIN_STYLE)
        return row

    def _parse_statistics(
        self, statistics: Union[str, Dict[str, Any], None]
    ) -> Optional[Dict[str, Any]]:
        """Parse statistics from various formats into a normalized dict."""
        if statistics is None:
            return None

        if isinstance(statistics, dict):
            return statistics

        if isinstance(statistics, str):
            if statistics == self._EMPTY_STAT:
                return {"insertedRows": 0, "deletedRows": 0}
            if statistics.startswith("{"):
                try:
                    return json.loads(statistics)
                except json.JSONDecodeError:
                    log.debug("Failed to parse statistics JSON: %r", statistics)
                    return None
        return None

    def _extract_stats(
        self, stats_json: Optional[Dict[str, Any]]
    ) -> Tuple[str, int, int]:
        if stats_json is not None:
            inserted = self._safe_int(stats_json.get("insertedRows", 0))
            deleted = self._safe_int(stats_json.get("deletedRows", 0))
            if inserted == 0 and deleted == 0:
                self._summary.up_to_date += 1
                return "UP-TO-DATE", inserted, deleted
            else:
                self._summary.refreshed += 1
                return "REFRESHED", inserted, deleted

        self._summary.unknown += 1
        return "UNKNOWN", 0, 0

    def generate_renderables(self, data: List[Dict[str, Any]]) -> Iterator[Text]:
        if not data:
            return

        for table in data:
            if not isinstance(table, dict):
                log.debug("Unexpected table entry type: %s", type(table))
                self._summary.unknown += 1
                continue

            raw_dt_name = table.get("dt_name", "UNKNOWN")
            dt_name = sanitize_for_terminal(raw_dt_name)
            stats_json = self._parse_statistics(table.get("statistics"))
            status, inserted, deleted = self._extract_stats(stats_json)

            inserted_str, deleted_str = "", ""
            if stats_json is not None:
                inserted_str = self._format_number(inserted)
                if inserted_str != "0":
                    inserted_str = "+" + inserted_str
                deleted_str = self._format_number(deleted)
                if deleted_str != "0":
                    deleted_str = "-" + deleted_str

            yield self._build_row(status, inserted_str, deleted_str, dt_name)

    def generate_summary(self) -> Text:
        total = self._summary.total
        if total == 0:
            return Text("No dynamic tables found in the project.")

        summary = Text()

        parts = []
        if (refreshed := self._summary.refreshed) > 0:
            part = Text(str(refreshed))
            part.append(" refreshed")
            parts.append(part)
        if (up_to_date := self._summary.up_to_date) > 0:
            part = Text(str(up_to_date))
            part.append(" up-to-date")
            parts.append(part)
        if (unknown := self._summary.unknown) > 0:
            part = Text(str(unknown))
            part.append(" unknown")
            parts.append(part)

        for i, part in enumerate(parts):
            if i > 0:
                summary.append(", ")
            summary.append_text(part)
        summary.append(".")

        return summary
