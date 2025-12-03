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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterator

from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.connector.cursor import SnowflakeCursor


class Reporter(ABC):
    def __init__(self) -> None:
        self.command_name = ""

    @abstractmethod
    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        """Extract the relevant data from the result JSON."""
        pass

    @abstractmethod
    def generate_renderables(self, data: Any) -> Iterator[Text]:
        """Generate Rich renderables for the data."""
        pass

    @abstractmethod
    def generate_summary(self, data: Any) -> Text:
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

        summary = self.generate_summary(data)
        cli_console.safe_print(Text("\n") + summary)


class RefreshReporter(Reporter):
    STATUS_WIDTH = 11
    STATS_WIDTH = 7
    _EMPTY_RESULTSET = "No new data"

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

    def extract_data(self, result_json: Dict[str, Any]) -> Any:
        return result_json.get("refreshed_tables", [])

    @staticmethod
    def _format_number(num: int) -> str:
        abs_num = abs(num)
        if abs_num >= 1_000_000_000_000_000_000:  # Quintillions (10^18)
            formatted = f"{abs_num / 1_000_000_000_000_000_000:.1f}E"
        elif abs_num >= 1_000_000_000_000_000:  # Quadrillions (10^15)
            formatted = f"{abs_num / 1_000_000_000_000_000:.1f}P"
        elif abs_num >= 1_000_000_000_000:  # Trillions
            formatted = f"{abs_num / 1_000_000_000_000:.1f}T"
        elif abs_num >= 1_000_000_000:  # Billions
            formatted = f"{abs_num / 1_000_000_000:.1f}B"
        elif abs_num >= 1_000_000:  # Millions
            formatted = f"{abs_num / 1_000_000:.1f}M"
        elif abs_num >= 1_000:  # Thousands
            formatted = f"{abs_num / 1_000:.1f}k"
        else:
            return str(num)

        formatted = formatted.replace(".0", "")
        return formatted

    def _build_row(self, status: str, inserted: str, deleted: str, name: str) -> Text:
        row = Text()
        row.append(status.ljust(self.STATUS_WIDTH) + " ", style=styles.STATUS_STYLE)
        row.append(inserted.rjust(self.STATS_WIDTH) + " ", style=styles.INSERTED_STYLE)
        row.append(deleted.rjust(self.STATS_WIDTH) + " ", style=styles.REMOVED_STYLE)
        row.append(name, style=styles.DOMAIN_STYLE)
        return row

    def generate_renderables(self, data: Any) -> Iterator[Text]:
        for table in data:
            dt_name = sanitize_for_terminal(table.get("dt_name", "UNKNOWN"))
            statistics = table.get("statistics", "")

            inserted_str, deleted_str = "", ""
            stats_json = None
            if isinstance(statistics, str) and statistics.startswith("{"):
                try:
                    stats_json = json.loads(statistics)
                except json.JSONDecodeError:
                    pass
            elif isinstance(statistics, dict):
                stats_json = statistics

            if (
                statistics == self._EMPTY_RESULTSET
                or stats_json is not None
                and (
                    not stats_json.get("insertedRows")
                    and not stats_json.get("deletedRows")
                )
            ):
                status = "UP-TO-DATE"
                self._summary.up_to_date += 1
            elif stats_json is not None and (
                stats_json.get("insertedRows") or stats_json.get("deletedRows")
            ):
                status = "REFRESHED"
                self._summary.refreshed += 1
            else:
                status = "UNKNOWN"
                self._summary.unknown += 1

            if stats_json is not None:
                inserted = stats_json.get("insertedRows", 0)
                deleted = stats_json.get("deletedRows", 0)

                inserted_str = self._format_number(inserted)
                if inserted_str != "0":
                    inserted_str = "+" + inserted_str
                deleted_str = self._format_number(deleted)
                if deleted_str != "0":
                    deleted_str = "-" + deleted_str

            yield self._build_row(status, inserted_str, deleted_str, dt_name)

    def generate_summary(self, *args, **kwargs) -> Text:
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
