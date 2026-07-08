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
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional

import typer
from pydantic import BaseModel, Field, ValidationError
from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.base import Reporter, cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)


class RefreshStatistics(BaseModel):
    inserted_rows: int = 0
    deleted_rows: int = 0


class RefreshTableResult(BaseModel):
    table_name: str = "UNKNOWN"
    statistics: Optional[RefreshStatistics] = None


class DtsRefreshResult(BaseModel):
    refreshed_tables: List[RefreshTableResult] = Field(default_factory=list)


class RefreshResponse(BaseModel):
    dts_refresh_result: Optional[DtsRefreshResult] = None


class RefreshStatus(Enum):
    UNKNOWN = "UNKNOWN"
    UP_TO_DATE = "UP-TO-DATE"
    REFRESHED = "REFRESHED"


@dataclass
class RefreshRow:
    """Represents a single table row in refresh results."""

    table_name: str = "UNKNOWN"
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
            log.info("Could not convert value to int: %r", value)
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
    def from_model(cls, table: RefreshTableResult) -> "RefreshRow":
        row = cls(table_name=sanitize_for_terminal(table.table_name))
        if table.statistics is None:
            return row

        row.inserted = table.statistics.inserted_rows
        row.deleted = table.statistics.deleted_rows

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

    @dataclass
    class Summary:
        up_to_date: int = 0
        refreshed: int = 0
        failed: int = 0

        @property
        def successful(self) -> int:
            """Tables that completed without error (refreshed or already up-to-date)."""
            return self.up_to_date + self.refreshed

        @property
        def total(self):
            return self.successful + self.failed

    def __init__(self, save_output: bool = False):
        super().__init__(save_output=save_output)
        self.command_name = "refresh"
        self._summary = self.Summary()

    def extract_data(self, result_json: Dict[str, Any]) -> List[RefreshTableResult]:
        if not isinstance(result_json, dict):
            log.info("Unexpected response type: %s, expected dict", type(result_json))
            raise CliError("Could not process response.")

        try:
            response = RefreshResponse.model_validate(result_json)
        except ValidationError as e:
            log.info("Failed to validate refresh response: %s", e)
            raise CliError("Could not process response.")

        if response.dts_refresh_result is None:
            return []

        return response.dts_refresh_result.refreshed_tables

    def parse_data(self, data: List[RefreshTableResult]) -> Iterator[RefreshRow]:
        for table in data:
            parsed = RefreshRow.from_model(table)
            if parsed.status == RefreshStatus.UP_TO_DATE:
                self._summary.up_to_date += 1
            elif parsed.status == RefreshStatus.REFRESHED:
                self._summary.refreshed += 1
            else:
                self._summary.failed += 1
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
            cli_console.styled_message(row.table_name, style=styles.OBJECT_NAME_STYLE)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        if self._summary.total == 0:
            return [Text("No dynamic tables found in the project.")]

        renderables: List[Text] = []
        successful = self._summary.successful
        failed = self._summary.failed

        if successful > 0:
            tables_word = "Dynamic Table" if successful == 1 else "Dynamic Tables"
            renderables.append(
                Text(
                    f"{successful} {tables_word} refreshed successfully.",
                    styles.PASS_STYLE,
                )
            )

        if failed > 0:
            refreshes_word = "Refresh" if failed == 1 else "Refreshes"
            if renderables:
                renderables.append(Text("\n"))
            renderables.append(
                Text(f"{failed} {refreshes_word} failed.", styles.FAIL_STYLE)
            )

        return renderables

    def _is_success(self) -> bool:
        # Always treat the run as "success" from the base reporter's perspective
        # so that `print_summary()` is always called — we want the styled
        # green/red summary rendered above the divider on every run (cf.
        # AnalyzeErrorsReporter). Exit code is set separately in
        # ``process_payload``.
        return True

    def process_payload(self, result_json: Dict[str, Any]) -> None:
        super().process_payload(result_json)
        if self._summary.failed > 0:
            # Failures fail the command, but the styled summary is already on
            # screen — exit silently so we don't double-render the message
            # inside an "Error" box.
            raise typer.Exit(code=1)
