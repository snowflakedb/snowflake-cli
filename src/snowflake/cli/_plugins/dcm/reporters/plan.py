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
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from pydantic import BaseModel, Field, ValidationError
from rich.style import Style
from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.base import Reporter, cli_console
from snowflake.cli.api.exceptions import CliError, FQNNameError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)

_OPERATION_WIDTH = 8
_DOMAIN_WIDTH = 20


class PlanObjectId(BaseModel):
    """Object identifier within a changeset entry."""

    domain: str
    fqn: str


class PlanEntityChange(BaseModel):
    """Top-level entity change in the changeset."""

    type_: str = Field(None, alias="type")
    object_id: PlanObjectId


class PlanResponse(BaseModel):
    """Top-level version 2 plan response."""

    version: int
    changeset: List[PlanEntityChange] = Field(default_factory=list)


_OPERATION_ORDER = {"CREATE": 0, "ALTER": 1, "DROP": 2}


@dataclass
class PlanRow:
    """Parsed entry ready for display"""

    operation: str
    domain: str
    fqn: Optional[FQN] = None

    @property
    def sort_key(self) -> tuple:
        """Sort key: operation priority (CREATE < ALTER < DROP < unknown), then domain alphabetically."""
        return (
            _OPERATION_ORDER.get(self.operation, len(_OPERATION_ORDER)),
            self.domain,
        )

    @classmethod
    def from_dict(cls, entry_dict: Dict[str, Any]) -> "PlanRow":
        """Parse a version 2 changeset entry into a display entry without dropping data."""
        try:
            entity = PlanEntityChange.model_validate(entry_dict)
            operation = sanitize_for_terminal(entity.type_.upper())
            domain = sanitize_for_terminal(entity.object_id.domain.upper())
            sanitized_fqn = sanitize_for_terminal(entity.object_id.fqn)
            fqn = FQN.from_string(sanitized_fqn)
        except (ValidationError, FQNNameError) as e:
            # Forward-compatible fallback: if a future version changes the
            # changeset entry shape, the CLI degrades gracefully instead of crashing.
            log.debug(
                "Failed strict validation for changeset entry, using fallback parser: %s",
                e,
            )
            operation = sanitize_for_terminal(
                str(entry_dict.get("type", "UNKNOWN")).upper()
            )
            object_id = entry_dict.get("object_id", {})
            object_id = object_id if isinstance(object_id, dict) else {}
            domain = sanitize_for_terminal(
                str(object_id.get("domain", "UNKNOWN")).upper()
            )
            fqn = None
            try:
                if "fqn" in object_id:
                    fqn = FQN.from_string(sanitize_for_terminal(str(object_id["fqn"])))
            except Exception as e:  # noqa: BLE001
                log.debug(
                    "Failed to read FQN from provided string: %s",
                    e,
                )
                fqn = None

        return cls(
            operation=operation,
            domain=domain,
            fqn=fqn,
        )

    def display_fqn(self) -> str:
        """Format an FQN for human-friendly display (unquoted)."""
        if self.fqn is None:
            return "UNKNOWN"
        parts = []
        if self.fqn.database:
            parts.append(unquote_identifier(self.fqn.database))
        if self.fqn.schema:
            parts.append(unquote_identifier(self.fqn.schema))
        parts.append(unquote_identifier(self.fqn.name))
        result = ".".join(parts)
        if self.fqn.signature:
            result += self.fqn.signature
        return result


class PlanReporter(Reporter[PlanRow]):
    """Reporter for generic human-friendly plan/deploy output."""

    @dataclass
    class Summary:
        created: int = 0
        altered: int = 0
        dropped: int = 0

        @property
        def total(self):
            return self.created + self.altered + self.dropped

    def __init__(self, command_name: str = "plan"):
        super().__init__()
        self.command_name = command_name
        self._summary = self.Summary()

    @staticmethod
    def _style_for_operation(operation: str) -> Style:
        """Return the style for a given operation type."""
        if operation == "CREATE":
            return styles.CREATE_STYLE
        elif operation == "ALTER":
            return styles.ALTER_STYLE
        elif operation == "DROP":
            return styles.DROP_STYLE
        return styles.UNKNOWN_STYLE

    def extract_data(self, result_json: Dict[str, Any]) -> List[PlanEntityChange]:
        if not isinstance(result_json, dict):
            log.debug("Unexpected response type: %s, expected dict", type(result_json))
            raise CliError("Could not process response.")
        try:
            response = PlanResponse.model_validate(result_json)
        except ValidationError as e:
            log.debug("Failed to validate plan response: %s", e)
            raise CliError("Could not process response.")
        if response.version < 2:
            raise CliError("Only version 2+ plan responses are supported.")
        if response.version > 2:
            log.debug(
                "Plan response version %s is newer than supported (v2); rendering with best effort.",
                response.version,
            )
        return response.changeset

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[PlanRow]:
        rows: List[PlanRow] = []
        for entry_dict in data:
            parsed = PlanRow.from_dict(entry_dict)
            if parsed.operation == "CREATE":
                self._summary.created += 1
            elif parsed.operation == "ALTER":
                self._summary.altered += 1
            elif parsed.operation == "DROP":
                self._summary.dropped += 1
            else:
                log.debug("Unknown operation type: %s", parsed.operation)
            rows.append(parsed)
        rows.sort(key=lambda row: row.sort_key)
        return iter(rows)

    def print_renderables(self, data: Iterator[PlanRow]) -> None:
        for entry in data:
            style = self._style_for_operation(entry.operation)

            cli_console.styled_message(
                entry.operation.ljust(_OPERATION_WIDTH) + " ",
                style=style,
            )
            cli_console.styled_message(entry.domain.ljust(_DOMAIN_WIDTH) + " ")
            cli_console.styled_message(entry.display_fqn(), style=styles.DOMAIN_STYLE)
            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        total = self._summary.total
        if total == 0:
            return [Text("No changes detected.")]

        SummaryLabels = namedtuple(
            "SummaryLabels", ["created", "altered", "dropped", "header"]
        )
        labels = {
            "plan": SummaryLabels("to create", "to alter", "to drop", "Planned"),
            "deploy": SummaryLabels("created", "altered", "dropped", "Deployed"),
        }[self.command_name]

        parts = [
            Text(
                f"{self._summary.created} {labels.created}",
                styles.CREATE_STYLE,
            ),
            Text(
                f"{self._summary.altered} {labels.altered}",
                styles.ALTER_STYLE,
            ),
            Text(
                f"{self._summary.dropped} {labels.dropped}",
                styles.DROP_STYLE,
            ),
        ]
        entity_singular_or_plural = "entity" if total == 1 else "entities"
        result = [Text(f"{labels.header} {total} {entity_singular_or_plural} (")]
        for i, part in enumerate(parts):
            if i > 0:
                result.append(Text(", "))
            result.append(part)
        result.append(Text(")."))
        return result

    def _is_success(self) -> bool:
        return True
