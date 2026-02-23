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
from typing import Any, Dict, Iterator, List, Optional

from pydantic import BaseModel, Field, ValidationError
from rich.style import Style
from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.base import Reporter, cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)

_OPERATION_WIDTH = 8
_DOMAIN_WIDTH = 16


class PlanObjectId(BaseModel):
    """Object identifier within a changeset entry."""

    domain: str
    name: str
    fqn: str
    database: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")

    model_config = {"populate_by_name": True}

    def to_fqn(self) -> FQN:
        """Convert to CLI FQN instance."""
        return FQN.from_string(self.fqn)


class PlanEntityChange(BaseModel):
    """Top-level entity change in the changeset."""

    type_: str = Field(None, alias="type")
    object_id: PlanObjectId


class PlanResponse(BaseModel):
    """Top-level version 2 plan response."""

    version: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    changeset: List[PlanEntityChange] = Field(default_factory=list)


@dataclass
class PlanRow:
    """Parsed entry ready for display -- the common currency of plan reporters."""

    operation: str
    domain: str
    fqn: Optional[FQN] = None
    fqn_text: str = ""

    def display_fqn(self) -> str:
        """Format an FQN for human-friendly display (unquoted)."""
        if self.fqn is not None:
            parts = []
            if self.fqn.database:
                parts.append(unquote_identifier(self.fqn.database))
            if self.fqn.schema:
                parts.append(unquote_identifier(self.fqn.schema))
            parts.append(unquote_identifier(self.fqn.name))
            return ".".join(parts)

        fallback = self.fqn_text if self.fqn_text else "UNKNOWN"
        return sanitize_for_terminal(fallback)


def _style_for_operation(operation: str) -> Style:
    """Return the style for a given operation type."""
    if operation == "CREATE":
        return styles.CREATE_STYLE
    elif operation == "ALTER":
        return styles.ALTER_STYLE
    elif operation == "DROP":
        return styles.DROP_STYLE
    return styles.STATUS_STYLE


def _parse_entity_change(entry_dict: Dict[str, Any]) -> Optional[PlanRow]:
    """Parse a version 2 changeset entry into a display entry without dropping data."""
    try:
        entity = PlanEntityChange.model_validate(entry_dict)
        operation = entity.type_.upper()
        domain = entity.object_id.domain
        fqn = entity.object_id.to_fqn()
        fqn_text = ""
    except ValidationError as e:
        log.debug(
            "Failed strict validation for changeset entry, using fallback parser: %s", e
        )
        operation = str(entry_dict.get("type", "UNKNOWN")).upper()
        object_id = entry_dict.get("object_id", {})
        object_id = object_id if isinstance(object_id, dict) else {}
        domain = str(object_id.get("domain", "UNKNOWN"))
        raw_fqn_text = object_id.get("fqn") or object_id.get("name") or "UNKNOWN"
        fqn_text = str(raw_fqn_text)
        fqn = None
        try:
            if "fqn" in object_id:
                fqn = FQN.from_string(str(object_id["fqn"]))
        except Exception:  # noqa: BLE001
            fqn = None

    return PlanRow(
        operation=operation,
        domain=domain,
        fqn=fqn,
        fqn_text=fqn_text,
    )


class PlanReporter(Reporter[PlanRow]):
    """Reporter for generic human-friendly plan output."""

    @dataclass
    class Summary:
        created: int = 0
        altered: int = 0
        dropped: int = 0

        @property
        def total(self):
            return self.created + self.altered + self.dropped

    def __init__(self, verbose: bool = True, command_name: str = "plan"):
        super().__init__()
        self.command_name = command_name
        self._summary = self.Summary()
        self._verbose = verbose

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
                "Plan response version %s detected; rendering in compatibility mode.",
                response.version,
            )
        return response.changeset

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[PlanRow]:
        for entry_dict in data:
            parsed = _parse_entity_change(entry_dict)
            if parsed is not None:
                if parsed.operation == "CREATE":
                    self._summary.created += 1
                elif parsed.operation == "ALTER":
                    self._summary.altered += 1
                elif parsed.operation == "DROP":
                    self._summary.dropped += 1
                yield parsed

    def print_renderables(self, data: Iterator[PlanRow]) -> None:
        for entry in data:
            style = _style_for_operation(entry.operation)

            cli_console.styled_message(
                entry.operation.ljust(_OPERATION_WIDTH) + " ",
                style=style,
            )
            cli_console.styled_message(entry.domain.ljust(_DOMAIN_WIDTH) + " ")

            if entry.fqn is not None:
                fqn_text = sanitize_for_terminal(entry.display_fqn())

            cli_console.styled_message(fqn_text, style=styles.DOMAIN_STYLE)

            cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        total = self._summary.total
        if total == 0:
            return [Text("No changes detected.")]

        parts = []
        operations = {
            "plan": ("to create", "to alter", "to drop", "Planned"),
            "deploy": ("created", "altered", "dropped", "Deployed"),
        }
        if self._summary.created > 0:
            parts.append(
                Text(
                    f"{self._summary.created} {operations[self.command_name][0]}",
                    styles.CREATE_STYLE,
                )
            )
        if self._summary.altered > 0:
            parts.append(
                Text(
                    f"{self._summary.altered} {operations[self.command_name][1]}",
                    styles.ALTER_STYLE,
                )
            )
        if self._summary.dropped > 0:
            parts.append(
                Text(
                    f"{self._summary.dropped} {operations[self.command_name][2]}",
                    styles.DROP_STYLE,
                )
            )

        result = [Text(f"{operations[self.command_name][3]} {total} entities (")]
        for i, part in enumerate(parts):
            if i > 0:
                result.append(Text(", "))
            result.append(part)
        result.append(Text(")."))
        return result

    def _is_success(self) -> bool:
        return True
