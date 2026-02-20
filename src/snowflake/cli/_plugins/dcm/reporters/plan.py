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
from typing import Annotated, Any, Dict, Iterator, List, Literal, Optional, Union

from pydantic import BaseModel, Discriminator, Field, Tag, ValidationError
from rich.style import Style
from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.base import Reporter, cli_console
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models for the Change type hierarchy
# Based on TypeScript interface in plans/plan_refactor/3_typescript_interface.md
# ---------------------------------------------------------------------------

# --- Attribute operations ---


class AttributeSet(BaseModel):
    """A new property or initial state being set."""

    kind: Literal["set"]
    attribute_name: str
    value: Any


class AttributeUnset(BaseModel):
    """A property being explicitly removed or reset."""

    kind: Literal["unset"]
    attribute_name: str
    prev_value: Any


class AttributeChanged(BaseModel):
    """A simple update to an existing scalar field."""

    kind: Literal["changed"]
    attribute_name: str
    value: Any
    prev_value: Any


# --- Collection item types ---

ItemId = Union[str, int, Dict[str, Any]]


class ItemAdded(BaseModel):
    """An item added to a collection."""

    kind: Literal["added"]
    item_id: ItemId
    changes: List["Change"] = Field(default_factory=list)


class ItemModified(BaseModel):
    """An existing item modified within a collection."""

    kind: Literal["modified"]
    item_id: ItemId
    changes: List["Change"] = Field(default_factory=list)


class ItemRemoved(BaseModel):
    """An item removed from a collection."""

    kind: Literal["removed"]
    item_id: ItemId
    changes: List["Change"] = Field(default_factory=list)


CollectionItemChange = Annotated[
    Union[
        Annotated[ItemAdded, Tag("added")],
        Annotated[ItemModified, Tag("modified")],
        Annotated[ItemRemoved, Tag("removed")],
    ],
    Discriminator("kind"),
]


# --- Collection and nested changes ---


class CollectionChange(BaseModel):
    """A unified list of items (columns, grants, constraints, etc.)."""

    kind: Literal["collection"]
    collection_name: str
    id_label: Optional[str] = None
    changes: List[CollectionItemChange] = Field(default_factory=list)


class NestedChange(BaseModel):
    """A logical grouping of changes for a specific nested path."""

    kind: Literal["nested"]
    attribute_name: str
    changes: List["Change"] = Field(default_factory=list)


# --- The unified Change discriminated union ---

Change = Annotated[
    Union[
        Annotated[AttributeSet, Tag("set")],
        Annotated[AttributeUnset, Tag("unset")],
        Annotated[AttributeChanged, Tag("changed")],
        Annotated[CollectionChange, Tag("collection")],
        Annotated[NestedChange, Tag("nested")],
    ],
    Discriminator("kind"),
]

# Rebuild models that use forward references to Change
ItemAdded.model_rebuild()
ItemModified.model_rebuild()
ItemRemoved.model_rebuild()
NestedChange.model_rebuild()


# ---------------------------------------------------------------------------
# Pydantic models for the version 2 plan response envelope
# ---------------------------------------------------------------------------


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

    type: str  # noqa: A003
    object_id: PlanObjectId
    changes: List[Change] = Field(default_factory=list)


class PlanResponse(BaseModel):
    """Top-level version 2 plan response."""

    version: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    changeset: List[Dict[str, Any]] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def _display_fqn(fqn: FQN) -> str:
    """Format an FQN for human-friendly display (unquoted)."""
    parts = []
    if fqn.database:
        parts.append(unquote_identifier(fqn.database))
    if fqn.schema:
        parts.append(unquote_identifier(fqn.schema))
    parts.append(unquote_identifier(fqn.name))
    return ".".join(parts)


# ---------------------------------------------------------------------------
# Parsed display model
# ---------------------------------------------------------------------------


@dataclass
class PlanDisplayEntry:
    """Parsed entry ready for display -- the common currency of plan reporters."""

    operation: str
    domain: str
    fqn: Optional[FQN] = None
    fqn_text: str = ""
    changes: List[Dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Terse output
# ---------------------------------------------------------------------------

_OPERATION_WIDTH = 8
_DOMAIN_WIDTH = 16


def _style_for_operation(operation: str) -> Style:
    """Return the style for a given operation type."""
    if operation == "CREATE":
        return styles.CREATE_STYLE
    elif operation == "ALTER":
        return styles.ALTER_STYLE
    elif operation == "DROP":
        return styles.DROP_STYLE
    return styles.STATUS_STYLE


def _print_terse(entry: PlanDisplayEntry) -> None:
    """Print a single terse plan entry line."""
    style = _style_for_operation(entry.operation)

    cli_console.styled_message(
        entry.operation.ljust(_OPERATION_WIDTH) + " ",
        style=style,
    )
    cli_console.styled_message(entry.domain.ljust(_DOMAIN_WIDTH) + " ")

    if entry.fqn is not None:
        fqn_text = sanitize_for_terminal(_display_fqn(entry.fqn))
    else:
        fallback = entry.fqn_text if entry.fqn_text else "UNKNOWN"
        fqn_text = sanitize_for_terminal(fallback)
    cli_console.styled_message(fqn_text, style=styles.DOMAIN_STYLE)

    cli_console.styled_message("\n")


# ---------------------------------------------------------------------------
# Parse entity change
# ---------------------------------------------------------------------------


def _parse_entity_change(
    entry_dict: Dict[str, Any], summary
) -> Optional[PlanDisplayEntry]:
    """Parse a version 2 changeset entry into a display entry without dropping data."""
    try:
        entity = PlanEntityChange.model_validate(entry_dict)
        operation = entity.type.upper()
        domain = entity.object_id.domain
        fqn = entity.object_id.to_fqn()
        fqn_text = ""
        raw_changes = entry_dict.get("changes", [])
        changes = raw_changes if isinstance(raw_changes, list) else []
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
        raw_changes = entry_dict.get("changes", [])
        changes = raw_changes if isinstance(raw_changes, list) else []

    if operation == "CREATE":
        summary.created += 1
    elif operation == "ALTER":
        summary.altered += 1
    elif operation == "DROP":
        summary.dropped += 1

    return PlanDisplayEntry(
        operation=operation,
        domain=domain,
        fqn=fqn,
        fqn_text=fqn_text,
        changes=changes,
    )


# ---------------------------------------------------------------------------
# PlanReporter
# ---------------------------------------------------------------------------


class PlanReporter(Reporter[PlanDisplayEntry]):
    """Reporter for generic human-friendly plan output."""

    @dataclass
    class Summary:
        created: int = 0
        altered: int = 0
        dropped: int = 0

        @property
        def total(self):
            return self.created + self.altered + self.dropped

    def __init__(self, verbose: bool = True):
        super().__init__()
        self.command_name = "plan"
        self._summary = self.Summary()
        self._verbose = verbose

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[PlanDisplayEntry]:
        for entry_dict in data:
            parsed = _parse_entity_change(entry_dict, self._summary)
            if parsed is not None:
                yield parsed

    def print_renderables(self, data: Iterator[PlanDisplayEntry]) -> None:
        for entry in data:
            _print_terse(entry)

    def _generate_summary_renderables(self) -> List[Text]:
        total = self._summary.total
        if total == 0:
            return [Text("No changes detected.")]

        parts = []
        if self._summary.created > 0:
            parts.append(
                Text(f"{self._summary.created} to create", styles.CREATE_STYLE)
            )
        if self._summary.altered > 0:
            parts.append(Text(f"{self._summary.altered} to alter", styles.ALTER_STYLE))
        if self._summary.dropped > 0:
            parts.append(Text(f"{self._summary.dropped} to drop", styles.DROP_STYLE))

        result = [Text(f"Planned {total} entities (")]
        for i, part in enumerate(parts):
            if i > 0:
                result.append(Text(", "))
            result.append(part)
        result.append(Text(")."))
        return result

    def _is_success(self) -> bool:
        return True
