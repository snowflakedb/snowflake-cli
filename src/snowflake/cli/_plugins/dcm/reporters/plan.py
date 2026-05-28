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
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

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
_COLLECTION_KIND = "collection"


@dataclass(frozen=True)
class _KindInfo:
    """Per-kind metadata: how to sort siblings, and how to color them.

    Keeping sort key and style in one row means a new kind only ever has
    to be added in one place — and the sort buckets stay aligned with the
    semantic CREATE / ALTER / DROP color scheme by construction.
    """

    sort_key: Tuple[int, int]
    style: Style


# Display order + style for sibling sub-changes under an ALTER row.
# Mirrors the top-level CREATE → ALTER → DROP ordering so the eye scans
# the same way whether it's an entity row or one of its indented
# children. Within a category the sub-keys group together ("added" and
# "set" both create things; "removed" and "unset" both destroy things);
# Python's ``list.sort`` is stable, so siblings sharing the same key
# preserve the server-supplied order.
_KIND_INFO: Dict[str, _KindInfo] = {
    "added": _KindInfo((0, 0), styles.CREATE_STYLE),
    "set": _KindInfo((0, 1), styles.CREATE_STYLE),
    "modified": _KindInfo((1, 0), styles.ALTER_STYLE),
    "renamed": _KindInfo((1, 1), styles.ALTER_STYLE),
    "removed": _KindInfo((2, 0), styles.DROP_STYLE),
    "unset": _KindInfo((2, 1), styles.DROP_STYLE),
}
_UNKNOWN_KIND_INFO = _KindInfo((3, 0), styles.UNKNOWN_STYLE)


def _kind_info(kind: str) -> _KindInfo:
    return _KIND_INFO.get((kind or "").lower(), _UNKNOWN_KIND_INFO)


# Tree-prefix cell glyphs (each cell is 3 columns wide so the tree column
# stays narrow and aligned regardless of depth). Box-drawing characters
# render in any UTF-8 terminal; the cells render with the default style
# so the colored kind keyword that follows is what catches the eye.
_TREE_BRANCH = "├─ "  # non-last sibling at this level
_TREE_LAST = "└─ "  # last sibling at this level
_TREE_PIPE = "│  "  # ancestor still has siblings to come
_TREE_GAP = "   "  # ancestor was the last sibling (no pipe)


def _render_tree_prefix(is_last_chain: Tuple[bool, ...]) -> str:
    """Render a tree-style leading prefix from ``is_last_chain``.

    Each element in the chain describes one ancestor level (ending with
    the node itself). ``True`` means "this node / ancestor was the last
    sibling at its level". The last element decides the current node's
    connector (└─ vs ├─); preceding elements decide whether each ancestor
    column shows a vertical continuation (│) or a blank gap.
    """
    if not is_last_chain:
        return ""
    parts = [_TREE_GAP if was_last else _TREE_PIPE for was_last in is_last_chain[:-1]]
    parts.append(_TREE_LAST if is_last_chain[-1] else _TREE_BRANCH)
    return "".join(parts)


class PlanObjectId(BaseModel):
    """Object identifier within a changeset entry."""

    domain: str
    fqn: str


class PlanChange(BaseModel):
    """One entry inside an entity-level ``changes`` array.

    Observed shapes in v2 changesets:
    - ``kind == "collection"`` — grouping wrapper: ``collection_name`` + nested ``changes``.
    - ``kind in ("added", "removed", "modified")`` — carries ``item_id``, which may be
      either a dict (with a ``desc`` and other metadata) or a bare string identifier
      (e.g. a column name).
    - ``kind == "set"`` — attribute change: ``attribute_name`` + ``value``.
    - ``kind == "unset"`` — attribute reset: ``attribute_name`` + ``prev_value``.

    ``modified`` (and sometimes ``added``) entries may additionally carry their own
    nested ``changes`` describing the sub-modifications.
    """

    kind: Optional[str] = None
    item_id: Optional[Union[Dict[str, Any], str]] = None
    attribute_name: Optional[str] = None
    value: Any = None
    prev_value: Any = None
    changes: List["PlanChange"] = Field(default_factory=list)


PlanChange.model_rebuild()


class PlanEntityChange(BaseModel):
    """Top-level entity change in the changeset."""

    type_: str = Field(None, alias="type")
    object_id: PlanObjectId
    changes: List[PlanChange] = Field(default_factory=list)


class PlanResponse(BaseModel):
    """Top-level plan response."""

    version: int
    changeset: List[PlanEntityChange] = Field(default_factory=list)


_OPERATION_ORDER = {"CREATE": 0, "ALTER": 1, "DROP": 2}


@dataclass
class PlanDetail:
    """One indented sub-line under an entity row, e.g. ``added <desc>``.

    ``is_last_chain`` carries one boolean per ancestor level, ending with
    the node itself. ``True`` means "this node (or ancestor) was the last
    sibling at its level"; the renderer uses this to draw tree connectors
    (├─ / └─ / │ / blank) without needing to keep a nested tree around.
    """

    kind: str
    desc: str
    is_last_chain: Tuple[bool, ...] = ()

    @property
    def depth(self) -> int:
        return len(self.is_last_chain)


def _format_scalar_value(value: Any) -> Optional[str]:
    """Render a JSON-decoded value compactly, or ``None`` for complex types.

    Used to render the right-hand side of ``set <attr> = <value>`` lines.
    Dicts and lists are skipped (they'd blow up the output).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        sanitized = sanitize_for_terminal(value)
        return "''" if sanitized == "" else sanitized
    return None


def _format_change_desc(
    kind: str,
    item_id: Any,
    attribute_name: Optional[str],
    value: Any,
) -> str:
    """Build the human-readable description for a single change entry.

    Handles every observed shape:
    - ``item_id`` is a dict with ``desc`` → use ``desc``.
    - ``item_id`` is a bare string → use it directly.
    - ``set`` → ``<attribute_name> = <value>`` (scalar values only).
    - ``unset`` → ``<attribute_name>``.
    """
    if isinstance(item_id, dict):
        desc_val = item_id.get("desc")
        if isinstance(desc_val, str):
            return sanitize_for_terminal(desc_val)
    if isinstance(item_id, str):
        return sanitize_for_terminal(item_id)
    if isinstance(attribute_name, str) and attribute_name:
        attr = sanitize_for_terminal(attribute_name)
        if kind == "set":
            scalar = _format_scalar_value(value)
            if scalar is not None:
                return f"{attr} = {scalar}"
        return attr
    return ""


@dataclass(frozen=True)
class _ChangeReader:
    """Field-access adapter so one flattener handles both shapes.

    The strict path operates on validated :class:`PlanChange` Pydantic
    models; the fallback path operates on the raw decoded dict from the
    server. Both have the same conceptual fields, so we abstract field
    access behind these callables and share a single walker.
    """

    get_kind: Callable[[Any], str]
    get_item_id: Callable[[Any], Any]
    get_attribute_name: Callable[[Any], Any]
    get_value: Callable[[Any], Any]
    get_children: Callable[[Any], List[Any]]


def _raw_children(change: Dict[str, Any]) -> List[Dict[str, Any]]:
    nested = change.get("changes") or []
    return [c for c in nested if isinstance(c, dict)]


_MODEL_READER = _ChangeReader(
    get_kind=lambda c: c.kind or "",
    get_item_id=lambda c: c.item_id,
    get_attribute_name=lambda c: c.attribute_name,
    get_value=lambda c: c.value,
    get_children=lambda c: c.changes,
)

_RAW_READER = _ChangeReader(
    get_kind=lambda c: str(c.get("kind", "")),
    get_item_id=lambda c: c.get("item_id"),
    get_attribute_name=lambda c: c.get("attribute_name"),
    get_value=lambda c: c.get("value"),
    get_children=_raw_children,
)


def _expand_collections(items: List[Any], reader: _ChangeReader) -> List[Any]:
    """Inline ``collection``-kind wrappers so their children become real siblings.

    Collection wrappers don't produce a line of their own, so for tree
    rendering we need to know the *displayed* sibling list at each level.
    Flattening collections in advance lets us compute ``is_last`` against
    the siblings the user will actually see.
    """
    out: List[Any] = []
    for item in items:
        if reader.get_kind(item).lower() == _COLLECTION_KIND:
            out.extend(_expand_collections(reader.get_children(item) or [], reader))
        else:
            out.append(item)
    return out


def _flatten_via(
    items: List[Any],
    reader: _ChangeReader,
    ancestor_is_last: Tuple[bool, ...] = (),
) -> List[PlanDetail]:
    """Walk a changes tree producing indented detail lines with tree metadata.

    ``kind == "collection"`` wrappers are unwrapped (no line emitted); their
    children take their place in the displayed sibling list at the current
    depth. Every other kind produces one line; if it carries nested
    ``changes``, those are emitted one level deeper. ``ancestor_is_last``
    records, for each ancestor level, whether the ancestor was the last
    sibling at its level — which the renderer turns into tree connectors.
    """
    expanded = _expand_collections(items, reader)
    # Pre-render to filter out leaves that would produce empty rows, so the
    # ``is_last`` computation reflects only what's actually displayed.
    renderable: List[Tuple[Any, str, str]] = []
    for item in expanded:
        kind = reader.get_kind(item).lower()
        desc = _format_change_desc(
            kind,
            reader.get_item_id(item),
            reader.get_attribute_name(item),
            reader.get_value(item),
        )
        sanitized_kind = sanitize_for_terminal(kind)
        if not sanitized_kind and not desc:
            continue
        renderable.append((item, sanitized_kind, desc))

    # Stable sort: group siblings by kind category so users see all
    # creations, then modifications, then deletions in a predictable order.
    renderable.sort(key=lambda triple: _kind_info(triple[1]).sort_key)

    out: List[PlanDetail] = []
    total = len(renderable)
    for index, (item, sanitized_kind, desc) in enumerate(renderable):
        is_last = index == total - 1
        chain = ancestor_is_last + (is_last,)
        out.append(PlanDetail(kind=sanitized_kind, desc=desc, is_last_chain=chain))
        children = reader.get_children(item)
        if children:
            out.extend(_flatten_via(children, reader, ancestor_is_last=chain))
    return out


def _flatten_changes(
    changes: List[PlanChange],
    ancestor_is_last: Tuple[bool, ...] = (),
) -> List[PlanDetail]:
    """Strict-mode flattener over validated :class:`PlanChange` models."""
    return _flatten_via(list(changes), _MODEL_READER, ancestor_is_last)


def _flatten_changes_from_raw(
    raw: Any,
    ancestor_is_last: Tuple[bool, ...] = (),
) -> List[PlanDetail]:
    """Best-effort flattener over the raw ``changes`` payload.

    Used by the fallback parser when the strict pydantic model can't validate
    the entry; we still surface as many sub-change lines as possible, with
    correct tree metadata.
    """
    items = [c for c in (raw or []) if isinstance(c, dict)]
    return _flatten_via(items, _RAW_READER, ancestor_is_last)


@dataclass
class PlanRow:
    """Parsed entry ready for display"""

    operation: str
    domain: str
    fqn: Optional[FQN] = None
    details: List[PlanDetail] = field(default_factory=list)

    @property
    def sort_key(self) -> tuple:
        """Sort key: operation priority (CREATE < ALTER < DROP < unknown), then domain alphabetically."""
        return (
            _OPERATION_ORDER.get(self.operation, len(_OPERATION_ORDER)),
            self.domain,
        )

    @classmethod
    def from_dict(cls, entry_dict: Dict[str, Any]) -> "PlanRow":
        """Parse a changeset entry into a display entry without dropping data."""
        try:
            entity = PlanEntityChange.model_validate(entry_dict)
            operation = sanitize_for_terminal(entity.type_.upper())
            domain = sanitize_for_terminal(entity.object_id.domain.upper())
            sanitized_fqn = sanitize_for_terminal(entity.object_id.fqn)
            fqn = FQN.from_string(sanitized_fqn)
            details = _flatten_changes(entity.changes) if operation == "ALTER" else []
        except (ValidationError, FQNNameError) as e:
            # Forward-compatible fallback: if a future version changes the
            # changeset entry shape, the CLI degrades gracefully instead of crashing.
            log.info(
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
                log.info(
                    "Failed to read FQN from provided string: %s",
                    e,
                )
                fqn = None
            details = (
                _flatten_changes_from_raw(entry_dict.get("changes"))
                if operation == "ALTER"
                else []
            )

        return cls(
            operation=operation,
            domain=domain,
            fqn=fqn,
            details=details,
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

    def __init__(self, save_output: bool = False, command_name: str = "plan"):
        super().__init__(save_output=save_output)
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

    @staticmethod
    def _style_for_change_kind(kind: str) -> Style:
        """Return the style for a sub-change kind (added/removed/modified/set/…)."""
        return _kind_info(kind).style

    def extract_data(self, result_json: Dict[str, Any]) -> List[PlanEntityChange]:
        if not isinstance(result_json, dict):
            log.info("Unexpected response type: %s, expected dict", type(result_json))
            raise CliError("Could not process response.")
        try:
            response = PlanResponse.model_validate(result_json)
        except ValidationError as e:
            log.info("Failed to validate plan response: %s", e)
            raise CliError("Could not process response.")
        if response.version > 2:
            log.info(
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
                log.info("Unknown operation type: %s", parsed.operation)
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
            for detail in entry.details:
                self._print_detail(detail)

    def _print_detail(self, detail: PlanDetail) -> None:
        # Tree prefix renders with the default style so the colored kind
        # keyword that follows is what catches the eye.
        prefix = _render_tree_prefix(detail.is_last_chain)
        if prefix:
            cli_console.styled_message(prefix)
        if detail.kind:
            # Only the operation keyword (added / removed / modified / set / …)
            # is colored; the entity name / description that follows renders
            # with the terminal default so the colored kind stands out. Unlike
            # the top-level CREATE/ALTER/DROP row, child rows do NOT pad the
            # keyword to a fixed column — the tree prefix already provides
            # visual indentation, so a single space separator keeps the line
            # compact.
            cli_console.styled_message(
                detail.kind,
                style=self._style_for_change_kind(detail.kind),
            )
        if detail.desc:
            cli_console.styled_message(" " + detail.desc)
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
            "purge": SummaryLabels("created", "altered", "dropped", "Purged"),
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
