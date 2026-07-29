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
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

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
_NESTED_KIND = "nested"
_CONTAINER_KINDS = (_COLLECTION_KIND, _NESTED_KIND)


_TREE_BRANCH = "├─ "  # non-last sibling at this level
_TREE_LAST = "└─ "  # last sibling at this level
_TREE_PIPE = "│  "  # ancestor still has siblings to come
_TREE_GAP = "   "  # ancestor was the last sibling (no pipe)


class PlanObjectId(BaseModel):
    """Object identifier within a changeset entry."""

    domain: str
    fqn: str


class PlanChange(BaseModel):
    """One entry inside an entity-level ``changes`` array."""

    kind: Optional[str] = None
    item_id: Optional[Union[Dict[str, Any], str]] = None
    attribute_name: Optional[str] = None
    collection_name: Optional[str] = None
    value: Any = None
    prev_value: Any = None
    changes: List["PlanChange"] = Field(default_factory=list)


PlanChange.model_rebuild()


class PlanEntityChange(BaseModel):
    """Top-level entity change in the changeset."""

    type_: Optional[str] = Field(None, alias="type")
    object_id: PlanObjectId
    changes: List[PlanChange] = Field(default_factory=list)


class PlanResponse(BaseModel):
    """Top-level plan response."""

    version: int
    changeset: List[PlanEntityChange] = Field(default_factory=list)


_OPERATION_ORDER = {"CREATE": 0, "ALTER": 1, "DROP": 2}


_MAX_VALUE_LEN = 50

_DIFF_CONTEXT = 12

_MIN_DIFF_TAIL = 8


def _collapse_whitespace(text: str) -> str:
    """Squash runs of whitespace (incl. newlines) to single spaces.

    ``sanitize_for_terminal`` only strips ANSI escapes, so a multi-line value
    would still contain newlines that break the single-line tree layout.
    """
    return " ".join(text.split())


def _cap_to_width(txt: str) -> str:
    """Cap an already-collapsed string at :data:`_MAX_VALUE_LEN`, appending an
    ellipsis when truncated."""
    return _window_value(txt, 0)


def _truncate_inline(text: str) -> str:
    """Collapse whitespace and cap the result at :data:`_MAX_VALUE_LEN`."""
    return _cap_to_width(_collapse_whitespace(text))


def _common_prefix_len(first: str, second: str) -> int:
    """Return the length of the longest shared leading substring."""
    limit = min(len(first), len(second))
    index = 0
    while index < limit and first[index] == second[index]:
        index += 1
    return index


def _window_value(txt: str, start: int) -> str:
    """Return a ``_MAX_VALUE_LEN`` slice of ``txt`` starting at ``start``.

    A leading ellipsis marks content clipped before the window (omitted when
    ``start`` is 0); a trailing ellipsis marks content clipped after it, so it's
    clear the value is only a fragment of a larger string.
    """
    end = start + _MAX_VALUE_LEN
    segment = txt[start:end]
    prefix = "…" if start > 0 else ""
    suffix = "…" if end < len(txt) else ""
    if prefix:
        segment = segment.lstrip()
    if suffix:
        segment = segment.rstrip()
    return prefix + segment + suffix


def _truncate_value_pair(prev: str, new: str) -> Tuple[str, str]:
    """Truncate a modified property's previous and new values for display.

    Both values are collapsed to a single line. When neither exceeds the width
    budget they're shown verbatim. When at least one is too long they're
    normally cut from the start — but if the two values share a long common
    prefix (e.g. a large ``SELECT`` whose only change is a trailing
    ``GROUP BY``), a head cut would show identical text on both sides of the
    arrow and hide the change. In that case a fixed-width window is anchored on
    the first differing character, keeping a little shared context before it,
    so the changed segment stays visible on both sides.
    """
    prev_collapsed = _collapse_whitespace(prev)
    new_collapsed = _collapse_whitespace(new)
    if len(prev_collapsed) <= _MAX_VALUE_LEN and len(new_collapsed) <= _MAX_VALUE_LEN:
        return prev_collapsed, new_collapsed

    diff_at = _common_prefix_len(prev_collapsed, new_collapsed)
    if diff_at <= _MAX_VALUE_LEN - _MIN_DIFF_TAIL:
        return _cap_to_width(prev_collapsed), _cap_to_width(new_collapsed)

    start = diff_at - _DIFF_CONTEXT
    return _window_value(prev_collapsed, start), _window_value(new_collapsed, start)


def _format_value(value: Any) -> Optional[str]:
    """Render a JSON-decoded value compactly, or ``None`` when the value is absent.
    Non-scalars are serialized to compact JSON.
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
    return sanitize_for_terminal(json.dumps(value, default=str, ensure_ascii=False))


def _item_desc(item_id: Any) -> str:
    """Human-readable label for a collection item's ``item_id``."""
    if isinstance(item_id, dict):
        desc = item_id.get("desc")
        return sanitize_for_terminal(desc) if isinstance(desc, str) else ""
    if isinstance(item_id, str):
        return sanitize_for_terminal(item_id)
    return ""


def _emit_property_name(attr: str) -> None:
    cli_console.styled_message(" ")
    cli_console.styled_message(attr, style=styles.NEUTRAL_STYLE)


def _emit_scalar(value_str: str) -> None:
    cli_console.styled_message(": ")
    cli_console.styled_message(value_str, style=styles.VALUE_STYLE)


def _emit_transition(prev_str: str, new_str: str) -> None:
    cli_console.styled_message(": ")
    cli_console.styled_message(prev_str, style=styles.VALUE_STYLE)
    cli_console.styled_message(" ")
    cli_console.styled_message("→", style=styles.ALTER_STYLE)
    cli_console.styled_message(" ")
    cli_console.styled_message(new_str, style=styles.VALUE_STYLE)


def _emit_property_value(value: Any, prev_value: Any) -> None:
    new = _format_value(value)
    prev = _format_value(prev_value)
    if new is not None and prev is not None:
        prev_str, new_str = _truncate_value_pair(prev, new)
        _emit_transition(prev_str, new_str)
    elif new is not None:
        _emit_scalar(_truncate_inline(new))


class _ChangeNode:
    """A typed changeset change. Subclasses render their own line content"""

    kind: str = ""
    sort_key: Tuple[int, int] = (100, 0)
    style: Style = styles.UNKNOWN_STYLE
    expand_children: bool = True

    def __init__(self, children: Optional[List["_ChangeNode"]] = None):
        self.children: List["_ChangeNode"] = children or []

    def _emit_keyword(self) -> None:
        cli_console.styled_message(self.kind, style=self.style)

    def render_content(self) -> None:
        raise NotImplementedError


class _PropertyNode(_ChangeNode):
    """Shared base for ``set``/``unset``/``changed`` — an ``attribute_name``
    plus its value(s)."""

    def __init__(
        self,
        attribute_name: Optional[str],
        value: Any = None,
        prev_value: Any = None,
        children: Optional[List[_ChangeNode]] = None,
    ):
        super().__init__(children)
        self.attribute_name = attribute_name
        self.value = value
        self.prev_value = prev_value

    def render_content(self) -> None:
        self._emit_keyword()
        attr = sanitize_for_terminal(self.attribute_name or "").upper()
        if not attr:
            return
        _emit_property_name(attr)
        self._emit_value_part()

    def _emit_value_part(self) -> None:
        pass


class _SetNode(_PropertyNode):
    kind = "set"
    sort_key = (0, 1)
    style = styles.CREATE_STYLE

    def _emit_value_part(self) -> None:
        new = _format_value(self.value)
        if new is not None:
            _emit_scalar(_truncate_inline(new))


class _ChangedNode(_PropertyNode):
    kind = "changed"
    sort_key = (1, 1)
    style = styles.ALTER_STYLE

    def _emit_value_part(self) -> None:
        _emit_property_value(self.value, self.prev_value)


class _UnsetNode(_PropertyNode):
    kind = "unset"
    sort_key = (2, 1)
    style = styles.DROP_STYLE


class _ItemNode(_ChangeNode):
    """Shared base for collection items (``added``/``modified``/``removed``)."""

    def __init__(self, item_id: Any, children: Optional[List[_ChangeNode]] = None):
        super().__init__(children)
        self.item_id = item_id

    def render_content(self) -> None:
        self._emit_keyword()
        desc = _item_desc(self.item_id)
        if desc:
            cli_console.styled_message(" " + desc)


class _ItemAddedNode(_ItemNode):
    kind = "added"
    sort_key = (0, 0)
    style = styles.CREATE_STYLE


class _ItemModifiedNode(_ItemNode):
    kind = "modified"
    sort_key = (1, 0)
    style = styles.ALTER_STYLE


class _ItemRemovedNode(_ItemNode):
    kind = "removed"
    sort_key = (2, 0)
    style = styles.DROP_STYLE
    # A removed item's sub-changes are noise — the whole item is gone.
    expand_children = False


class _ContainerNode(_ChangeNode):
    """A named group (``collection``/``nested``) whose ``label`` heads an
    indented block of child changes."""

    sort_key = (3, 0)

    def __init__(self, label: str, children: Optional[List[_ChangeNode]] = None):
        super().__init__(children)
        self.label = label

    def render_content(self) -> None:
        cli_console.styled_message(self.label)


class _GenericNode(_ChangeNode):
    """Best-effort rendering for an unrecognized change kind."""

    def __init__(
        self,
        kind: str,
        item_id: Any,
        attribute_name: Optional[str],
        value: Any,
        prev_value: Any,
        children: Optional[List[_ChangeNode]] = None,
    ):
        super().__init__(children)
        self.kind = kind
        self.item_id = item_id
        self.attribute_name = attribute_name
        self.value = value
        self.prev_value = prev_value

    def render_content(self) -> None:
        if self.kind:
            self._emit_keyword()
        desc = _item_desc(self.item_id)
        if desc:
            cli_console.styled_message(" " + desc)
        elif self.attribute_name:
            _emit_property_name(sanitize_for_terminal(self.attribute_name).upper())
            _emit_property_value(self.value, self.prev_value)


_ITEM_NODE_TYPES = {
    "added": _ItemAddedNode,
    "modified": _ItemModifiedNode,
    "removed": _ItemRemovedNode,
}


def _build_node(change: PlanChange) -> Optional[_ChangeNode]:
    """Convert a single change into its typed node, or ``None`` if it would
    render nothing (a label-less container, or an unknown change with neither a
    keyword nor content). Children are built recursively."""
    kind = sanitize_for_terminal(change.kind or "")
    children = _build_nodes(change.changes)

    if kind in _CONTAINER_KINDS:
        if kind == _COLLECTION_KIND:
            label = change.collection_name
        elif kind == _NESTED_KIND:
            label = change.attribute_name
        else:
            label = None
        if not label:
            return None
        return _ContainerNode(sanitize_for_terminal(label), children)

    if kind == "set":
        return _SetNode(change.attribute_name, change.value, children=children)
    if kind == "unset":
        return _UnsetNode(change.attribute_name, children=children)
    if kind == "changed":
        return _ChangedNode(
            change.attribute_name, change.value, change.prev_value, children=children
        )
    if kind in _ITEM_NODE_TYPES:
        return _ITEM_NODE_TYPES[kind](change.item_id, children)

    node = _GenericNode(
        kind,
        change.item_id,
        change.attribute_name,
        change.value,
        change.prev_value,
        children,
    )
    if not kind and not _item_desc(node.item_id) and not node.attribute_name:
        return None
    log.debug("Unrecognized change kind %r; rendering with a generic node", kind)
    return node


def _build_nodes(changes: List[PlanChange]) -> List[_ChangeNode]:
    return [node for c in changes if (node := _build_node(c)) is not None]


def _render_nodes(nodes: List[_ChangeNode], prefix: str = "") -> None:
    """Render sibling nodes as an indented tree.

    Owns the cross-cutting layout: stable sort by kind category, tree
    connectors, and skipping the children of nodes that opt out
    (``expand_children``). Each node renders only its own line content via
    :meth:`_ChangeNode.render_content`. ``prefix`` is the accumulated ancestor
    indentation (pipe/gap columns); each node appends its own connector, and its
    children inherit ``prefix`` extended by one more column.
    """
    ordered = sorted(nodes, key=lambda node: node.sort_key)
    last = len(ordered) - 1
    for index, node in enumerate(ordered):
        is_last = index == last
        cli_console.styled_message(
            prefix + (_TREE_LAST if is_last else _TREE_BRANCH), style="dim"
        )
        node.render_content()
        cli_console.styled_message("\n")
        if node.expand_children and node.children:
            _render_nodes(
                node.children, prefix + (_TREE_GAP if is_last else _TREE_PIPE)
            )


@dataclass
class PlanRow:
    """Parsed entry ready for display"""

    operation: str
    domain: str
    fqn: Optional[FQN] = None
    details: List[_ChangeNode] = field(default_factory=list)

    @property
    def sort_key(self) -> tuple:
        """Sort key: operation priority (CREATE < ALTER < DROP < unknown), then domain alphabetically."""
        return (
            _OPERATION_ORDER.get(self.operation, len(_OPERATION_ORDER)),
            self.domain,
        )

    @staticmethod
    def _alter_details(operation: str, changes: List[PlanChange]) -> List[_ChangeNode]:
        """Only ALTER entities render sub-changes; CREATE/DROP stay terse."""
        return _build_nodes(changes) if operation == "ALTER" else []

    @staticmethod
    def _parse_fqn(fqn: str) -> Optional[FQN]:
        """Parse an FQN string, degrading to ``None`` when it isn't parseable."""
        try:
            return FQN.from_string(sanitize_for_terminal(fqn))
        except FQNNameError as e:
            log.info("Could not parse FQN %r: %s", fqn, e)
            return None

    @classmethod
    def from_entity(cls, entity: PlanEntityChange) -> "PlanRow":
        """Build a display row from a validated changeset entry."""
        operation = sanitize_for_terminal((entity.type_ or "UNKNOWN").upper())
        domain = sanitize_for_terminal(entity.object_id.domain.upper())
        fqn = cls._parse_fqn(entity.object_id.fqn)
        details = cls._alter_details(operation, entity.changes)
        return cls(operation=operation, domain=domain, fqn=fqn, details=details)

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

    def parse_data(self, data: List[PlanEntityChange]) -> Iterator[PlanRow]:
        rows: List[PlanRow] = []
        for entity in data:
            parsed = PlanRow.from_entity(entity)
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
        entries = list(data)
        last_index = len(entries) - 1
        for index, entry in enumerate(entries):
            style = self._style_for_operation(entry.operation) + styles.BOLD_STYLE
            cli_console.styled_message(
                entry.operation.ljust(_OPERATION_WIDTH) + " ",
                style=style,
            )
            cli_console.styled_message(entry.domain.ljust(_DOMAIN_WIDTH) + " ")
            cli_console.styled_message(
                entry.display_fqn(), style=styles.OBJECT_NAME_STYLE
            )
            cli_console.styled_message("\n")
            _render_nodes(entry.details)
            if entry.details and index != last_index:
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
