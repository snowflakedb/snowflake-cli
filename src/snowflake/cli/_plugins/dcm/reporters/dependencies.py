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
"""Generates a Mermaid dependency diagram from a DCM ``ANALYZE`` response.

The same ``EXECUTE DCM PROJECT ... ANALYZE`` payload consumed by
:mod:`~snowflake.cli._plugins.dcm.reporters.analyze` also carries, for every
definition, its upstream ``dependencies`` and (for dynamic tables) a
``TARGET_LAG`` property. This module walks that payload, keeps only the
"data" objects worth graphing (tables, dynamic tables, views, functions,
procedures, tasks), and emits a Markdown file containing a Mermaid
``flowchart`` so the dependency graph can be opened in any IDE's Markdown
preview.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

from rich.text import Text
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.analyze import _files_from_response
from snowflake.cli._plugins.dcm.reporters.base import Reporter
from snowflake.cli._plugins.dcm.utils import OUTPUT_FOLDER
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

DEFAULT_DEPENDENCIES_FILENAME = "dependencies.md"

# A node's identity within the project: (database, schema, name, domain).
# ``database`` and ``schema`` may be ``None`` for account-level objects.
NodeKey = Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]


@dataclass(frozen=True)
class _Category:
    """Presentation metadata for one graphable kind of object."""

    key: str  # stable bucket key (drives declaration + class grouping order)
    mermaid_class: str  # Mermaid ``classDef`` name
    label_prefix: str  # e.g. "Table", "View" (unused for dynamic tables)
    section_title: str  # comment header in the generated diagram


# Declaration order matches the grouping used in the diagram. Anything whose
# ``refined_domain`` is absent from this map (databases, schemas, warehouses,
# roles, sequences, stages, file formats, alerts, ...) is treated as a
# structural object and excluded from both nodes and edges.
_CATEGORIES: List[_Category] = [
    _Category("table", "table", "Table", "Tables"),
    _Category("dynamic_table", "dynTable", "Dynamic Table", "Dynamic Tables"),
    _Category("view", "view", "View", "Views"),
    _Category("function", "func", "Function", "Functions"),
    _Category("procedure", "proc", "Procedure", "Procedures"),
    _Category("task", "task", "Task", "Tasks"),
]

_CATEGORY_BY_KEY: Dict[str, _Category] = {c.key: c for c in _CATEGORIES}

# ``refined_domain`` value -> category key.
_DOMAIN_TO_CATEGORY: Dict[str, str] = {
    "table": "table",
    "dynamic_table": "dynamic_table",
    "view": "view",
    "function": "function",
    "data_metric_function": "function",
    "procedure": "procedure",
    "task": "task",
}

# Mermaid ``classDef`` styling, reproduced from the reference diagram so the
# generated file renders with consistent, readable colors.
_CLASS_DEFS: List[str] = [
    "classDef table     fill:#3b82f6,stroke:#1d4ed8,stroke-width:2px,color:#fff",
    "classDef dynTable  fill:#10b981,stroke:#065f46,stroke-width:2px,color:#fff",
    "classDef view      fill:#8b5cf6,stroke:#5b21b6,stroke-width:2px,color:#fff",
    "classDef func      fill:#f59e0b,stroke:#b45309,stroke-width:2px,color:#000",
    "classDef proc      fill:#ef4444,stroke:#991b1b,stroke-width:2px,color:#fff",
    "classDef task      fill:#64748b,stroke:#334155,stroke-width:2px,color:#fff",
]

_MERMAID_INIT = '%%{init: {"layout": "elk"}}%%'


@dataclass
class _Node:
    key: NodeKey
    mermaid_id: str
    category: str
    display_name: str
    qualifier: str
    lag: Optional[str] = None


@dataclass
class DependencyGraph:
    """A resolved, render-ready dependency graph."""

    # Category key -> nodes, preserving first-seen (file/definition) order.
    nodes_by_category: Dict[str, List[_Node]] = field(default_factory=dict)
    # Ordered, de-duplicated (source_mermaid_id, target_mermaid_id) edges.
    edges: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def node_count(self) -> int:
        return sum(len(nodes) for nodes in self.nodes_by_category.values())

    @property
    def edge_count(self) -> int:
        return len(self.edges)


def _sanitize_mermaid_id(name: str) -> str:
    """Turn an object name into a Mermaid-safe node id.

    Every character outside ``[0-9A-Za-z_]`` becomes ``_`` so that function /
    procedure signatures (which contain parentheses, spaces and commas) yield
    a valid identifier.
    """
    return re.sub(r"[^0-9A-Za-z_]", "_", name)


def _node_key(id_dict: Dict[str, Any]) -> NodeKey:
    return (
        id_dict.get("database"),
        id_dict.get("schema"),
        id_dict.get("name"),
        id_dict.get("domain"),
    )


def _display_name(name: str) -> str:
    """Strip a function/procedure signature, keeping the bare object name."""
    return name.split("(", 1)[0]


def _qualifier(id_dict: Dict[str, Any]) -> str:
    parts = [
        part
        for part in (id_dict.get("database"), id_dict.get("schema"))
        if isinstance(part, str) and part
    ]
    return ".".join(parts)


def _target_lag(definition: Dict[str, Any]) -> Optional[str]:
    for prop in definition.get("properties") or []:
        if isinstance(prop, dict) and prop.get("name") == "TARGET_LAG":
            value = prop.get("value")
            if isinstance(value, str) and value:
                return value
    return None


class _MermaidIdAllocator:
    """Hands out unique Mermaid ids, disambiguating sanitized collisions."""

    def __init__(self) -> None:
        self._used: set[str] = set()

    def allocate(self, name: str) -> str:
        base = _sanitize_mermaid_id(name) or "node"
        candidate = base
        suffix = 2
        while candidate in self._used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        self._used.add(candidate)
        return candidate


def build_dependency_graph(files: List[Dict[str, Any]]) -> DependencyGraph:
    """Build a :class:`DependencyGraph` from the ``files`` array of an analyze response."""
    graph = DependencyGraph()
    allocator = _MermaidIdAllocator()
    nodes_by_key: Dict[NodeKey, _Node] = {}

    # First pass: register every graphable definition as a node, preserving
    # file/definition order within each category bucket.
    for file_entry in files:
        if not isinstance(file_entry, dict):
            continue
        for definition in file_entry.get("definitions") or []:
            if not isinstance(definition, dict):
                continue
            refined = definition.get("refined_domain")
            category = _DOMAIN_TO_CATEGORY.get(refined) if refined else None
            if category is None:
                continue
            id_dict = definition.get("id")
            if not isinstance(id_dict, dict) or not id_dict.get("name"):
                continue
            key = _node_key(id_dict)
            if key in nodes_by_key:
                continue
            node = _Node(
                key=key,
                mermaid_id=allocator.allocate(id_dict["name"]),
                category=category,
                display_name=_display_name(id_dict["name"]),
                qualifier=_qualifier(id_dict),
                lag=_target_lag(definition) if category == "dynamic_table" else None,
            )
            nodes_by_key[key] = node
            graph.nodes_by_category.setdefault(category, []).append(node)

    # Second pass: resolve dependency edges. Only edges between graphable
    # nodes are kept; dependencies on structural objects are dropped.
    seen_edges: set[Tuple[str, str]] = set()
    for file_entry in files:
        if not isinstance(file_entry, dict):
            continue
        for definition in file_entry.get("definitions") or []:
            if not isinstance(definition, dict):
                continue
            target = nodes_by_key.get(_node_key(definition.get("id") or {}))
            if target is None:
                continue
            for dependency in definition.get("dependencies") or []:
                if not isinstance(dependency, dict):
                    continue
                source_id = dependency.get("source_id")
                if not isinstance(source_id, dict):
                    continue
                source = nodes_by_key.get(_node_key(source_id))
                if source is None or source.mermaid_id == target.mermaid_id:
                    continue
                edge = (source.mermaid_id, target.mermaid_id)
                if edge in seen_edges:
                    continue
                seen_edges.add(edge)
                graph.edges.append(edge)

    return graph


def _node_label(node: _Node) -> str:
    name = node.display_name.replace('"', "'")
    qualifier = node.qualifier.replace('"', "'")
    if node.category == "dynamic_table":
        lag = (node.lag or "unknown").replace('"', "'")
        body = f"Dynamic Table [lag: {lag}]\\n{name}"
    else:
        prefix = _CATEGORY_BY_KEY[node.category].label_prefix
        body = f"{prefix}: {name}"
    if qualifier:
        body = f"{body}\\n{qualifier}"
    return body


def render_dependencies_markdown(graph: DependencyGraph, project_name: str) -> str:
    """Render a full Markdown document embedding the Mermaid dependency diagram."""
    lines: List[str] = [
        f"# DCM Project dependencies for {project_name}",
        "",
        "> Auto-generated by `snow dcm dependencies`.",
        "> Open this file in your IDE's Markdown preview to explore the graph.",
        "",
        "```mermaid",
        _MERMAID_INIT,
        "flowchart LR",
        "",
    ]

    if graph.node_count == 0:
        lines.append("    %% No graphable objects found in this project.")
        lines.append("```")
        lines.append("")
        return "\n".join(lines)

    for category in _CATEGORIES:
        nodes = graph.nodes_by_category.get(category.key)
        if not nodes:
            continue
        lines.append(f"    %% ── {category.section_title} ──")
        for node in nodes:
            lines.append(f'    {node.mermaid_id}("{_node_label(node)}")')
        lines.append("")

    if graph.edges:
        lines.append("    %% ── Edges ──")
        for source, target in graph.edges:
            lines.append(f"    {source} --> {target}")
        lines.append("")

    lines.append("    linkStyle default stroke:#475569,stroke-width:2.5px")
    lines.append("")
    for class_def in _CLASS_DEFS:
        lines.append(f"    {class_def}")
    lines.append("")

    for category in _CATEGORIES:
        nodes = graph.nodes_by_category.get(category.key)
        if not nodes:
            continue
        ids = ",".join(node.mermaid_id for node in nodes)
        lines.append(f"    class {ids} {category.mermaid_class}")

    lines.append("```")
    lines.append("")
    return "\n".join(lines)


class DependenciesReporter(Reporter[Dict[str, Any]]):
    """Writes a Mermaid dependency diagram and points the user at the file.

    The ``ANALYZE`` payload is identical to the one consumed by the
    ``compile`` command; here we project it onto a dependency graph and
    persist a Markdown file rather than printing findings. Analyze issues, if
    any, do not fail this command — the dependency diagram is informational.
    """

    def __init__(
        self,
        project_identifier: Optional[FQN] = None,
        output_path: Optional[SecurePath] = None,
        save_output: bool = False,
    ) -> None:
        super().__init__(save_output=save_output)
        self.command_name = "dependencies"
        # ``dependencies`` writes its own ``dependencies.md`` (always) and
        # downloads backend output via ``collect_output``; the raw response is
        # redundant, so don't write ``dependencies_result.json``.
        self.write_result_file = False
        self._project_identifier = project_identifier
        self._output_path = output_path or (
            SecurePath(OUTPUT_FOLDER) / DEFAULT_DEPENDENCIES_FILENAME
        )
        self._graph: Optional[DependencyGraph] = None
        self._written_path: Optional[str] = None

    def _project_name(self) -> str:
        if self._project_identifier is not None:
            return self._project_identifier.name
        return "DCM Project"

    def extract_data(self, result_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _files_from_response(result_json)

    def parse_data(self, data: List[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        self._graph = build_dependency_graph(data)
        # Nothing is streamed to ``print_renderables``; the graph lives on
        # ``self`` and is rendered/written there.
        return iter(())

    def print_renderables(self, data: Iterator[Dict[str, Any]]) -> None:
        for _ in data:
            pass
        graph = self._graph if self._graph is not None else DependencyGraph()
        markdown = render_dependencies_markdown(graph, self._project_name())

        output = self._output_path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(markdown)
        self._written_path = str(output.path.resolve())
        log.info(
            "Wrote DCM dependency diagram (%d nodes, %d edges) to %s.",
            graph.node_count,
            graph.edge_count,
            self._written_path,
        )

    def print_summary(self) -> None:
        """Print summary without a leading blank line, one item per line."""
        renderables = self._generate_summary_renderables()
        for renderable in renderables:
            cli_console.styled_message(renderable.plain, style=renderable.style)
            cli_console.styled_message("\n")
        cli_console.styled_message("\n")

    def _generate_summary_renderables(self) -> List[Text]:
        graph = self._graph if self._graph is not None else DependencyGraph()
        if graph.node_count == 0:
            return [
                Text(
                    "No objects were found to graph in this DCM Project.",
                    styles.WARNING_STYLE,
                )
            ]
        objects_word = "object" if graph.node_count == 1 else "objects"
        deps_word = "dependency" if graph.edge_count == 1 else "dependencies"
        return [
            Text(
                f"  Dependency diagram for {graph.node_count} {objects_word} "
                f"and {graph.edge_count} {deps_word} written to:",
                style="dim",
            ),
            Text(f"  {self._written_path}", styles.FILE_PATH_STYLE),
            Text(
                "  Open it in your IDE's Markdown preview to explore the graph.",
                style="dim",
            ),
        ]

    def _is_success(self) -> bool:
        return True
