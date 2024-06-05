from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, Optional, TypeVar

from click import ClickException

T = TypeVar("T")


@dataclass
class Node(Generic[T]):
    key: str
    data: T
    neighbors: set[Node[T]] = field(default_factory=set)
    status: Optional[str] = None

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other.key


class Graph(Generic[T]):
    def __init__(self):
        self._graph_nodes_map: dict[str, Node[T]] = {}

    def get(self, key: str) -> Node[T]:
        if key in self._graph_nodes_map:
            return self._graph_nodes_map[key]
        raise KeyError(f"Node with key {key} not found")

    def get_all_nodes(self) -> set[Node[T]]:
        return set(self._graph_nodes_map.values())

    def add(self, node: Node[T]):
        if node.key in self._graph_nodes_map:
            raise KeyError(f"Node key {node.key} already exists")
        self._graph_nodes_map[node.key] = node

    def add_directed_edge(self, from_node_key: str, to_node_key: str):
        from_node = self.get(from_node_key)
        to_node = self.get(to_node_key)
        from_node.neighbors.add(to_node)

    def _dfs_visit(self, node: Node[T], visit_action):
        if node.status == "VISITED":
            return

        node.status = "VISITING"
        for neighbour_node in node.neighbors:
            if neighbour_node.status == "VISITING":
                raise ClickException("Cycle detected")
            self._dfs_visit(neighbour_node, visit_action)

        visit_action(node)

        node.status = "VISITED"

    def dfs(self, visit_action=lambda node: None):
        for node in self._graph_nodes_map.values():
            node.status = "NOT_VISITED"
        for node in self._graph_nodes_map.values():
            self._dfs_visit(node, visit_action)

    def __contains__(self, key):
        return key in self._graph_nodes_map
