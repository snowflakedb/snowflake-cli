from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar

T = TypeVar("T")


class VisitStatus(Enum):
    VISITING = 1
    VISITED = 2


@dataclass
class Node(Generic[T]):
    key: str
    data: T
    neighbors: set[Node[T]] = field(default_factory=set)

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

    def add(self, node: Node[T]) -> None:
        if node.key in self._graph_nodes_map:
            raise KeyError(f"Node key {node.key} already exists")
        self._graph_nodes_map[node.key] = node

    def add_directed_edge(self, from_node_key: str, to_node_key: str) -> None:
        from_node = self.get(from_node_key)
        to_node = self.get(to_node_key)
        from_node.neighbors.add(to_node)

    @staticmethod
    def _dfs_visit(
        nodes_status: dict[str, VisitStatus],
        node: Node[T],
        visit_action,
        on_cycle_action,
    ) -> None:
        if nodes_status.get(node.key) == VisitStatus.VISITED:
            return

        nodes_status[node.key] = VisitStatus.VISITING
        for neighbor_node in node.neighbors:
            if nodes_status.get(neighbor_node.key) == VisitStatus.VISITING:
                on_cycle_action()
            else:
                Graph._dfs_visit(
                    nodes_status, neighbor_node, visit_action, on_cycle_action
                )

        visit_action(node)

        nodes_status[node.key] = VisitStatus.VISITED

    def dfs(self, visit_action=lambda node: None, on_cycle_action=lambda: None) -> None:
        nodes_status: dict[str, VisitStatus] = {}
        for node in self._graph_nodes_map.values():
            Graph._dfs_visit(nodes_status, node, visit_action, on_cycle_action)

    def __contains__(self, key: str) -> bool:
        return key in self._graph_nodes_map
