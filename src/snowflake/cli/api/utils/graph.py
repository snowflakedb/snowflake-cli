from __future__ import annotations

from dataclasses import dataclass, field


class Graph:
    @dataclass
    class Node:
        key: str
        status: str | None = None
        dependencies: set[Graph.Node] = field(default_factory=set)

        def __eq__(self, other):
            return self.key == other.key

        def __hash__(self):
            return hash(self.key)

    def __init__(self):
        self._graph_nodes_map: dict[str, Graph.Node] = {}

    def get(self, key: str):
        if key in self._graph_nodes_map:
            return self._graph_nodes_map.get(key)
        raise KeyError(f"Node with key {key} not found")

    def get_all_nodes(self):
        return self._graph_nodes_map.values()

    def add(self, node: Node):
        if node.key in self._graph_nodes_map:
            raise KeyError(f"Node key {node.key} already exists")
        self._graph_nodes_map[node.key] = node

    def add_dependency(self, key1: str, key2: str):
        node1 = self.get(key1)
        node2 = self.get(key2)
        node1.dependencies.add(node2)

    def _dfs_visit(self, node: Node, visit_action):
        if node.status == "VISITED":
            return

        node.status = "VISITING"
        for neighbour_node in node.dependencies:
            if neighbour_node.status == "VISITING":
                raise RecursionError("Cycle detected")
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
