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

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, Generic, Set, TypeVar

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

    def contains_node(self, key: str) -> bool:
        return self.__contains__(key)

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
        visit_action: Callable[[Node[T]], None],
        on_cycle_action: Callable[[Node[T]], None],
    ) -> None:
        if nodes_status.get(node.key) == VisitStatus.VISITED:
            return

        nodes_status[node.key] = VisitStatus.VISITING
        for neighbor_node in node.neighbors:
            if nodes_status.get(neighbor_node.key) == VisitStatus.VISITING:
                on_cycle_action(node)
            else:
                Graph._dfs_visit(
                    nodes_status, neighbor_node, visit_action, on_cycle_action
                )

        visit_action(node)

        nodes_status[node.key] = VisitStatus.VISITED

    def dfs(
        self,
        visit_action: Callable[[Node[T]], None] = lambda node: None,
        on_cycle_action: Callable[[Node[T]], None] = lambda node: None,
    ) -> None:
        nodes_status: dict[str, VisitStatus] = {}
        for node in self._graph_nodes_map.values():
            Graph._dfs_visit(nodes_status, node, visit_action, on_cycle_action)

    def layers(
        self, starting_node_id: str
    ):  # TODO: probably can be removed as it is not used
        """
        Creates a list of graph layers, relative to the starting node
        """
        result: Dict[int, Set] = {}

        def _add_layer(node: Node[T], layer_number: int):
            if not result.get(layer_number):
                result[layer_number] = set()
            result[layer_number].add(node)
            for neighbor in node.neighbors:
                _add_layer(neighbor, layer_number + 1)

        _add_layer(self.get(starting_node_id), 0)

        return [tuple(result[key]) for key in sorted(result.keys())]

    def __contains__(self, key: str) -> bool:
        return key in self._graph_nodes_map
