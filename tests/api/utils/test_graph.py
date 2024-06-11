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

import pytest
from snowflake.cli.api.utils.graph import Graph, Node


@pytest.fixture
def nodes() -> list[Node]:
    nodes = []
    for i in range(5):
        nodes.append(Node(key=i, data=str(i)))
    return nodes


def test_graph_create_and_get(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    graph.add(nodes[1])
    assert graph.get(nodes[0].key) == nodes[0]
    assert graph.get(nodes[1].key) == nodes[1]
    assert graph.get_all_nodes() == set([nodes[0], nodes[1]])


def test_graph_get_non_existing_node(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    with pytest.raises(KeyError):
        graph.get(nodes[1].key)


def test_graph_add_existing_node(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    with pytest.raises(KeyError):
        graph.add(nodes[0])


def test_add_edges(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    graph.add(nodes[1])
    graph.add_directed_edge(nodes[0].key, nodes[1].key)

    assert nodes[0].neighbors == set([nodes[1]])


def test_add_already_existing_edges(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    graph.add(nodes[1])
    graph.add_directed_edge(nodes[0].key, nodes[1].key)
    graph.add_directed_edge(nodes[0].key, nodes[1].key)

    assert nodes[0].neighbors == set([nodes[1]])


def test_add_edges_non_existing_node(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    graph.add(nodes[1])
    with pytest.raises(KeyError):
        graph.add_directed_edge(nodes[0].key, nodes[2].key)


def test_graph_dfs(nodes: list[Node]):
    graph = Graph()
    for i in range(5):
        graph.add(nodes[i])

    graph.add_directed_edge(nodes[0].key, nodes[1].key)
    graph.add_directed_edge(nodes[1].key, nodes[2].key)

    graph.add_directed_edge(nodes[0].key, nodes[3].key)
    graph.add_directed_edge(nodes[3].key, nodes[4].key)

    visits: list[Node] = []

    def track_visits_order(node: Node):
        visits.append(node)

    graph.dfs(visit_action=track_visits_order)

    assert visits == [nodes[4], nodes[3], nodes[2], nodes[1], nodes[0]] or visits == [
        nodes[2],
        nodes[1],
        nodes[4],
        nodes[3],
        nodes[0],
    ]


def test_graph_dfs_with_cycle(nodes: list[Node]):
    graph = Graph()
    for i in range(5):
        graph.add(nodes[i])

    graph.add_directed_edge(nodes[0].key, nodes[1].key)
    graph.add_directed_edge(nodes[0].key, nodes[2].key)
    graph.add_directed_edge(nodes[2].key, nodes[3].key)
    graph.add_directed_edge(nodes[3].key, nodes[4].key)
    graph.add_directed_edge(nodes[4].key, nodes[0].key)

    cycles_detected = {"count": 0}

    def cycle_detected_action(node):
        cycles_detected["count"] += 1

    graph.dfs(on_cycle_action=cycle_detected_action)

    assert cycles_detected["count"] == 1
