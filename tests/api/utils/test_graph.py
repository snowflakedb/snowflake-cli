from __future__ import annotations

import pytest
from snowflake.cli.api.utils.graph import Graph, Node


@pytest.fixture
def nodes() -> list[Node]:
    nodes = []
    for i in range(5):
        nodes.append(Node(key=i, data=str(i)))
    return nodes


def test_create_new_graph(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    graph.add(nodes[1])
    assert graph.get_all_nodes() == set([nodes[0], nodes[1]])


def test_add_edges(nodes: list[Node]):
    graph = Graph()
    graph.add(nodes[0])
    graph.add(nodes[1])
    graph.add_directed_edge(nodes[0].key, nodes[1].key)

    assert nodes[0].neighbors == set([nodes[1]])


def test_dfs(nodes: list[Node]):
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
