from typing import Any, List

from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.exceptions import CycleDetectedError
from snowflake.cli.api.project.schemas.entities.common import (
    Dependency,
    EntityModelBase,
)
from snowflake.cli.api.utils.graph import Graph, Node


class DependencyResolver:
    """
    Base class for resolving dependencies logic.
    Any logic for resolving dependencies, calling their actions or validating them should be implemented here.
    If an entity uses it's specific logic, it should implement its own resolver, inheriting from this one
    """

    def __init__(self, model: EntityModelBase):
        self.entity_model = model
        self.dependencies: List[Dependency] = []

    def depends_on(self, action_ctx: ActionContext) -> List[Dependency]:
        """
        Returns a list of entities that this entity depends on.
        The list is sorted in order they should be called- last one depends on all the previous.
        """
        if not self.dependencies:
            graph = self._create_dependency_graph(action_ctx)
            self.dependencies = self._check_and_sort_dependencies(graph)

        return self.dependencies

    def perform_for_dep(
        self, action: EntityActions, action_ctx: ActionContext, *args, **kwargs
    ):
        """
        Method used to perform selected
        """
        for dependency in self.depends_on(action_ctx):
            entity = action_ctx.get_entity(dependency.entity_id)
            if entity.supports(action):
                getattr(entity, action.value.method_name)(*args, **kwargs)

    def _create_dependency_graph(self, action_ctx: ActionContext) -> Graph[Dependency]:
        """
        Creates a graph for dependencies. We need the graph, instead of a simple list, because we need to check if
        calling dependencies actions in selected order is possible.
        """
        graph = Graph()
        depends_on = self.entity_model.meta.depends_on if self.entity_model.meta else []  # type: ignore
        self_dependency = Dependency(id=self.entity_model.entity_id)  # type: ignore
        resolved_nodes = set()

        graph.add(Node(key=self_dependency.entity_id, data=self_dependency))

        def _resolve_dependencies(parent_id: str, dependency: Dependency) -> None:

            if not graph.contains_node(dependency.entity_id):
                dependency_node = Node(key=dependency.entity_id, data=dependency)
                graph.add(dependency_node)

            graph.add_directed_edge(parent_id, dependency.entity_id)

            resolved_nodes.add(dependency_node.key)

            for child_dependency in self._get_child_dependencies(
                dependency.entity_id, action_ctx
            ):
                if child_dependency.entity_id not in resolved_nodes:
                    _resolve_dependencies(dependency_node.key, child_dependency)
                else:
                    graph.add_directed_edge(
                        dependency_node.key, child_dependency.entity_id
                    )

        for dependency in depends_on:
            _resolve_dependencies(self_dependency.entity_id, dependency)

        return graph

    @staticmethod
    def _check_and_sort_dependencies(
        graph: Graph[Dependency],
    ) -> List[Dependency]:
        """
        This function is used to check and organize the dependency list.
        The check has two stages:
         * Cycle detection in dependency
         * Clearing duplicate

        In the first stage, if cycle is detected, it raises CycleDetectedError with node causing it specified.
        The result list, shows entities this one depends on, in order they should be called.
        Duplicates are removed in a way, that preserves earliest possible call.
        Last item is removed from the result list, as it is this entity itself.
        """
        result = []

        def _on_cycle(node: Node[Dependency]) -> None:
            raise CycleDetectedError(
                f"Cycle detected in entity dependencies: {node.key}"
            )

        def _on_visit(node: Node[Dependency]) -> None:
            result.append(node.data)

        graph.dfs(on_cycle_action=_on_cycle, visit_action=_on_visit)

        return clear_duplicates_from_list(result)[:-1]

    @staticmethod
    def _get_child_dependencies(
        dependency_id: str, action_ctx: ActionContext
    ) -> List[Dependency]:
        child_dependency = action_ctx.get_entity(dependency_id)

        if not child_dependency:
            raise ValueError(f"Entity with id {dependency_id} not found in project")

        if child_dependency.model.meta:
            return child_dependency.model.meta.depends_on

        else:
            return []


def clear_duplicates_from_list(input_list: list[Any]) -> list[Any]:
    """
    Removes duplicates from the input list, preserving the first occurrence.
    """
    seen = set()
    return [x for x in input_list if not (x in seen or seen.add(x))]  # type: ignore
