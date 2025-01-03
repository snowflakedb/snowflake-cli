import functools
from enum import Enum
from pathlib import Path
from typing import Any, Generic, List, Type, TypeVar, get_args

from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.cli_global_context import span
from snowflake.cli.api.exceptions import CycleDetectedError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import Dependency
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.cli.api.utils.graph import Graph, Node
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


class EntityActions(str, Enum):
    BUNDLE = "action_bundle"
    DEPLOY = "action_deploy"
    DROP = "action_drop"
    VALIDATE = "action_validate"
    EVENTS = "action_events"

    VERSION_LIST = "action_version_list"
    VERSION_CREATE = "action_version_create"
    VERSION_DROP = "action_version_drop"

    RELEASE_DIRECTIVE_UNSET = "action_release_directive_unset"
    RELEASE_DIRECTIVE_SET = "action_release_directive_set"
    RELEASE_DIRECTIVE_LIST = "action_release_directive_list"

    RELEASE_CHANNEL_LIST = "action_release_channel_list"
    RELEASE_CHANNEL_ADD_ACCOUNTS = "action_release_channel_add_accounts"
    RELEASE_CHANNEL_REMOVE_ACCOUNTS = "action_release_channel_remove_accounts"
    RELEASE_CHANNEL_ADD_VERSION = "action_release_channel_add_version"
    RELEASE_CHANNEL_REMOVE_VERSION = "action_release_channel_remove_version"


T = TypeVar("T")


def attach_spans_to_entity_actions(entity_name: str):
    """
    Class decorator for EntityBase subclasses to automatically wrap
    every implemented entity action method with a metrics span

    Args:
        entity_name (str): Custom name for entity type to be displayed in metrics
    """

    def decorator(cls: type[T]) -> type[T]:
        for attr_name, attr_value in vars(cls).items():
            is_entity_action = attr_name in [
                enum_member for enum_member in EntityActions
            ]

            if is_entity_action and callable(attr_value):
                attr_name_without_action_prefix = attr_name.partition("_")[2]
                span_name = f"action.{entity_name}.{attr_name_without_action_prefix}"
                action_with_span = span(span_name)(attr_value)
                setattr(cls, attr_name, action_with_span)
        return cls

    return decorator


class EntityBase(Generic[T]):
    """
    Base class for the fully-featured entity classes.
    """

    def __init__(self, entity_model: T, workspace_ctx: WorkspaceContext):
        self._entity_model = entity_model
        self._workspace_ctx = workspace_ctx

    @property
    def entity_id(self) -> str:
        return self._entity_model.entity_id  # type: ignore

    @property
    def root(self) -> Path:
        return self._workspace_ctx.project_root

    @property
    def identifier(self) -> str:
        return self.model.fqn.sql_identifier

    @property
    def fqn(self) -> FQN:
        return self._entity_model.fqn  # type: ignore[attr-defined]

    @functools.cached_property
    def _sql_executor(
        self,
    ) -> SqlExecutor:
        return get_sql_executor()

    def _execute_query(self, sql: str) -> SnowflakeCursor:
        return self._sql_executor.execute_query(sql)

    @functools.cached_property
    def _conn(self) -> SnowflakeConnection:
        return self._sql_executor._conn  # noqa

    @property
    def model(self):
        return self._entity_model

    @classmethod
    def get_entity_model_type(cls) -> Type[T]:
        """
        Returns the generic model class specified in each entity class.

        For example, calling ApplicationEntity.get_entity_model_type() will return the ApplicationEntityModel class.
        """
        return get_args(cls.__orig_bases__[0])[0]  # type: ignore[attr-defined]

    def supports(self, action: EntityActions) -> bool:
        """
        Checks whether this entity supports the given action. An entity is considered to support an action if it implements a method with the action name.
        """
        return callable(getattr(self, action, None))

    def perform(
        self, action: EntityActions, action_ctx: ActionContext, *args, **kwargs
    ):
        """
        Performs the requested action.
        """
        return getattr(self, action)(action_ctx, *args, **kwargs)

    def dependent_entities(self, action_ctx: ActionContext) -> List[Dependency]:
        """
        Returns a list of entities that this entity depends on.
        """
        graph = self._create_dependency_graph(action_ctx)
        sorted_dependecies = self._check_dependency_graph_for_cycles(graph)

        return sorted_dependecies

    def _create_dependency_graph(self, action_ctx: ActionContext) -> Graph[Dependency]:
        """
        Creates a graph for dependencies. We need the graph, instead of a simple list, because we need to check if
        calling dependencies actions in selected order is possible.
        """
        graph = Graph()
        depends_on = self._entity_model.depends_on or []  # type: ignore
        self_dependency = Dependency(id=self.model.entity_id)  # type: ignore
        resolved_nodes = set()

        graph.add(Node(key=self_dependency.entity_id, data=self_dependency))

        def _resolve_dependencies(parent_id: str, dependency: Dependency) -> None:

            if not graph.contains_node(dependency.entity_id):
                dependency_node = Node(key=dependency.entity_id, data=dependency)
                graph.add(dependency_node)

            graph.add_directed_edge(parent_id, dependency.entity_id)

            resolved_nodes.add(dependency_node.key)

            for child_dependency in action_ctx.get_entity(
                dependency.entity_id
            ).model.depends_on:
                if child_dependency.entity_id not in resolved_nodes:
                    _resolve_dependencies(dependency_node.key, child_dependency)
                else:
                    graph.add_directed_edge(
                        dependency_node.key, child_dependency.entity_id
                    )

        for dependency in depends_on:
            _resolve_dependencies(self_dependency.entity_id, dependency)

        return graph

    def _check_dependency_graph_for_cycles(
        self, graph: Graph[Dependency]
    ) -> List[Dependency]:
        """
        This function is used to check, if dependency graph have any cycles, that would make it impossible to
        deploy entities in correct order.
        If cycle is detected, it raises CycleDetectedError
        The result list, shows entities this one depends on, in order they should be called.
        Last item is removed from the result list, as it is this entity itself.
        """
        result = []

        def _on_cycle(node: Node[T]) -> None:
            raise CycleDetectedError(
                f"Cycle detected in entity dependencies: {node.key}"
            )

        def _on_visit(node: Node[T]) -> None:
            result.append(node.data)

        graph.dfs(on_cycle_action=_on_cycle, visit_action=_on_visit)

        return clear_duplicates_from_list(result)[:-1]

    def get_usage_grant_sql(self, app_role: str) -> str:
        return f"GRANT USAGE ON {self.model.type.upper()} {self.identifier} TO ROLE {app_role};"

    def get_describe_sql(self) -> str:
        return f"DESCRIBE {self.model.type.upper()} {self.identifier};"

    def get_drop_sql(self) -> str:
        return f"DROP {self.model.type.upper()} {self.identifier};"


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()


def clear_duplicates_from_list(input_list: list[Any]) -> list[Any]:
    """
    Removes duplicates from the input list and returns a new list.
    """
    seen = set()
    return [x for x in input_list if not (x in seen or seen.add(x))]  # type: ignore
