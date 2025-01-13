import functools
from enum import Enum
from pathlib import Path
from typing import Generic, Type, TypeVar, get_args

from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.cli_global_context import span
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


class EntityActions(str, Enum):
    BUNDLE = "action_bundle"
    DEPLOY = "action_deploy"
    DROP = "action_drop"
    VALIDATE = "action_validate"
    EVENTS = "action_events"
    DIFF = "action_diff"

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

    PUBLISH = "action_publish"


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

    def get_usage_grant_sql(self, app_role: str) -> str:
        return f"GRANT USAGE ON {self.model.type.upper()} {self.identifier} TO ROLE {app_role};"

    def get_describe_sql(self) -> str:
        return f"DESCRIBE {self.model.type.upper()} {self.identifier};"

    def get_drop_sql(self) -> str:
        return f"DROP {self.model.type.upper()} {self.identifier};"


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()
