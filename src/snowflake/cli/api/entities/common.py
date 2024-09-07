from enum import Enum
from typing import Callable, Generic, Type, TypeVar, get_args

from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.sql_execution import SqlExecutor


class EntityActions(str, Enum):
    BUNDLE = "bundle"
    DEPLOY = "deploy"
    DROP = "drop"
    VALIDATE = "validate"
    VERSION_CREATE = "version_create"
    VERSION_DROP = "version_drop"
    VERSION_LIST = "version_list"

    @property
    def verb(self) -> str:
        return self.value.replace("_", " ")

    @property
    def attr_name(self) -> str:
        return f"action_{self.value}"

    @property
    def command_path(self) -> list[str]:
        return self.value.split("_")


T = TypeVar("T")


class EntityBase(Generic[T]):
    """
    Base class for the fully-featured entity classes.
    """

    def __init__(self, entity_model: T):
        self._entity_model = entity_model

    @classmethod
    def get_entity_model_type(cls) -> Type[T]:
        """
        Returns the generic model class specified in each entity class.

        For example, calling ApplicationEntity.get_entity_model_type() will return the ApplicationEntityModel class.
        """
        return get_args(cls.__orig_bases__[0])[0]  # type: ignore[attr-defined]

    @classmethod
    def supports(cls, action: EntityActions) -> bool:
        """
        Checks whether this entity supports the given action.
        An entity is considered to support an action if it implements a method with the action name.
        """
        return callable(getattr(cls, action.attr_name, None))

    @classmethod
    def get_action_callable(cls, action: EntityActions) -> Callable:
        """
        Returns a generic action callable that is _not_ bound to a particular entity.
        """
        attr = getattr(cls, action.attr_name)  # raises KeyError
        if not callable(attr):
            raise ValueError(f"{action} method exists but is not callable")
        return attr

    def perform(
        self, action: EntityActions, action_ctx: ActionContext, *args, **kwargs
    ):
        """Performs the requested action."""
        return getattr(self, action.attr_name)(action_ctx, *args, **kwargs)


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()
