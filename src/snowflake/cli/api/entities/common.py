from enum import Enum
from typing import Generic, Type, TypeVar, get_args

from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.sql_execution import SqlExecutor


class EntityActions(str, Enum):
    BUNDLE = "action_bundle"
    DEPLOY = "action_deploy"
    DROP = "action_drop"
    VALIDATE = "action_validate"
    EVENTS = "action_events"

    VERSION_LIST = "action_version_list"
    VERSION_CREATE = "action_version_create"
    VERSION_DROP = "action_version_drop"


T = TypeVar("T")


class EntityBaseMetaclass(type):
    def __new__(mcs, name, bases, attrs):  # noqa: N804
        cls = super().__new__(mcs, name, bases, attrs)
        generic_bases = attrs.get("__orig_bases__", [])
        if not generic_bases:
            # Subclass is not generic
            return cls

        target_model_class = get_args(generic_bases[0])[0]  # type: ignore[attr-defined]
        if target_model_class is T:
            # Generic parameter is not filled in
            return cls

        target_entity_class = getattr(target_model_class, "_entity_class", None)
        if target_entity_class is not None:
            raise ValueError(
                f"Entity model class {target_model_class} is already "
                f"associated with entity class {target_entity_class}, "
                f"cannot associate with {cls}"
            )

        setattr(target_model_class, "_entity_class", cls)
        return cls


class EntityBase(Generic[T], metaclass=EntityBaseMetaclass):
    """
    Base class for the fully-featured entity classes.
    """

    def __init__(self, entity_model: T, workspace_ctx: WorkspaceContext):
        self._entity_model = entity_model
        self._workspace_ctx = workspace_ctx

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


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()
