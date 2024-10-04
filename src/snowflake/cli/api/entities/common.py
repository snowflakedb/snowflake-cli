from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from typing import Generic, Type, TypeVar, get_args

from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.connector.errors import ProgrammingError


class EntityActions(str, Enum):
    BUNDLE = "action_bundle"
    DEPLOY = "action_deploy"
    DROP = "action_drop"
    VALIDATE = "action_validate"

    VERSION_LIST = "action_version_list"
    VERSION_CREATE = "action_version_create"
    VERSION_DROP = "action_version_drop"


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


class SFService(ABC):
    # Should the error handling be done here or in the implementing layer hmm
    @abstractmethod
    def use_object(self, object_type: ObjectType, name: str):
        pass

    @abstractmethod
    def get_current_role(self):
        pass

    @abstractmethod
    def switch_to_role(self, role: str):
        pass

    @abstractmethod
    def get_existing_pkg(self, pkg_name: str):
        pass


class SqlService(SFService):
    def __init__(self):
        self.sql_executor = SqlExecutor()

    def use_object(self, object_type: ObjectType, name: str):
        try:
            self.sql_executor.execute_query(f"use {object_type.value.sf_name} {name}")
        # TODO: replace with CouldNotUseObject error
        except ProgrammingError:
            raise ProgrammingError(
                f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
            )

    def get_current_role(self) -> str:
        # pj-question: do we just propagate sql errors or do we add context? how do I know which is which
        return self.sql_executor.execute_query(f"select current_role()").fetchone()[0]

    @contextmanager
    def switch_to_role(self, role: str):
        prev_role = self.get_current_role()
        is_different_role = role.lower() != prev_role.lower()
        if is_different_role:
            self.sql_executor.log_debug(f"Assuming different role: {role}")
            self.use_object(ObjectType.ROLE, role)
        try:
            yield
        finally:
            if is_different_role:
                self.use_object(ObjectType.ROLE, prev_role)
