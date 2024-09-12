import logging
from typing import Callable, Generic, Type, TypeVar, get_args

from snowflake.cli.api.entities.actions import ENTITY_ACTION_ATTR, EntityAction
from snowflake.cli.api.sql_execution import SqlExecutor

logger = logging.getLogger(__name__)


T = TypeVar("T")


class EntityBase(Generic[T]):
    """
    Base class for the fully-featured entity classes.
    """

    def __init__(self, entity_model: T):
        self._entity_model = entity_model

    @classmethod
    def get_action_callable(cls, action: EntityAction) -> Callable:
        """
        Returns a generic action callable that is _not_ bound to a particular entity.
        """
        fn = next(
            (
                v
                for (k, v) in cls.__dict__.items()
                if getattr(v, ENTITY_ACTION_ATTR, None) == action
            ),
            None,
        )
        if fn is None:
            raise KeyError(
                f"No implementation for {action.key} exists on {cls.__name__}"
            )
        return fn

    @classmethod
    def get_entity_model_type(cls) -> Type[T]:
        """
        Returns the generic model class specified in each entity class.

        For example, calling ApplicationEntity.get_entity_model_type() will return the ApplicationEntityModel class.
        """
        return get_args(cls.__orig_bases__[0])[0]  # type: ignore[attr-defined]

    @classmethod
    def supports(cls, action: EntityAction) -> bool:
        """
        Checks whether this entity supports the given action.
        An entity is considered to support an action if it implements a method with the action name.
        """
        try:
            cls.get_action_callable(action)
            return True
        except KeyError:
            return False

    @classmethod
    def implements(cls, action: EntityAction, *args, **kwargs):
        """
        Registers the wrapped function against the given action for this entity type.
        """

        def wrapper(func):
            wrapped_func = action.implementation(*args, **kwargs)(func)
            setattr(cls, func.__name__, wrapped_func)
            return wrapped_func

        return wrapper


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()
