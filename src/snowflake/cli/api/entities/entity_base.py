import logging
from typing import Callable, Generic, Type, TypeVar, get_args

from snowflake.cli.api.entities.actions import EntityAction

logger = logging.getLogger(__name__)


T = TypeVar("T")


ENTITY_ACTION_ATTR = "_entity_action"


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
        fn = cls.__dict__.get(action.key, None)

        if fn is None:
            raise KeyError(f"{action.key} does not exist on {cls.__name__}")

        if not callable(fn) or not hasattr(fn, ENTITY_ACTION_ATTR):
            # expected an action callable; got something else
            raise KeyError(
                f"{action.key} exists on {cls.__name__} but is not an action implementation"
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
            cls.get_action_callable(action.key)
            return True
        except KeyError:
            return False

    @classmethod
    def implements(cls, action: EntityAction):
        """
        Registers the wrapped function against the given action for this entity type.
        """

        def wrapper(func):
            # TODO: implement rules to ensure that function fits the expected shape of arguments for the action
            setattr(func, ENTITY_ACTION_ATTR, action)
            return func

        return wrapper
