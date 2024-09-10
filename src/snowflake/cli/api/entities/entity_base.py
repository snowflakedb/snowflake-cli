import logging
from typing import Callable, Dict, Generic, Type, TypeVar, get_args

from snowflake.cli.api.entities.actions import EntityAction

logger = logging.getLogger(__name__)


T = TypeVar("T")


class EntityBase(Generic[T]):
    """
    Base class for the fully-featured entity classes.
    """

    _entity_actions_attrs: Dict[str, EntityAction]

    class __metaclass__(type):  # noqa: N801
        def __new__(cls, name, bases, attr_dict):
            """
            Discover @EntityAction.impl() annotations.
            """
            kls = type.__new__(cls, name, bases, attr_dict)

            kls._entity_actions_attrs = {}  # noqa: SLF001
            for (key, func) in attr_dict.items():
                try:
                    action: EntityAction = getattr(func, "entity_action", None)
                    logger.warning(
                        "Found entity action %s (%s) on %s", key, action, name
                    )
                    kls._entity_actions_attrs[action.key] = key  # noqa: SLF001

                except AttributeError:
                    pass

            return kls

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
    def supports(cls, action: EntityAction) -> bool:
        """
        Checks whether this entity supports the given action.
        An entity is considered to support an action if it implements a method with the action name.
        """
        return action.key in cls._entity_actions_attrs

    @classmethod
    def get_action_callable(cls, action: EntityAction) -> Callable:
        """
        Returns a generic action callable that is _not_ bound to a particular entity.
        """
        try:
            return cls._entity_actions_attrs[action.key]
        except KeyError:
            raise ValueError(
                f"Entity {cls.__name__} does not implement action {action}"
            )
