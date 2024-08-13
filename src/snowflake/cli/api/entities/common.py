from enum import Enum
from typing import Generic, TypeVar, get_args


class EntityActions(str, Enum):
    BUNDLE = "action_bundle"


T = TypeVar("T")


class EntityBase(Generic[T]):
    """
    Base class for the fully-featured entity classes.
    """

    def __init__(self, entity_model: T):
        self._entity_model = entity_model

    @classmethod
    def get_entity_model_type(cls) -> T:
        """
        Returns the generic model type specified in each entity class.
        For example:
            ApplicationEntity(EntityBase[ApplicationEntityModel]
                                                    ^
        """
        return get_args(cls.__orig_bases__[0])[0]  # type: ignore[attr-defined]

    def supports(self, action: EntityActions) -> bool:
        """
        Checks whether this entity supports the given action. An entity is considered to support an action if it implements a method with the action name.
        """
        return callable(getattr(self, action, None))
