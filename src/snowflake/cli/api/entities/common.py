from enum import Enum
from typing import Generic, TypeVar


class EntityActions(str, Enum):
    BUNDLE = "bundle"


TEntityModel = TypeVar("TEntityModel")


class EntityBase(Generic[TEntityModel]):
    """
    Base class for the fully-featured entity classes.
    """

    def __init__(self, entity_model: TEntityModel):
        self._entity_model = entity_model

    def supports(self, action: EntityActions) -> bool:
        """
        Checks whether this entity supports the given action. An entity is considered to support an action if it implements a method with the action name.
        """
        return callable(getattr(self, action, None))
