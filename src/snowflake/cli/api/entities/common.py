from enum import Enum


class EntityActions(str, Enum):
    BUNDLE = "bundle"


class EntityBase:
    """
    Base class for the fully-featured entity classes.
    """

    def supports(self, action: EntityActions) -> bool:
        """
        Checks whether this entity supports the given action. An entity is considered to support an action if it implements a method with the action name.
        """
        return callable(getattr(self, action, None))
