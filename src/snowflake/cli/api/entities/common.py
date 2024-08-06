from enum import Enum


class EntityActions(str, Enum):
    BUNDLE = "bundle"


class EntityBase:
    """
    Base class for the fully-featured entity classes.
    """

    def supports(self, action: EntityActions) -> bool:
        return callable(getattr(self, action, None))
