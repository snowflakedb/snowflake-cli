from enum import Enum
from typing import Callable, Optional

ENTITY_ACTION_ATTR = "_entity_action"


class EntityAction:
    key: str
    declaration: Optional[Callable]

    def __init__(
        self,
        key: str,
        declaration: Optional[Callable],
    ):
        self.key = key
        self.declaration = declaration

    def __str__(self) -> str:
        return f"EntityAction[{self.key}]"

    @property
    def verb(self) -> str:
        return self.key.replace("_", " ")

    @property
    def command_path(self) -> list[str]:
        return self.key.split("_")

    def implementation(self):
        """
        Registers the wrapped function against this action for the given entity type.
        """

        def wrapper(func):
            # TODO: implement rules to ensure that function fits the expected shape of arguments for the action
            setattr(func, ENTITY_ACTION_ATTR, self)
            return func

        return wrapper


class EntityActions(EntityAction, Enum):
    BUNDLE = ("bundle",)
    DIFF = ("diff",)
    VALIDATE = ("validate",)
    BUILD = ("build",)

    CREATE = ("create",)
    DEPLOY = ("deploy",)
    DROP = ("drop",)

    OPEN = ("open",)
    URL = ("url",)
    DESCRIBE = ("describe",)
    EVENTS = ("events",)

    VERSION_CREATE = ("version_create",)
    VERSION_DROP = ("version_drop",)
    VERSION_LIST = ("version_list",)
