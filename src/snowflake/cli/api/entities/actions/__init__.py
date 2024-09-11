from enum import Enum
from typing import Callable, Optional


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
        Validates the wrapped function's signature against the stored declaration
        signature, and registers it as the implementation for this action (requires
        EntityBase class __metaclass__).
        """

        def wrapper(func):
            # TODO: implement RULES when we have self.definition:
            # 1. For annotations with the same name, impl. must have same type or narrower
            # 2. If both have same optional arg, copy default over impl. sig if no default given in impl.
            # sig = inspect.signature(func)

            func.entity_action = self
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
