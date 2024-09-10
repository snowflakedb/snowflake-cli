import inspect
from enum import Enum
from typing import Callable, List

from .deploy import deploy_signature


class EntityAction:
    key: str
    delegation_actions: List[str]
    declaration: inspect.Signature

    def __init__(
        self, key: str, declaration: Callable, delegation_actions: List[str] = []
    ):
        self.key = key
        self.declaration = inspect.signature(declaration)
        self.delegation_actions = delegation_actions

    def __str__(self) -> str:
        return f"EntityAction[{self.key}]"

    # @functools.cached_property
    # def params_map(self) -> Dict[str, ActionParameter]:
    #     return {param.name: param for param in (self.params or [])}

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
            sig = inspect.signature(func)

            # RULES
            # 1. For annotations with the same name, impl. must have same type or narrower
            # 2. If both have same optional arg, copy default over impl. sig if no default given in impl.
            # 3.

            func.entity_action = self
            return func

        return wrapper


class EntityActions(EntityAction, Enum):
    BUNDLE = ("bundle",)
    DIFF = ("diff",)
    VALIDATE = ("validate",)
    BUILD = ("build",)

    CREATE = ("create",)
    DEPLOY = ("deploy", deploy_signature, ["deploy"])
    DROP = ("drop",)  # N.B. no recursive drop

    OPEN = ("open",)
    URL = (("url"),)
    DESCRIBE = ("describe",)
    EVENTS = ("events",)

    VERSION_CREATE = ("version_create",)
    VERSION_DROP = ("version_drop",)
    VERSION_LIST = ("version_list",)
