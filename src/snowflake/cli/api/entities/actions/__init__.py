import inspect
from enum import Enum
from typing import Callable, List, Optional

from snowflake.cli._plugins.workspace.action_context import ActionContext

from .deploy import deploy_signature


class EntityAction:
    key: str
    declaration: Optional[Callable]
    delegation_action_allowlist: List[str]

    def __init__(
        self,
        key: str,
        declaration: Optional[Callable],
        delegation_action_allowlist: List[str] = [],
    ):
        self.key = key
        self.declaration = declaration
        self.delegation_action_allowlist = delegation_action_allowlist

    def __str__(self) -> str:
        return f"EntityAction[{self.key}]"

    @property
    def verb(self) -> str:
        return self.key.replace("_", " ")

    @property
    def command_path(self) -> list[str]:
        return self.key.split("_")

    def execute(self, ctx: ActionContext, *args, **kwargs):
        inner_ctx = ctx.clone()
        # TODO: what will we need to pass in for Entity type / concrete entity?
        # TODO: implement the rules from below

    def implementation(self):
        """
        Validates the wrapped function's signature against the stored declaration
        signature, and registers it as the implementation for this action (requires
        EntityBase class __metaclass__).
        """

        def wrapper(func):
            sig = inspect.signature(func)

            # TODO: implement RULES
            # 0. If there's no base definition, we skip all other rules
            # 1. For annotations with the same name, impl. must have same type or narrower
            # 2. If both have same optional arg, copy default over impl. sig if no default given in impl.

            func.entity_action = self
            return func

        return wrapper


class EntityActions(EntityAction, Enum):
    BUNDLE = ("bundle",)
    DIFF = ("diff",)
    VALIDATE = ("validate",)
    BUILD = ("build", None, ["build"])

    CREATE = ("create", None, ["create"])
    DEPLOY = ("deploy", deploy_signature, ["create", "deploy"])
    DROP = ("drop",)  # N.B. no recursive drop

    OPEN = ("open",)
    URL = ("url",)
    DESCRIBE = ("describe",)
    EVENTS = ("events",)

    VERSION_CREATE = ("version_create",)
    VERSION_DROP = ("version_drop",)
    VERSION_LIST = ("version_list",)
