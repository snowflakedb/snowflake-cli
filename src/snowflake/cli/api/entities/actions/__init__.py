from enum import Enum

ENTITY_ACTION_ATTR = "_entity_action"


class EntityAction:
    key: str

    def __init__(
        self,
        key: str,
    ):
        self.key = key

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
        TODO: implement rules to ensure that function fits the expected shape of arguments for the action
        """

        def wrapper(func):
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
