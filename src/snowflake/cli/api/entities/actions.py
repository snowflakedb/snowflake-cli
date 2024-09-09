import inspect
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Annotated, Callable, List, Optional


@dataclass
class HelpText:
    value: str


class ParameterDeclarations:
    decls: List[str]

    def __init__(self, *decls: str):
        self.decls = list(decls)


class EntityAction:
    key: str
    _declaration: inspect.Signature

    def __init__(self, key: str):
        self.key = key

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

    def declaration(self, func: Callable):
        """
        Inspects the function signature of the wrapped function to define required
        and default parameters for an entity action. The function is never called.
        """
        self._declaration = inspect.signature(func)

    def implementation(self):
        """
        Validates the wrapped function's signature against the stored declaration
        signature.
        """
        if not self._declaration:
            raise RuntimeError(
                f"{str(self)} has no base declaration; cannot register implementation."
            )

        def wrapper(func):
            func.entity_action = self
            return func

        return wrapper


class EntityActions(EntityAction, Enum):
    BUNDLE = ("bundle",)
    DEPLOY = ("deploy",)
    DROP = ("drop",)
    VALIDATE = ("validate",)
    VERSION_CREATE = (("version_create",),)
    VERSION_DROP = (("version_drop",),)
    VERSION_LIST = ("version_list",)


@EntityActions.DEPLOY.declaration
def deploy(
    prune: Annotated[
        Optional[bool],
        HelpText(
            "Whether to delete specified files from the stage if they don't exist locally. If set, the command deletes files that exist in the stage, but not in the local filesystem. This option cannot be used when paths are specified."
        ),
    ] = None,
    recursive: Annotated[
        Optional[bool],
        ParameterDeclarations("--recursive/--no-recursive", "-r"),
        HelpText(
            "Whether to traverse and deploy files from subdirectories. If set, the command deploys all files and subdirectories; otherwise, only files in the current directory are deployed."
        ),
    ] = None,
    paths: Annotated[
        Optional[List[Path]],
        HelpText(
            dedent(
                f"""
            Paths, relative to the the project root, of files or directories you want to upload to a stage. If a file is
            specified, it must match one of the artifacts src pattern entries in snowflake.yml. If a directory is
            specified, it will be searched for subfolders or files to deploy based on artifacts src pattern entries. If
            unspecified, the command syncs all local changes to the stage."""
            ).strip()
        ),
    ] = None,
):
    """
    Generic help text for deploy.
    """
    raise NotImplementedError()
