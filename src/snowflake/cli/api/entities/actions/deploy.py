from pathlib import Path
from textwrap import dedent
from typing import Annotated, List, Optional

from .lib import HelpText, ParameterDeclarations


def deploy_signature(
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
