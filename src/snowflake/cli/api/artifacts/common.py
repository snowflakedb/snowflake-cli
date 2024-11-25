from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from typing import Optional, Union

from click import ClickException


class DeployRootError(ClickException):
    """
    The deploy root was incorrectly specified.
    """

    def __init__(self, msg: str):
        super().__init__(msg)


class ArtifactError(ClickException):
    """
    Could not parse source or destination artifact.
    """

    def __init__(self, msg: str):
        super().__init__(msg)


class SourceNotFoundError(ClickException):
    """
    No match was found for the specified source in the project directory
    """

    def __init__(self, src: Union[str, Path]):
        super().__init__(f"{dedent(str(self.__doc__))}: {src}".strip())


class TooManyFilesError(ClickException):
    """
    Multiple file or directories were mapped to one output destination.
    """

    dest_path: Path

    def __init__(self, dest_path: Path):
        super().__init__(
            f"{dedent(str(self.__doc__))}\ndestination = {dest_path}".strip()
        )
        self.dest_path = dest_path


class NotInDeployRootError(ClickException):
    """
    The specified destination path is outside of the deploy root, or
    would entirely replace it. This can happen when a relative path
    with ".." is provided, or when "." is used as the destination
    (use "./" instead to copy into the deploy root).
    """

    dest_path: Union[str, Path]
    deploy_root: Path
    src_path: Optional[Union[str, Path]]

    def __init__(
        self,
        *,
        dest_path: Union[Path, str],
        deploy_root: Path,
        src_path: Optional[Union[str, Path]] = None,
    ):
        message = dedent(str(self.__doc__))
        message += f"\ndestination = {dest_path}"
        message += f"\ndeploy root = {deploy_root}"
        if src_path is not None:
            message += f"""\nsource = {src_path}"""
        super().__init__(message.strip())
        self.dest_path = dest_path
        self.deploy_root = deploy_root
        self.src_path = src_path
