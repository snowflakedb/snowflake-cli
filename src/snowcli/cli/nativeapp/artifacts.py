import os
import shutil
from click.exceptions import ClickException
from pathlib import Path
from typing import List, Union
from dataclasses import dataclass


class ArtifactError(ClickException):
    """
    Could not parse source or destination artifact.
    """

    def __init__(self, msg: str):
        super().__init__(msg)


class GlobMatchedNothingError(ClickException):
    """
    No files were found that matched the provided glob pattern.
    """

    def __init__(self, src: str):
        super().__init__(f"{self.__doc__}: {src}")


class SourceNotFoundError(ClickException):
    """
    The specifically-referenced source file or directory was not found
    in the project directory.
    """

    path: Path

    def __init__(self, path: Path):
        super().__init__(f"{self.__doc__}\npath = {path}")
        self.path = path


class TooManyFilesError(ClickException):
    """
    Multiple files were mapped to one output file.
    """

    dest_path: Path

    def __init__(self, dest_path: Path):
        super().__init__(f"self.__doc__\ndest_path = {dest_path}")
        self.dest_path = dest_path


class OutsideDeployRootError(ClickException):
    """
    The specified path is outside of the deploy root.
    This can happen when a relative path with ".." is provided.
    """

    dest_path: Path
    deploy_root: Path

    def __init__(self, dest_path: Path, deploy_root: Path):
        super().__init__(
            f"""
            self.__doc__
            \ndest_path = {dest_path}
            \ndeploy_root = {deploy_root}
            """
        )
        self.dest_path = dest_path
        self.deploy_root = deploy_root


@dataclass
class ArtifactMapping:
    """
    Used to keep track of equivalent paths / globs so we can copy
    artifacts from the project folder to the deploy root.
    """

    src: str
    dest: str


def is_glob(s: str) -> bool:
    return "*" in s


def specifies_directory(s: str) -> bool:
    """
    Does the path (as seen from the project definition) refer to
    a directory? For destination paths, we enforce the usage of a
    trailing slash (i.e. \ for windows; / for others).

    This means that to put a file in the root of the stage, we need
    to specify "./" as its destination, or omit it (but only if the
    file already lives in the project root).
    """
    return s.endswith(os.sep)


def symlink_or_copy(src: Path, dst: Path, makedirs=True) -> None:
    """
    Tries to create a symlink to src at dst; failing that (i.e. in Windows
    without Administrator / Developer Mode) copies the file from src to dst instead.
    If makedirs is True, the directory hierarchy above dst is created if any
    of those directories do not exist.
    """
    if makedirs:
        dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.symlink(src, dst)
    except OSError:
        shutil.copy(src, dst)


def translate_artifact(item: Union[dict, str]) -> ArtifactMapping:
    if isinstance(item, dict):
        return ArtifactMapping(item["src"], item.get("dest", item["src"]))

    elif isinstance(item, str):
        return ArtifactMapping(item, item)

    # XXX: validation should have caught this
    raise ArtifactError("Item is not a valid artifact!")


def get_source_paths(artifact: ArtifactMapping, project_root: Path) -> List[Path]:
    """
    Expands globs, ensuring at least one file exists that matches artifact.src.
    Returns a list of paths that resolve to actual files in the project root dir structure.
    """
    source_paths: List[Path]

    if is_glob(artifact.src):
        source_paths = list(project_root.glob(artifact.src))
        if not source_paths:
            raise GlobMatchedNothingError(artifact.src)
    else:
        source_path = Path(project_root, artifact.src)
        source_paths = [source_path]
        if not source_path.exists():
            raise SourceNotFoundError(source_path)

    return source_paths


def build_bundle(
    project_root: Path, deploy_root: Path, artifacts: List[ArtifactMapping]
):
    """
    Prepares a local folder (deploy_root) with configured app artifacts.
    This folder can then be uploaded to a stage.
    """
    resolved_root = deploy_root.resolve()
    if resolved_root.exists() and not resolved_root.is_dir():
        raise ValueError(f"Deploy root {resolved_root} exists, but is not a directory!")

    for artifact in artifacts:
        # make sure we are only modifying files / directories inside the deploy root
        dest_path = Path(resolved_root, artifact.dest).resolve()
        if resolved_root != dest_path and resolved_root not in dest_path.parents:
            raise OutsideDeployRootError(dest_path, resolved_root)

        if dest_path.is_file():
            dest_path.unlink()

        source_paths = get_source_paths(artifact, project_root)

        if specifies_directory(artifact.dest):
            # copy all files as children of the given destination path
            for source_path in source_paths:
                symlink_or_copy(source_path, dest_path / source_path.name)
        else:
            if len(source_paths) == 1:
                # copy a single file as the given destination path
                symlink_or_copy(source_paths[0], dest_path)
            else:
                # refuse to map multiple source files to one destination (undefined behaviour)
                raise TooManyFilesError(dest_path)
