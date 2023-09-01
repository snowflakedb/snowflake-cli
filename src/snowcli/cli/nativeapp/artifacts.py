import os
import shutil
from pathlib import Path
from typing import List, Union
from dataclasses import dataclass


class ArtifactError(Exception):
    pass


class InvalidArtifactError(ArtifactError):
    pass


class GlobMatchedNothingError(ArtifactError):
    """
    No files were found that matched the provided glob pattern.
    """

    pass


class SourceNotFoundError(ArtifactError):
    """
    The specifically-referenced source file or directory was not found
    in the project directory.
    """

    path: Path

    def __init__(self, path: Path):
        super().__init__()
        self.path = path

    def __str__(self):
        return self.__doc__ + f"\path = {self.path}"


class TooManyFilesError(ArtifactError):
    """
    Multiple files were mapped to one output file.
    """

    dest_path: Path

    def __init__(self, dest_path: Path):
        super().__init__()
        self.dest_path = dest_path

    def __str__(self):
        return self.__doc__ + f"\ndest_path = {self.dest_path}"


class OutsideDeployRootError(ArtifactError):
    """
    The specified path is outside of the deploy root.
    This can happen when a relative path with ".." is provided.
    """

    dest_path: Path
    deploy_root: Path

    def __init__(self, dest_path: Path, deploy_root: Path):
        super().__init__()
        self.dest_path = dest_path
        self.deploy_root = deploy_root

    def __str__(self):
        return (
            self.__doc__
            + f"\ndest_path = {self.dest_path}"
            + f"\ndeploy_root = {self.deploy_root}"
        )


@dataclass
class SrcDestPair:
    """
    A pair of paths, relative to some context.
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


def translate_artifact(item: Union[dict, str]) -> SrcDestPair:
    if isinstance(item, dict):
        if "dest" in item:
            return SrcDestPair(item["src"], item["dest"])
        else:
            return SrcDestPair(item["src"], item["src"])

    elif isinstance(item, str):
        return SrcDestPair(item, item)

    else:
        # XXX: validation should have caught this
        raise InvalidArtifactError(item)


def build_bundle(project_root: Path, deploy_root: Path, artifacts: List[SrcDestPair]):
    """
    Prepares a local folder (deploy_root) with configured app artifacts.
    This folder can then be uploaded to a stage.
    """
    deploy_root = deploy_root.resolve()
    if deploy_root.exists() and not deploy_root.is_dir():
        raise ValueError(f"Deploy root {deploy_root} exists, but is not a directory!")

    for artifact in artifacts:
        # build the list of source files
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

        # make sure we are only modifying files / directories inside deploy_root
        dest_path = Path(deploy_root, artifact.dest).resolve()
        if deploy_root != dest_path and deploy_root not in dest_path.parents:
            raise OutsideDeployRootError(dest_path, deploy_root)

        if dest_path.is_file():
            dest_path.unlink()

        if specifies_directory(artifact.dest):
            # copy all files as children of the given destination path
            for source_path in source_paths:
                symlink_or_copy(source_path, dest_path / source_path.name)
        else:
            # copy a single file as the given destination path
            if len(source_paths) == 1:
                symlink_or_copy(source_path, dest_path)
            else:
                # refuse to map multiple source files to one destination (undefined behaviour)
                raise TooManyFilesError(dest_path)
