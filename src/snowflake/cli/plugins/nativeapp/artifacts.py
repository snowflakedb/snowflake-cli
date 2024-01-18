import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Union

import strictyaml
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
        super().__init__(f"{self.__doc__}\ndest_path = {dest_path}")
        self.dest_path = dest_path


class NotInDeployRootError(ClickException):
    """
    The specified destination path is outside of the deploy root, or
    would entirely replace it. This can happen when a relative path
    with ".." is provided, or when "." is used as the destination
    (use "./" instead to copy into the deploy root).
    """

    artifact_src: str
    dest_path: Path
    deploy_root: Path

    def __init__(self, artifact_src: str, dest_path: Path, deploy_root: Path):
        super().__init__(
            f"""
            {self.__doc__}
            artifact_src = {artifact_src}
            dest_path = {dest_path}
            deploy_root = {deploy_root}
            """
        )
        self.artifact_src = artifact_src
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
    trailing forward slash (/). Note that we use the forward slash
    even on Windows so that snowflake.yml can be shared between OSes.

    This means that to put a file in the root of the stage, we need
    to specify "./" as its destination, or omit it (but only if the
    file already lives in the project root).
    """
    return s.endswith("/")


def delete(path: Path) -> None:
    """
    Obliterates whatever is at the given path, or is a no-op if the
    given path does not represent a file or directory that exists.
    """
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains


def symlink_or_copy(src: Path, dst: Path, makedirs=True, overwrite=True) -> None:
    """
    Tries to create a symlink to src at dst; failing that (i.e. in Windows
    without Administrator / Developer Mode) copies the file from src to dst instead.
    If makedirs is True, the directory hierarchy above dst is created if any
    of those directories do not exist.
    """
    if makedirs:
        dst.parent.mkdir(parents=True, exist_ok=True)
    if overwrite:
        delete(dst)
    try:
        os.symlink(src, dst)
    except OSError:
        shutil.copy(src, dst)


def translate_artifact(item: Union[dict, str]) -> ArtifactMapping:
    """
    Builds an artifact mapping from a project definition value.
    Validation is done later when we actually resolve files / folders.
    """

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
    If a glob does not specify a directory (i.e. does not end with a path separator)

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


def resolve_without_follow(path: Path) -> Path:
    """
    Resolves a Path to an absolute version of itself, without following
    symlinks like Path.resolve() does.
    """
    return Path(os.path.abspath(path))


def build_bundle(
    project_root: Path, deploy_root: Path, artifacts: List[ArtifactMapping]
):
    """
    Prepares a local folder (deploy_root) with configured app artifacts.
    This folder can then be uploaded to a stage.
    """
    resolved_root = deploy_root.resolve()
    if resolved_root.exists() and not resolved_root.is_dir():
        raise DeployRootError(
            f"Deploy root {resolved_root} exists, but is not a directory!"
        )

    if project_root.resolve() not in resolved_root.parents:
        raise DeployRootError(
            f"Deploy root {resolved_root} is not a descendent of the project directory!"
        )

    # users may have removed files or entire artifact mappings from their project
    # definition since the last time we bundled; we need to clear the deploy root first
    if resolved_root.exists():
        delete(resolved_root)

    for artifact in artifacts:
        dest_path = resolve_without_follow(Path(resolved_root, artifact.dest))
        source_paths = get_source_paths(artifact, project_root)

        if specifies_directory(artifact.dest):
            # make sure we are only modifying files / directories inside the deploy root
            if resolved_root != dest_path and resolved_root not in dest_path.parents:
                raise NotInDeployRootError(artifact.src, dest_path, resolved_root)

            # copy all files as children of the given destination path
            for source_path in source_paths:
                symlink_or_copy(source_path, dest_path / source_path.name)
        else:
            # ensure we are copying into the deploy root, not replacing it!
            if resolved_root not in dest_path.parents:
                raise NotInDeployRootError(artifact.src, dest_path, resolved_root)

            if len(source_paths) == 1:
                # copy a single file as the given destination path
                symlink_or_copy(source_paths[0], dest_path)
            else:
                # refuse to map multiple source files to one destination (undefined behaviour)
                raise TooManyFilesError(dest_path)


def find_manifest_file(deploy_root: Path) -> Path:
    """
    Find manifest.yml file, if available, in the deploy_root of the native apps project.
    """
    resolved_root = deploy_root.resolve()
    for root, _, files in os.walk(resolved_root):
        for file in files:
            if file.lower() == "manifest.yml":
                return Path(os.path.join(root, file))

    raise ClickException(
        "Required manifest.yml file not found in the deploy root of the native apps project."
    )


def find_version_info_in_manifest_file(
    deploy_root: Path,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Find version and patch, if available, in the manifest.yml file.
    """
    version_field = "version"
    name_field = "name"
    patch_field = "patch"

    manifest_file = find_manifest_file(deploy_root)
    with open(manifest_file, "r") as file:
        manifest_content = strictyaml.load(file.read())

    version_name: Optional[str] = None
    patch_name: Optional[str] = None

    if version_field in manifest_content:
        version_info = manifest_content[version_field]
        version_name = version_info[name_field]
        if patch_field in version_info:
            patch_name = version_info[patch_field]

    return version_name, patch_name
