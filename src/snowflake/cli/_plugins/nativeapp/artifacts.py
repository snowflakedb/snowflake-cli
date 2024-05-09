import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from click.exceptions import ClickException
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.secure_path import SecurePath
from yaml import safe_load

# Map from source directories and files in the project directory to their path in the deploy directory. Both paths are absolute.
ArtifactDeploymentMap = Dict[Path, Path]


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
    spath = SecurePath(path)
    if spath.path.is_file():
        spath.unlink()  # remove the file
    elif spath.path.is_dir():
        spath.rmdir(recursive=True)  # remove dir and all contains


def symlink_or_copy(src: Path, dst: Path, makedirs=True, overwrite=True) -> None:
    """
    Tries to create a symlink to src at dst; failing that (i.e. in Windows
    without Administrator / Developer Mode) copies the file from src to dst instead.
    If makedirs is True, the directory hierarchy above dst is created if any
    of those directories do not exist.
    """
    ssrc = SecurePath(src)
    sdst = SecurePath(dst)
    if makedirs:
        sdst.parent.mkdir(parents=True, exist_ok=True)
    if overwrite:
        delete(dst)
    try:
        os.symlink(src, dst)
    except OSError:
        ssrc.copy(dst)


def translate_artifact(item: Union[dict, str]) -> ArtifactMapping:
    """
    Builds an artifact mapping from a project definition value.
    Validation is done later when we actually resolve files / folders.
    """

    if isinstance(item, PathMapping):
        return ArtifactMapping(item.src, item.dest if item.dest else item.src)

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
    project_root: Path,
    deploy_root: Path,
    artifacts: List[ArtifactMapping],
) -> ArtifactDeploymentMap:
    """
    Prepares a local folder (deploy_root) with configured app artifacts.
    This folder can then be uploaded to a stage.
    Returns a map of the copied source files, pointing to where they were copied.
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

    mapped_files: ArtifactDeploymentMap = {}
    for artifact in artifacts:
        dest_path = resolve_without_follow(Path(resolved_root, artifact.dest))
        source_paths = get_source_paths(artifact, project_root)

        if specifies_directory(artifact.dest):
            # make sure we are only modifying files / directories inside the deploy root
            if resolved_root != dest_path and resolved_root not in dest_path.parents:
                raise NotInDeployRootError(artifact.src, dest_path, resolved_root)

            # copy all files as children of the given destination path
            for source_path in source_paths:
                dest_child_path = dest_path / source_path.name
                symlink_or_copy(source_path, dest_child_path)
                mapped_files[source_path.resolve()] = dest_child_path
        else:
            # ensure we are copying into the deploy root, not replacing it!
            if resolved_root not in dest_path.parents:
                raise NotInDeployRootError(artifact.src, dest_path, resolved_root)

            if len(source_paths) == 1:
                # copy a single file as the given destination path
                symlink_or_copy(source_paths[0], dest_path)
                mapped_files[source_paths[0].resolve()] = dest_path
            else:
                # refuse to map multiple source files to one destination (undefined behaviour)
                raise TooManyFilesError(dest_path)
    return mapped_files


def find_manifest_file(deploy_root: Path) -> Path:
    """
    Find manifest.yml file, if available, in the deploy_root of the Snowflake Native App project.
    """
    resolved_root = deploy_root.resolve()
    for root, _, files in os.walk(resolved_root):
        for file in files:
            if file.lower() == "manifest.yml":
                return Path(os.path.join(root, file))

    raise ClickException(
        "Required manifest.yml file not found in the deploy root of the Snowflake Native App project."
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
    with SecurePath(manifest_file).open(
        "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
    ) as file:
        manifest_content = safe_load(file.read())

    version_name: Optional[str] = None
    patch_name: Optional[str] = None

    if version_field in manifest_content:
        version_info = manifest_content[version_field]
        version_name = version_info[name_field]
        if patch_field in version_info:
            patch_name = version_info[patch_field]

    return version_name, patch_name


def source_path_to_deploy_path(
    source_path: Path, mapped_files: ArtifactDeploymentMap
) -> Path:
    """Returns the absolute path where the specified source path was copied to during bundle."""

    source_path = source_path.resolve()

    if source_path in mapped_files:
        return mapped_files[source_path]

    # Find the first parent directory that exists in mapped_files
    common_root = source_path
    while common_root:
        if common_root in mapped_files:
            break
        elif common_root.parent != common_root:
            common_root = common_root.parent
        else:
            raise ClickException(f"Could not find the deploy path of {source_path}")

    # Construct the target deploy path
    path_to_symlink = mapped_files[common_root]
    relative_path_to_target = Path(source_path).relative_to(common_root)
    result = Path(path_to_symlink, relative_path_to_target)

    return result
