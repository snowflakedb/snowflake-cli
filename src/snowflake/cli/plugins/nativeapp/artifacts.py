from __future__ import annotations

import itertools
import os
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

from click.exceptions import ClickException
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.secure_path import SecurePath
from yaml import safe_load


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
    Multiple files were mapped to one output file.
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


@dataclass
class ArtifactMapping:
    """
    Used to keep track of equivalent paths / globs so we can copy artifacts from the project folder to the deploy root.
    """

    src: str
    dest: str


ArtifactPredicate = Callable[[Path, Path], bool]


class BundleMap:
    """
    Computes the mapping between project directory artifacts (aka source artifacts) to their deploy root location
    (aka destination artifact). This information is primarily used when bundling a native applications project.
    """

    def __init__(self, *, project_root: Path, deploy_root: Path):
        self._project_root: Path = resolve_without_follow(project_root)
        self._deploy_root: Path = resolve_without_follow(deploy_root)
        self._src_to_dest: Dict[Path, List[Path]] = {}
        self._dest_to_src: Dict[Path, List[Path]] = {}
        self._dest_is_dir: Dict[Path, bool] = {}

    def deploy_root(self) -> Path:
        return self._deploy_root

    def project_root(self) -> Path:
        return self._project_root

    def _add(self, src: Path, dest: Path, map_as_child: bool) -> None:
        """
        Adds the specified artifact mapping rule to this map.

        Arguments:
            src {Path} -- the source path
            dest {Path} -- the destination path
            map_as_child {bool} -- when True, the source will be added as a child of the specified destination.
        """
        absolute_src = self._absolute_src(src)
        absolute_dest = self._absolute_dest(dest, src_path=src)
        dest_is_dir = absolute_src.is_dir() or map_as_child

        # Check for the special case of './' as a target ('.' is not allowed)
        if absolute_dest == self._deploy_root and not map_as_child:
            raise NotInDeployRootError(
                dest_path=dest, deploy_root=self._deploy_root, src_path=src
            )

        if self._deploy_root in absolute_src.parents:
            # ignore this item since it's in the deploy root. This can happen if the bundle map is create
            # after the bundle step and a project is using rules that are not sufficiently constrained.
            # Since the bundle step starts with deleting the deploy root, we wouldn't normally encounter this situation.
            return

        canonical_src = self._canonical_src(src)
        canonical_dest = self._canonical_dest(dest)

        src_is_dir = absolute_src.is_dir()
        if map_as_child:
            # Make sure the destination is a child of the original, since this was requested
            canonical_dest = canonical_dest / canonical_src.name
            dest_is_dir = src.is_dir()

        # Verify that multiple files are not being mapped to a single file destination
        current_sources = self._dest_to_src.setdefault(canonical_dest, [])
        if not dest_is_dir:
            # the destination is a file
            if (canonical_src not in current_sources) and len(current_sources) > 0:
                raise TooManyFilesError(dest)

        # Perform all updates together we don't end up with inconsistent state
        self._update_dest_is_dir(canonical_dest, dest_is_dir)
        current_dests = self._src_to_dest.setdefault(canonical_src, [])
        if canonical_dest not in current_dests:
            current_dests.append(canonical_dest)
        if canonical_src not in current_sources:
            current_sources.append(canonical_src)

    def _add_mapping(self, src: str, dest: Optional[str]):
        """
        Adds the specified artifact rule to this instance. The source should be relative to the project directory. It
        is interpreted as a file, directory or glob pattern. If the destination path is not specified, each source match
        is mapped to an identical path in the deploy root.
        """
        match_found = False

        src_path = Path(src)
        if src_path.is_absolute():
            raise ArtifactError("Source path must be a relative path")

        for resolved_src in self._project_root.glob(src):
            match_found = True

            if dest:
                dest_stem = dest.rstrip("/")
                if not dest_stem:
                    # handle '/' as the destination as a special case. This is because specifying only '/' as a
                    # a destination looks like '.' once all forwards slashes are stripped. If we don't handle it
                    # specially here, `dest: /` would incorrectly be allowed.
                    raise NotInDeployRootError(
                        dest_path=dest,
                        deploy_root=self._deploy_root,
                        src_path=resolved_src,
                    )
                dest_path = Path(dest.rstrip("/"))
                if dest_path.is_absolute():
                    raise ArtifactError("Destination path must be a relative path")
                self._add(resolved_src, dest_path, specifies_directory(dest))
            else:
                self._add(
                    resolved_src,
                    resolved_src.relative_to(self._project_root),
                    False,
                )

        if not match_found:
            raise SourceNotFoundError(src)

    def add(self, mapping: Union[ArtifactMapping, PathMapping]) -> None:
        """
        Adds an artifact mapping rule to this instance.
        """
        if isinstance(mapping, ArtifactMapping):
            self._add_mapping(mapping.src, mapping.dest)
        elif isinstance(mapping, PathMapping):
            self._add_mapping(mapping.src, mapping.dest)
        else:
            raise RuntimeError(f"Unsupported mapping type: {type(mapping)}")

    def _mappings_for_source(
        self,
        src: Path,
        absolute: bool = False,
        expand_directories: bool = False,
        predicate: ArtifactPredicate = lambda src, dest: True,
    ) -> Iterator[Tuple[Path, Path]]:
        canonical_src = self._canonical_src(src)
        canonical_dests = self._src_to_dest.get(canonical_src)
        assert canonical_dests is not None

        absolute_src = self._absolute_src(canonical_src)
        src_for_output = self._to_output_src(absolute_src, absolute)
        dests_for_output = [self._to_output_dest(p, absolute) for p in canonical_dests]

        for d in dests_for_output:
            if predicate(src_for_output, d):
                yield src_for_output, d

        if absolute_src.is_dir() and expand_directories:
            # both src and dest are directories, and expanding directories was requested. Traverse src, and map each
            # file to the dest directory
            for (root, subdirs, files) in os.walk(absolute_src, followlinks=True):
                relative_root = Path(root).relative_to(absolute_src)
                for name in itertools.chain(subdirs, files):
                    for d in dests_for_output:
                        src_file_for_output = src_for_output / relative_root / name
                        dest_file_for_output = d / relative_root / name
                        if predicate(src_file_for_output, dest_file_for_output):
                            yield src_file_for_output, dest_file_for_output

    def all_mappings(
        self,
        absolute: bool = False,
        expand_directories: bool = False,
        predicate: ArtifactPredicate = lambda src, dest: True,
    ) -> Iterator[Tuple[Path, Path]]:
        """
        Yields a (src, dest) pair for each deployed artifact in the project. Each pair corresponds to a single file
        in the project. Source directories are resolved as needed to resolve their contents.

        Arguments:
            self: this instance
            absolute (bool): Specifies whether the yielded paths should be joined with the project or deploy roots,
             as appropriate.
            expand_directories (bool): Specifies whether directory to directory mappings should be expanded to
             resolve their contained files.
            predicate (PathPredicate): If provided, the predicate is invoked with both the source path and the
             destination path as arguments. Only pairs selected by the predicate are returned.

        Returns:
          An iterator over all matching deployed artifacts.
        """
        for src in self._src_to_dest.keys():
            for deployed_src, deployed_dest in self._mappings_for_source(
                src,
                absolute=absolute,
                expand_directories=expand_directories,
                predicate=predicate,
            ):
                yield deployed_src, deployed_dest

    def to_deploy_paths(self, src: Path) -> List[Path]:
        """
        Converts a source path to its corresponding deploy root path. If the input path is relative to the project root,
        a path relative to the deploy root is returned. If the input path is absolute, an absolute path is returned.

        Returns:
            The deploy root paths for the given source path, or an empty list if no such path exists.
        """
        is_absolute = src.is_absolute()

        try:
            absolute_src = self._absolute_src(src)
            if not absolute_src.exists():
                return []
            canonical_src = self._canonical_src(absolute_src)
        except ArtifactError:
            # No mapping is possible for this src path
            return []

        output_destinations: List[Path] = []

        canonical_dests = self._src_to_dest.get(canonical_src)
        if canonical_dests is not None:
            for d in canonical_dests:
                output_destinations.append(self._to_output_dest(d, is_absolute))

        canonical_parent = canonical_src.parent
        canonical_parent_dests = self.to_deploy_paths(canonical_parent)
        if canonical_parent_dests:
            canonical_child = canonical_src.relative_to(canonical_parent)
            for d in canonical_parent_dests:
                output_destinations.append(
                    self._to_output_dest(d / canonical_child, is_absolute)
                )

        return output_destinations

    def _absolute_src(self, src: Path) -> Path:
        if src.is_absolute():
            resolved_src = resolve_without_follow(src)
        else:
            resolved_src = resolve_without_follow(self._project_root / src)
        if self._project_root not in resolved_src.parents:
            raise ArtifactError(
                f"Source is not in the project root: {src}, root={self._project_root}"
            )
        return resolved_src

    def _absolute_dest(self, dest: Path, src_path: Optional[Path] = None) -> Path:
        if dest.is_absolute():
            resolved_dest = resolve_without_follow(dest)
        else:
            resolved_dest = resolve_without_follow(self._deploy_root / dest)
        if (
            self._deploy_root != resolved_dest
            and self._deploy_root not in resolved_dest.parents
        ):
            raise NotInDeployRootError(
                dest_path=dest, deploy_root=self._deploy_root, src_path=src_path
            )

        return resolved_dest

    def _canonical_src(self, src: Path) -> Path:
        """
        Returns the canonical version of a source path, relative to the project root.
        """
        absolute_src = self._absolute_src(src)
        return absolute_src.relative_to(self._project_root)

    def _canonical_dest(self, dest: Path) -> Path:
        """
        Returns the canonical version of a destination path, relative to the deploy root.
        """
        absolute_dest = self._absolute_dest(dest)
        return absolute_dest.relative_to(self._deploy_root)

    def _to_output_dest(self, dest: Path, absolute: bool) -> Path:
        return self._absolute_dest(dest) if absolute else self._canonical_dest(dest)

    def _to_output_src(self, src: Path, absolute: bool) -> Path:
        return self._absolute_src(src) if absolute else self._canonical_src(src)

    def _update_dest_is_dir(self, canonical_dest: Path, is_dir: bool) -> None:
        current_is_dir = self._dest_is_dir.get(canonical_dest, None)
        if current_is_dir is not None and is_dir != current_is_dir:
            raise ArtifactError(
                "Conflicting type for destination path: {canonical_dest}"
            )

        parent = canonical_dest.parent
        if parent != canonical_dest:
            self._update_dest_is_dir(parent, True)

        self._dest_is_dir[canonical_dest] = is_dir


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


def symlink_or_copy(
    src: Path, dst: Path, deploy_root: Path, makedirs=True, overwrite=True
) -> None:
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

    # Verify that the mapping isn't accidentally trying to create a file in the project source through symlinks.
    # We need to ensure we're resolving symlinks for this check to be effective.
    resolved_dst = dst.resolve()
    resolved_deploy_root = deploy_root.resolve()
    if resolved_deploy_root not in resolved_dst.parents:
        raise NotInDeployRootError(dest_path=dst, deploy_root=deploy_root, src_path=src)

    if overwrite:
        delete(dst)
    try:
        os.symlink(src, dst)
    except OSError:
        ssrc.copy(dst)


def translate_artifact(item: Union[PathMapping, str]) -> ArtifactMapping:
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
) -> BundleMap:
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

    bundle_map = BundleMap(project_root=project_root, deploy_root=deploy_root)
    for artifact in artifacts:
        bundle_map.add(artifact)

    for (absolute_src, absolute_dest) in bundle_map.all_mappings(
        absolute=True, expand_directories=False
    ):
        symlink_or_copy(absolute_src, absolute_dest, deploy_root=deploy_root)

    return bundle_map


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


def find_and_read_manifest_file(deploy_root: Path) -> Dict[str, Any]:
    """
    Finds the manifest file in the deploy root of the project, and reads the contents and returns them
    as a dictionary.
    """
    manifest_file = find_manifest_file(deploy_root=deploy_root)
    with SecurePath(manifest_file).open(
        "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
    ) as file:
        manifest_content = safe_load(file.read())
    return manifest_content


def find_setup_script_file(deploy_root: Path) -> Path:
    """
    Find the setup script file, if available, in the deploy_root of the Snowflake Native App project.
    """
    artifacts = "artifacts"
    setup_script = "setup_script"

    manifest_content = find_and_read_manifest_file(deploy_root=deploy_root)

    if (artifacts in manifest_content) and (
        setup_script in manifest_content[artifacts]
    ):
        setup_script_rel_path = manifest_content[artifacts][setup_script]
        file_name = Path(deploy_root / setup_script_rel_path)
        if file_name.is_file():
            return file_name
        else:
            raise ClickException(f"Could not find setup script file at {file_name}.")
    else:
        raise ClickException(
            "Manifest.yml file must contain an artifacts section to specify the location of the setup script."
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

    manifest_content = find_and_read_manifest_file(deploy_root=deploy_root)

    version_name: Optional[str] = None
    patch_name: Optional[str] = None

    if version_field in manifest_content:
        version_info = manifest_content[version_field]
        version_name = version_info[name_field]
        if patch_field in version_info:
            patch_name = version_info[patch_field]

    return version_name, patch_name
