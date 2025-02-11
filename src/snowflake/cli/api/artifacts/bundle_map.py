from __future__ import annotations

import itertools
import os
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from snowflake.cli.api.artifacts.common import (
    ArtifactError,
    NotInDeployRootError,
    SourceNotFoundError,
    TooManyFilesError,
)
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.utils.path_utils import resolve_without_follow

ArtifactPredicate = Callable[[Path, Path], bool]


def _specifies_directory(s: str) -> bool:
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


class BundleMap:
    """
    Computes the mapping between project directory artifacts (aka source artifacts) to their deploy root location
    (aka destination artifact).

    :param project_root: The root directory of the project and base for all relative paths. Must be an absolute path.
    :param deploy_root: The directory where artifacts should be copied to. Must be an absolute path.
    """

    def __init__(self, *, project_root: Path, deploy_root: Path):
        # If a relative path ends up here, it's a bug in the app and can lead to other
        # subtle bugs as paths would be resolved relative to the current working directory.
        assert (
            project_root.is_absolute()
        ), f"Project root {project_root} must be an absolute path."
        assert (
            deploy_root.is_absolute()
        ), f"Deploy root {deploy_root} must be an absolute path."

        self._project_root: Path = resolve_without_follow(project_root)
        self._deploy_root: Path = resolve_without_follow(deploy_root)
        self._artifact_map = _ArtifactPathMap(project_root=self._project_root)

    def is_empty(self) -> bool:
        return self._artifact_map.is_empty()

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
            # ignore this item since it's in the deploy root. This can happen if the bundle map is created
            # after the bundle step and a project is using rules that are not sufficiently constrained.
            # Since the bundle step starts with deleting the deploy root, we wouldn't normally encounter this situation.
            return

        canonical_src = self._canonical_src(src)
        canonical_dest = self._canonical_dest(dest)

        if map_as_child:
            # Make sure the destination is a child of the original, since this was requested
            canonical_dest = canonical_dest / canonical_src.name
            dest_is_dir = absolute_src.is_dir()

        self._artifact_map.put(
            src=canonical_src, dest=canonical_dest, dest_is_dir=dest_is_dir
        )

    def _add_mapping(self, src: str, dest: Optional[str] = None):
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
                self._add(resolved_src, dest_path, _specifies_directory(dest))
            else:
                self._add(
                    resolved_src,
                    resolved_src.relative_to(self._project_root),
                    False,
                )

        if not match_found:
            raise SourceNotFoundError(src)

    def add(self, mapping: PathMapping) -> None:
        """
        Adds an artifact mapping rule to this instance.
        """
        self._add_mapping(mapping.src, mapping.dest)

    def _expand_artifact_mapping(
        self,
        src: Path,
        dest: Path,
        absolute: bool = False,
        expand_directories: bool = False,
        predicate: ArtifactPredicate = lambda src, dest: True,
    ) -> Iterator[Tuple[Path, Path]]:
        """
        Expands the specified source-destination mapping according to the provided options.
        The original mapping is yielded, followed by any expanded mappings derived from
        it.

        Arguments:
            src {Path} -- the source path
            dest {Path} -- the destination path
            absolute {bool} -- when True, all mappings will be yielded as absolute paths
            expand_directories {bool} -- when True, child mappings are yielded if the source path is a directory.
            predicate {ArtifactPredicate} -- when specified, only mappings satisfying this predicate will be yielded.
        """
        canonical_src = self._canonical_src(src)
        canonical_dest = self._canonical_dest(dest)

        absolute_src = self._absolute_src(canonical_src)
        absolute_dest = self._absolute_dest(canonical_dest)
        src_for_output = self._to_output_src(absolute_src, absolute)
        dest_for_output = self._to_output_dest(absolute_dest, absolute)

        if predicate(src_for_output, dest_for_output):
            yield src_for_output, dest_for_output

        if absolute_src.is_dir() and expand_directories:
            # both src and dest are directories, and expanding directories was requested. Traverse src, and map each
            # file to the dest directory
            for root, subdirs, files in os.walk(absolute_src, followlinks=True):
                relative_root = Path(root).relative_to(absolute_src)
                for name in itertools.chain(subdirs, files):
                    src_file_for_output = src_for_output / relative_root / name
                    dest_file_for_output = dest_for_output / relative_root / name
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
        for src, dest in self._artifact_map:
            for deployed_src, deployed_dest in self._expand_artifact_mapping(
                src,
                dest,
                absolute=absolute,
                expand_directories=expand_directories,
                predicate=predicate,
            ):
                yield deployed_src, deployed_dest

    def to_deploy_paths(self, src: Path) -> List[Path]:
        """
        Converts a source path to its corresponding deploy root path. If the input path is relative to the project root,
        paths relative to the deploy root are returned. If the input path is absolute, absolute paths are returned.

        Note that the provided source path must be part of a mapping. If the source path is not part of any mapping,
        an empty list is returned. For example, if `app/*` is specified as the source of a mapping,
        `to_deploy_paths(Path("app"))` will not yield any result.

        Arguments:
            src {Path} -- the source path within the project root, in canonical or absolute form.

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

        # 1. Check for exact rule matches for this path
        canonical_dests = self._artifact_map.get_destinations(canonical_src)
        if canonical_dests:
            for d in canonical_dests:
                output_destinations.append(self._to_output_dest(d, is_absolute))

        # 2. Check for any matches to parent directories for this path that would
        # cause this path to be part of the recursive copy
        canonical_parent = canonical_src.parent
        canonical_parent_dests = self.to_deploy_paths(canonical_parent)
        if canonical_parent_dests:
            canonical_child = canonical_src.relative_to(canonical_parent)
            for d in canonical_parent_dests:
                output_destinations.append(
                    self._to_output_dest(d / canonical_child, is_absolute)
                )

        return output_destinations

    def all_sources(self, absolute: bool = False) -> Iterator[Path]:
        """
        Yields each registered artifact source in the project.

        Arguments:
            self: this instance
            absolute (bool): Specifies whether the yielded paths should be joined with the absolute project root.
        Returns:
          An iterator over all artifact mapping source paths.
        """
        for src in self._artifact_map.all_sources():
            yield self._to_output_src(src, absolute)

    def to_project_path(self, dest: Path) -> Optional[Path]:
        """
        Converts a deploy root path to its corresponding project source path. If the input path is relative to the
        deploy root, a path relative to the project root is returned. If the input path is absolute, an absolute path is
        returned.

        Arguments:
            dest {Path} -- the destination path within the deploy root, in canonical or absolute form.

        Returns:
            The project root path for the given deploy root path, or None if no such path exists.
        """
        is_absolute = dest.is_absolute()
        try:
            canonical_dest = self._canonical_dest(dest)
        except NotInDeployRootError:
            # No mapping possible for the dest path
            return None

        # 1. Look for an exact rule matching this path. If we find any, then
        # stop searching. This is because each destination path can only originate
        # from a single source (however, one source can be copied to multiple destinations).
        canonical_src = self._artifact_map.get_source(canonical_dest)
        if canonical_src is not None:
            return self._to_output_src(canonical_src, is_absolute)

        # 2. No exact match was found, look for a match for parent directories of this
        # path, recursively. Stop when a match is found
        canonical_parent = canonical_dest.parent
        if canonical_parent == canonical_dest:
            return None
        canonical_parent_src = self.to_project_path(canonical_parent)
        if canonical_parent_src is not None:
            canonical_child = canonical_dest.relative_to(canonical_parent)
            canonical_child_candidate = canonical_parent_src / canonical_child
            if self._absolute_src(canonical_child_candidate).exists():
                return self._to_output_src(canonical_child_candidate, is_absolute)

        # No mapping for this destination path
        return None

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


class _ArtifactPathMap:
    """
    A specialized version of an ordered multimap used to keep track of artifact
    source-destination mappings. The mapping is bidirectional, so it can be queried
    by source or destination paths. All paths manipulated by this class must be in
    relative, canonical form (relative to the project or deploy roots, as appropriate).
    """

    def __init__(self, project_root: Path):
        self._project_root = project_root

        # All (src,dest) pairs in inserting order, for iterating
        self.__src_dest_pairs: List[Tuple[Path, Path]] = []
        # built-in dict instances are ordered as of Python 3.7
        self.__src_to_dest: Dict[Path, List[Path]] = {}
        self.__dest_to_src: Dict[Path, Optional[Path]] = {}

        # This dictionary accumulates keys for each directory or file to be created in
        # the deploy root for any artifact mapping rule being processed. This includes
        # children of directories that are copied to the deploy root. Having this
        # information available is critical to detect possible clashes between rules.
        self._dest_is_dir: Dict[Path, bool] = {}

    def put(self, src: Path, dest: Path, dest_is_dir: bool) -> None:
        """
        Adds a new source-destination mapping pair to this map, if necessary. Note that
        this is internal logic that assumes that src-dest pairs have already been preprocessed
        by the enclosing BundleMap (for example, only file -> file and
        directory -> directory mappings are possible here due to the preprocessing step).

        Arguments:
            src {Path} -- the source path, in canonical form.
            dest {Path} -- the destination path, in canonical form.
            dest_is_dir {bool} -- whether the destination path is a directory.
        """
        # Both paths should be in canonical form
        assert not src.is_absolute()
        assert not dest.is_absolute()

        absolute_src = self._project_root / src

        current_source = self.__dest_to_src.get(dest)
        src_is_dir = absolute_src.is_dir()
        if dest_is_dir:
            assert src_is_dir  # file -> directory is not possible here given how rules are processed

            # directory -> directory
            # Check that dest is currently unmapped
            current_is_dir = self._dest_is_dir.get(dest, False)
            if current_is_dir:
                # mapping to an existing directory is not allowed
                raise TooManyFilesError(dest)
        else:
            # file -> file
            # Check that there is no previous mapping for the same file.
            if current_source is not None and current_source != src:
                # There is already a different source mapping to this destination
                raise TooManyFilesError(dest)

        if src_is_dir:
            # mark all subdirectories of this source as directories so that we can
            # detect accidental clobbering
            for root, _, files in os.walk(absolute_src, followlinks=True):
                canonical_subdir = Path(root).relative_to(absolute_src)
                canonical_dest_subdir = dest / canonical_subdir
                self._update_dest_is_dir(canonical_dest_subdir, is_dir=True)
                for f in files:
                    self._update_dest_is_dir(canonical_dest_subdir / f, is_dir=False)

        # make sure we check for dest_is_dir consistency regardless of whether the
        # insertion happened. This update can fail, so we need to do it first to
        # avoid applying partial updates to the underlying data storage.
        self._update_dest_is_dir(dest, dest_is_dir)

        dests = self.__src_to_dest.setdefault(src, [])
        if dest not in dests:
            dests.append(dest)
            self.__dest_to_src[dest] = src
            self.__src_dest_pairs.append((src, dest))

    def get_source(self, dest: Path) -> Optional[Path]:
        """
        Returns the source path associated with the provided destination path, if any.
        """
        return self.__dest_to_src.get(dest)

    def get_destinations(self, src: Path) -> Iterable[Path]:
        """
        Returns all destination paths associated with the provided source path, in insertion order.
        """
        return self.__src_to_dest.get(src, [])

    def all_sources(self) -> Iterable[Path]:
        """
        Returns all source paths associated with this map, in insertion order.
        """
        return self.__src_to_dest.keys()

    def is_empty(self) -> bool:
        """
        Returns True if this map has no source-destination mappings.
        """
        return len(self.__src_dest_pairs) == 0

    def __iter__(self) -> Iterator[Tuple[Path, Path]]:
        """
        Returns all (source, destination) pairs known to this map, in insertion order.
        """
        return iter(self.__src_dest_pairs)

    def _update_dest_is_dir(self, dest: Path, is_dir: bool) -> None:
        """
        Recursively marks seen destination paths as either files or folders, raising an error if any inconsistencies
        from previous invocations of this method are encountered.

        Arguments:
            dest {Path} -- the destination path, in canonical form.
            is_dir {bool} -- whether the destination path is a directory.
        """
        assert not dest.is_absolute()  # dest must be in canonical relative form

        current_is_dir = self._dest_is_dir.get(dest, None)
        if current_is_dir is not None and current_is_dir != is_dir:
            raise ArtifactError(f"Conflicting type for destination path: {dest}")

        parent = dest.parent
        if parent != dest:
            self._update_dest_is_dir(parent, True)

        self._dest_is_dir[dest] = is_dir
