# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import errno
import logging
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union

from snowflake.cli.api.exceptions import DirectoryIsNotEmptyError, FileTooLargeError
from snowflake.cli.api.secure_utils import (
    chmod as secure_chmod,
)
from snowflake.cli.api.secure_utils import (
    restrict_file_permissions,
)

log = logging.getLogger(__name__)

UNLIMITED = -1


class SecurePath:
    def __init__(self, path: Union[Path, str]):
        self._path = Path(path)

    def __repr__(self):
        return f'SecurePath("{self._path}")'

    def __truediv__(self, key):
        return SecurePath(self._path / key)

    @property
    def path(self) -> Path:
        """
        Returns itself in pathlib.Path format
        """
        return self._path

    def chmod(self, permissions_mask: int) -> None:
        """
        Change the file mode and permissions, like os.chmod().
        """
        secure_chmod(self._path, permissions_mask)

    @property
    def parent(self):
        """
        The logical parent of the path. For details, check pathlib.Path.parent
        """
        return SecurePath(self._path.parent)

    def absolute(self):
        """
        Make the path absolute, without normalization or resolving symlinks.
        """
        return SecurePath(self._path.absolute())

    def iterdir(self):
        """
        When the path points to a directory, yield path objects of the directory contents.
        Otherwise, NotADirectoryError is raised.
        If the location does not exist, FileNotFoundError is raised.

        For details, check pathlib.Path.iterdir()
        """
        self.assert_exists()
        self.assert_is_directory()
        return (SecurePath(p) for p in self._path.iterdir())

    def exists(self) -> bool:
        """
        Return True if the path points to an existing file or directory.
        """
        return self._path.exists()

    def is_dir(self) -> bool:
        """
        Return True if the path points to a directory (or a symbolic link pointing to a directory),
        False if it points to another kind of file.
        """
        return self._path.is_dir()

    def is_file(self) -> bool:
        """
        Return True if the path points to a regular file (or a symbolic link pointing to a regular file),
        False if it points to another kind of file.
        """
        return self._path.is_file()

    @property
    def name(self) -> str:
        """A string representing the final path component."""
        return self._path.name

    def restrict_permissions(self) -> None:
        """
        Restrict file/directory permissions to owner-only.
        """
        restrict_file_permissions(self._path)

    def touch(self, permissions_mask: int = 0o600, exist_ok: bool = True) -> None:
        """
        Create a file at this given path. For details, check pathlib.Path.touch()
        """
        if not self.exists():
            log.info("Creating file %s", str(self._path))
        self._path.touch(mode=permissions_mask, exist_ok=exist_ok)

    def mkdir(
        self,
        permissions_mask: int = 0o700,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        """
        Create a directory at this given path. For details, check pathlib.Path.mkdir()
        """
        if parents and not self.parent.exists():
            self.parent.mkdir(
                permissions_mask=permissions_mask, exist_ok=exist_ok, parents=True
            )
        if not self.exists():
            log.info("Creating directory %s", str(self._path))
        self._path.mkdir(mode=permissions_mask, exist_ok=exist_ok)

    def read_text(self, file_size_limit_mb: int, *args, **kwargs) -> str:
        """
        Return the decoded contents of the file as a string.
        Raises an error of the file exceeds the specified size limit.
        For details, check pathlib.Path.read_text()
        """
        self._assert_exists_and_is_file()
        self._assert_file_size_limit(file_size_limit_mb)
        log.info("Reading file %s", self._path)
        return self._path.read_text(*args, **kwargs)

    def write_text(self, *args, **kwargs):
        """
        Open the file pointed to in text mode, write data to it, and close the file.
        """
        if not self.exists():
            self.touch()
        log.info("Writing to file %s", self._path)
        self.path.write_text(*args, **kwargs)

    @contextmanager
    def open(  # noqa: A003
        self,
        mode="r",
        read_file_limit_mb: Optional[int] = None,
        **open_kwargs,
    ):
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        If the file is opened for reading, [read_file_limit_kb] parameter must be provided.
        Raises error if the read file exceeds the specified size limit.
        """
        opened_for_reading = "r" in mode
        if opened_for_reading:
            assert (
                read_file_limit_mb is not None
            ), "For reading mode ('r') read_file_limit_mb must be provided"
            self._assert_exists_and_is_file()
            self._assert_file_size_limit(read_file_limit_mb)

        if self.exists():
            self.assert_is_file()
        else:
            self.touch()  # makes sure permissions of freshly-created file are strict

        log.info("Opening file %s in mode '%s'", self._path, mode)
        with self._path.open(mode=mode, **open_kwargs) as fd:
            yield fd
        log.info("Closing file %s", self._path)

    def move(self, destination: Union[Path, str]) -> "SecurePath":
        """Recursively move a file or directory (src) to another location and return the destination.

        If dst is an existing directory or a symlink to a directory, then src is moved inside that directory.
        The destination path in that directory must not already exist.
        """
        destination = Path(destination)
        if destination.is_dir():
            destination = destination / self._path.name
        if destination.exists():
            _raise_file_exists_error(destination)
        log.info("Moving %s to %s", str(self._path), destination.resolve())
        return SecurePath(shutil.move(str(self._path), destination))

    def copy(
        self, destination: Union[Path, str], dirs_exist_ok: bool = False
    ) -> "SecurePath":
        """
        Copy the file/directory into the destination.
        If source is a directory, its whole content is copied recursively.
        Permissions of the copy are limited only to the owner.

        If destination is an existing directory, the copy will be created inside it,
        unless dirs_exist_ok is true and the destination has the same name as this path.

        Otherwise, the copied file/base directory will be renamed to match destination.
        If dirs_exist_ok is false (the default) and dst already exists,
        a FileExistsError is raised. If dirs_exist_ok is true,
        the copying operation will continue if it encounters existing directories,
        and files within the destination tree will be overwritten by corresponding
        files from the src tree.
        """
        self.assert_exists()

        destination = Path(destination)
        if destination.exists():
            if destination.is_dir() and (
                destination.name != self._path.name or self.path.is_file()
            ):
                destination = destination / self._path.name

            if destination.exists():
                if not all([destination.is_dir(), self._path.is_dir(), dirs_exist_ok]):
                    raise FileExistsError(
                        errno.EEXIST, os.strerror(errno.EEXIST), self._path.resolve()
                    )

        def _recursive_check_for_conflicts(src: Path, dst: Path):
            if dst.exists() and not dirs_exist_ok:
                _raise_file_exists_error(dst)
            if dst.is_file() and not src.is_file():
                _raise_not_a_directory_error(dst)
            if dst.is_dir() and not src.is_dir():
                _raise_is_a_directory_error(dst)
            if src.is_dir():
                for child in src.iterdir():
                    _recursive_check_for_conflicts(child, dst / child.name)

        def _recursive_copy(src: SecurePath, dst: SecurePath):
            if src.is_file():
                log.info("Copying file %s into %s", src.path, dst.path)
                if dst.exists():
                    dst.unlink()
                shutil.copyfile(src.path, dst.path)
                dst.restrict_permissions()
            if src.is_dir():
                dst.mkdir(exist_ok=True)
                for child in src.iterdir():
                    _recursive_copy(child, dst / child.name)

        _recursive_check_for_conflicts(self._path, destination)
        _recursive_copy(self, self.__class__(destination))

        return SecurePath(destination)

    def unlink(self, missing_ok=False):
        """
        Remove this file or symbolic link.
        If the path points to a directory, use SecurePath.rmdir() instead.

        Check pathlib.Path.unlink() for details.
        """
        if not self.exists():
            if not missing_ok:
                self.assert_exists()
            return

        self.assert_is_file()
        log.info("Removing file %s", self._path)
        self._path.unlink()

    def rmdir(self, recursive=False, missing_ok=False):
        """
        Remove this directory.
        If the path points to a file, use SecurePath.unlink() instead.

        If path points to a file, NotADirectoryError will be raised.
        If directory does not exist, FileNotFoundError will be raised unless [missing_ok] is True.
        If the directory is not empty, DirectoryNotEmpty will be raised unless [recursive] is True.
        """
        if not self.exists():
            if not missing_ok:
                self.assert_exists()
            return

        self.assert_is_directory()

        if not recursive and any(self._path.iterdir()):
            raise DirectoryIsNotEmptyError(self._path.resolve())

        log.info("Removing directory %s", self._path)
        shutil.rmtree(str(self._path))

    @classmethod
    @contextmanager
    def temporary_directory(cls):
        """
        Creates a temporary directory in the most secure manner possible.
        The directory is readable, writable, and searchable only by the creating user ID.
        Yields SecurePath pointing to the absolute location of created directory.

        Works similarly to tempfile.TemporaryDirectory
        """
        with tempfile.TemporaryDirectory(prefix="snowflake-cli") as tmpdir:
            log.info("Created temporary directory %s", tmpdir)
            yield SecurePath(tmpdir)
            log.info("Removing temporary directory %s", tmpdir)

    def _assert_exists_and_is_file(self) -> None:
        self.assert_exists()
        self.assert_is_file()

    def assert_exists(self) -> None:
        if not self.exists():
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), self._path.resolve()
            )

    def assert_is_file(self) -> None:
        if not self._path.is_file():
            _raise_is_a_directory_error(self._path.resolve())

    def assert_is_directory(self) -> None:
        if not self._path.is_dir():
            _raise_not_a_directory_error(self._path.resolve())

    def _assert_file_size_limit(self, size_limit_in_mb):
        if (
            size_limit_in_mb != UNLIMITED
            and self._path.stat().st_size > size_limit_in_mb * 1024 * 1024
        ):
            raise FileTooLargeError(self._path.resolve(), size_limit_in_mb)

    def rename(self, new_name: Union[str | Path]):
        self._path.rename(new_name)


def _raise_file_exists_error(path: Path):
    raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), path)


def _raise_is_a_directory_error(path: Path):
    raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)


def _raise_not_a_directory_error(path: Path):
    raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), path)
