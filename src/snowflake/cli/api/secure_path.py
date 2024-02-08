import logging
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union

from snowflake.cli.api.exceptions import FileTooLargeError

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
    def parent(self):
        return SecurePath(self._path.parent)

    @property
    def path(self) -> Path:
        return self._path

    def exists(self) -> bool:
        return self._path.exists()

    def chmod(self, permissions_mask: int) -> None:
        log.info(
            "Update permissions of file %s to %s", self._path, oct(permissions_mask)
        )
        # TODO: windows
        self._path.chmod(permissions_mask)

    def touch(self, permissions_mask: int = 0o600, exist_ok: bool = True) -> None:
        """
        Create a file at this given path. For details, check pathlib.Path.touch()
        """
        if not self.exists():
            log.info("Creating file %s", str(self._path))
        self._path.touch(mode=permissions_mask, exist_ok=exist_ok)

    def read_text(self, file_size_limit_kb: int, encoding=None, errors=None) -> str:
        """
        Return the decoded contents of the file as a string, performing additional checks.
        """
        self._assert_exists_and_is_file()
        self._assert_file_size_limit(file_size_limit_kb)
        log.info("Reading file %s", self._path)
        return self._path.read_text(encoding=encoding, errors=errors)

    @contextmanager
    def open(  # noqa: A003
        self,
        mode="r",
        read_file_limit_kb: Optional[int] = None,
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
    ):
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        If the file is opened for reading, provide [read_file_limit_kb] parameter.
        The function check whether the file exists and its size is within the limit.
        """
        opened_for_reading = "r" in mode
        if opened_for_reading:
            assert (
                read_file_limit_kb is not None
            ), "For reading mode ('r') read_file_limit_kb must be provided"
            self._assert_exists_and_is_file()
            self._assert_file_size_limit(read_file_limit_kb)

        if self._path.exists():
            self._assert_is_file()
        else:
            self.touch()  # makes sure permissions of freshly-created file are strict

        log.info("Opening file %s in mode '%s'", self._path, mode)
        with self._path.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        ) as fd:
            yield fd
        log.info("Closing file %s", self._path)

    def copy(self, destination: Union[Path, str]) -> "SecurePath":
        destination = Path(destination)
        if destination.exists():
            if destination.is_dir():
                destination = destination / self._path.name
            if destination.exists():
                raise FileExistsError(destination)

        self._assert_exists()
        if self._path.is_file():
            log.info("Copying file %s into %s", self._path, destination)
            shutil.copyfile(str(self._path), str(destination))
        elif self._path.is_dir():
            log.info("Copying directory %s into %s", self._path, destination)
            shutil.copytree(
                str(self._path),
                str(destination),
                dirs_exist_ok=False,
                copy_function=shutil.copy,
            )
        else:
            raise NotImplementedError

        return SecurePath(destination)

    def _assert_exists_and_is_file(self) -> None:
        self._assert_exists()
        self._assert_is_file()

    def _assert_exists(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(self._path.resolve())

    def _assert_is_file(self) -> None:
        if not self._path.is_file():
            raise IsADirectoryError(self._path.resolve())

    def _assert_file_size_limit(self, size_limit_in_kb):
        if (
            size_limit_in_kb != UNLIMITED
            and self._path.stat().st_size > size_limit_in_kb * 1024
        ):
            raise FileTooLargeError(self._path.resolve(), size_limit_in_kb)
