import logging
import os
from pathlib import Path
from typing import Union

from snowflake.cli.api.exceptions import FileTooLargeError

log = logging.getLogger(__name__)

UNLIMITED = -1


class SecurePath:
    def __init__(self, path: Union[Path, str]):
        self._path = Path(path)

    def chmod(self, mask: int) -> None:
        # TODO: windows
        self._path.chmod(mask)

    def chown(self, user_id: int, group_id: int) -> None:
        os.chown(self._path, user_id, group_id)

    def read_text(self, file_size_limit_kb: int, encoding=None, errors=None) -> str:
        """
        Return the decoded contents of the file as a string, performing additional checks.
        """
        self._assert_exists_and_is_file()
        self._assert_file_size_limit(file_size_limit_kb)
        log.info("Reading file %s", self._path)
        return self._path.read_text(encoding=encoding, errors=errors)

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
