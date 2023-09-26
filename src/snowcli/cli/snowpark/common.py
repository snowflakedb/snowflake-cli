from __future__ import annotations

import sys
from typing import TextIO, Optional, List

import click
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin

if not sys.stdout.closed and sys.stdout.isatty():
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    ORANGE = "\033[38:2:238:76:44m"
    GRAY = "\033[2m"
    ENDC = "\033[0m"
else:
    GREEN = ""
    ORANGE = ""
    BLUE = ""
    GRAY = ""
    ENDC = ""


def _prefix_line(prefix: str, line: str) -> str:
    """
    _prefix_line ensure the prefix is still present even when dealing with return characters
    """
    if "\r" in line:
        line = line.replace("\r", f"\r{prefix}")
    if "\n" in line[:-1]:
        line = line[:-1].replace("\n", f"\n{prefix}") + line[-1:]
    if not line.startswith("\r"):
        line = f"{prefix}{line}"
    return line


def print_log_lines(file: TextIO, name, id, logs):
    prefix = f"{GREEN}{name}/{id}{ENDC} "
    logs = logs[0:-1]
    for log in logs:
        print(_prefix_line(prefix, log + "\n"), file=file, end="", flush=True)


class SnowparkObjectManager(SqlExecutionMixin):
    @property
    def _object_type(self):
        raise NotImplementedError()

    @property
    def _object_execute(self):
        raise NotImplementedError()

    def create(self, *args, **kwargs) -> SnowflakeCursor:
        raise NotImplementedError()

    def execute(self, execution_identifier: str) -> SnowflakeCursor:
        return self._execute_query(f"{self._object_execute} {execution_identifier}")

    def drop(self, identifier: str) -> SnowflakeCursor:
        return self._execute_query(f"drop {self._object_type} {identifier}")

    def show(self, like: Optional[str] = None) -> SnowflakeCursor:
        query = f"show user {self._object_type}s"
        if like:
            query += f" like '{like}'"
        return self._execute_query(query)

    def describe(self, identifier: str) -> SnowflakeCursor:
        return self._execute_query(f"describe {self._object_type} {identifier}")

    @staticmethod
    def artifact_stage_path(identifier: str):
        return (
            identifier.replace(
                "(",
                "",
            )
            .replace(
                ")",
                "",
            )
            .replace(
                " ",
                "_",
            )
            .replace(
                ",",
                "",
            )
            .lower()
        )
