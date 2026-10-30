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

from contextlib import contextmanager
from logging import getLogger
from threading import Lock
from types import SimpleNamespace
from typing import Callable, Iterator, Protocol, Sequence

from snowflake.cli._plugins.sql.completion.context import CompletionKind
from snowflake.cli.api.project.util import (
    escape_like_pattern,
    to_quoted_identifier,
    to_string_literal,
    unquote_identifier,
)

log = getLogger(__name__)

SHOW_TIMEOUT_SECONDS = 1

_TABLE_KINDS = frozenset({"TABLE", "TRANSIENT", "TEMPORARY", "ICEBERG"})
_VIEW_KINDS = frozenset({"VIEW", "MATERIALIZED VIEW"})
_PRIVILEGE_ERRNOS = frozenset({2003, 2043})
_TIMEOUT_ERRNOS = frozenset({604})


class MetadataError(Exception):
    """Provider failure classified for cache policy (not a payload)."""

    def __init__(self, error_class: str):
        super().__init__(error_class)
        self.error_class = error_class


class MetadataProvider(Protocol):
    def lookup(
        self,
        kind: CompletionKind,
        path: tuple[str, ...],
        prefix: str,
    ) -> Sequence[str]:
        """Return catalog names for ``kind``/``path``/``prefix``.

        Raise :class:`MetadataError` with ``privilege``, ``timeout``,
        ``network``, or ``busy``. Empty sequence is a successful empty SHOW.
        """


def build_show_sql(
    kind: CompletionKind,
    path: tuple[str, ...],
    prefix: str,
) -> str | None:
    """Return scoped SHOW SQL, or None when the lookup must not run."""
    if kind is CompletionKind.COLUMN:
        fqn = _column_fqn(path)
        if fqn is None:
            return None
        return f"SHOW COLUMNS IN {fqn}"
    if not prefix:
        return None
    like = _like_prefix(prefix)
    if kind is CompletionKind.DATABASE:
        return f"SHOW DATABASES LIKE {like}"
    if kind is CompletionKind.SCHEMA:
        if not path:
            return None
        return f"SHOW SCHEMAS LIKE {like} IN DATABASE {_qid(path[0])}"
    if kind in (CompletionKind.TABLE, CompletionKind.VIEW):
        if len(path) < 2:
            return None
        return f"SHOW OBJECTS LIKE {like} IN SCHEMA {_qid(path[0])}.{_qid(path[1])}"
    return None


def _like_prefix(prefix: str) -> str:
    return to_string_literal(escape_like_pattern(unquote_identifier(prefix)) + "%")


def _qid(name: str) -> str:
    """Quote a resolved name verbatim; the classifier already uppercased unquoted input."""
    return to_quoted_identifier(name)


def _column_fqn(path: tuple[str, ...]) -> str | None:
    if not path:
        return None
    return ".".join(_qid(part) for part in path)


class SnowflakeMetadataProvider:
    """Dedicated-cursor SHOW adapter. Never steals the user print cursor."""

    def __init__(
        self,
        connection_factory: Callable,
        is_busy: Callable[[], bool] | None = None,
        generation: Callable[[], int] | None = None,
        connection_lock: Lock | None = None,
    ):
        self._connection_factory = connection_factory
        self._is_busy = is_busy or (lambda: False)
        self._generation = generation
        self._connection_lock = connection_lock
        self._last_sql: str | None = None
        self._last_rows: list = []
        self._last_description = None
        self._last_generation: int | None = None

    @contextmanager
    def _hold_connection(self) -> Iterator[bool]:
        lock = self._connection_lock
        if lock is None:
            yield True
            return
        if not lock.acquire(blocking=False):
            yield False
            return
        try:
            yield True
        finally:
            lock.release()

    def lookup(
        self,
        kind: CompletionKind,
        path: tuple[str, ...],
        prefix: str,
    ) -> Sequence[str]:
        if self._is_busy():
            raise MetadataError("busy")
        with self._hold_connection() as held:
            if not held:
                raise MetadataError("busy")
            return self._lookup_locked(kind, path, prefix)

    def _lookup_locked(
        self,
        kind: CompletionKind,
        path: tuple[str, ...],
        prefix: str,
    ) -> Sequence[str]:
        cursor = None
        cached = False
        rows: list = []
        description = None
        sql: str | None = None
        gen_fn = self._generation
        generation = gen_fn() if gen_fn is not None else None
        try:
            resolved = self._resolve_path(kind, path)
            sql = build_show_sql(kind, resolved, prefix)
            if sql is None:
                return ()
            if sql == self._last_sql and generation == self._last_generation:
                cached = True
                rows = self._last_rows
                description = self._last_description
            else:
                connection = self._connection_factory()
                cursor = connection.cursor()
                cursor.execute(sql, timeout=SHOW_TIMEOUT_SECONDS)
                rows = list(cursor)
                description = getattr(cursor, "description", None)
        except MetadataError:
            raise
        except Exception as error:
            raise _classify_error(error) from error
        finally:
            close = getattr(cursor, "close", None) if cursor is not None else None
            if close is not None:
                close()
        if sql is None:
            return ()
        if not cached:
            if generation is not None and gen_fn is not None and gen_fn() != generation:
                return ()
        names = _extract_names(kind, rows, SimpleNamespace(description=description))
        if not cached:
            self._last_sql = sql
            self._last_rows = rows
            self._last_description = description
            self._last_generation = generation
        return names

    def _resolve_path(
        self, kind: CompletionKind, path: tuple[str, ...]
    ) -> tuple[str, ...]:
        if kind is CompletionKind.DATABASE:
            return ()
        if path:
            if kind is CompletionKind.SCHEMA:
                return path[:1]
            if kind in (CompletionKind.TABLE, CompletionKind.VIEW) and len(path) >= 2:
                return path[:2]
            if kind is CompletionKind.COLUMN:
                return path
        connection = self._connection_factory()
        database = getattr(connection, "database", None)
        schema = getattr(connection, "schema", None)
        if kind is CompletionKind.SCHEMA:
            return (database,) if database else ()
        if kind in (CompletionKind.TABLE, CompletionKind.VIEW):
            if path:
                database = path[0]
            if database and schema:
                return (database, schema)
            return ()
        if kind is CompletionKind.COLUMN:
            return path
        return path


def _extract_names(kind: CompletionKind, rows, cursor) -> tuple[str, ...]:
    folded = _fold_rows(rows, cursor)
    if kind is CompletionKind.COLUMN:
        names = tuple(row["column_name"] for row in folded if row.get("column_name"))
        return names
    wanted = None
    if kind is CompletionKind.TABLE:
        wanted = _TABLE_KINDS
    elif kind is CompletionKind.VIEW:
        wanted = _VIEW_KINDS
    collected: list[str] = []
    for row in folded:
        name = row.get("name")
        if not name:
            continue
        if wanted is not None:
            row_kind = str(row.get("kind") or "").upper()
            if row_kind not in wanted:
                continue
        collected.append(name)
    return tuple(collected)


def _fold_rows(rows, cursor) -> list[dict]:
    if not rows:
        return []
    first = rows[0]
    if isinstance(first, dict):
        return [{str(key).lower(): value for key, value in row.items()} for row in rows]
    headers = [
        str(column[0]).lower()
        for column in (getattr(cursor, "description", None) or ())
    ]
    return [dict(zip(headers, row)) for row in rows]


def _classify_error(error: Exception) -> MetadataError:
    errno = getattr(error, "errno", None)
    message = str(error)
    if errno in _TIMEOUT_ERRNOS or "000604" in message:
        return MetadataError("timeout")
    if errno in _PRIVILEGE_ERRNOS or "002003" in message or "002043" in message:
        return MetadataError("privilege")
    return MetadataError("network")
