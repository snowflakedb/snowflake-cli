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

import re
import threading
import time
from dataclasses import dataclass
from typing import Callable

from snowflake.cli._plugins.sql.completion.context import CompletionKind
from snowflake.cli._plugins.sql.completion.introspection import (
    MetadataError,
    MetadataProvider,
)

PRIVILEGE_TTL_SECONDS = 30.0
_SCOPE_KINDS = frozenset(
    {
        CompletionKind.SCHEMA,
        CompletionKind.TABLE,
        CompletionKind.VIEW,
        CompletionKind.COLUMN,
    }
)
_DDL_KINDS = {
    "TABLE": CompletionKind.TABLE,
    "VIEW": CompletionKind.VIEW,
    "SCHEMA": CompletionKind.SCHEMA,
    "DATABASE": CompletionKind.DATABASE,
}


@dataclass(frozen=True)
class SessionScope:
    database: str | None = None
    schema: str | None = None
    primary_role: str | None = None
    session_id: str | None = None


@dataclass(frozen=True)
class _CacheKey:
    kind: CompletionKind
    database: str | None
    schema: str | None
    object_name: str | None
    primary_role: str | None
    session_generation: int


@dataclass(frozen=True)
class _Entry:
    names: tuple[str, ...]
    fetched_prefixes: frozenset[str]


class ObjectCatalog:
    """In-memory object snapshots. No SHOW SQL — the provider is injected.

    Relation lookups with an empty prefix never call the provider; they
    filter a warm snapshot or return nothing. Any nonempty prefix may LIKE.
    Column lookups pass ``allow_empty_prefix=True`` in T9 so one-object
    SHOW COLUMNS can run with an empty column prefix.
    """

    def __init__(
        self,
        provider: MetadataProvider | None,
        scope: SessionScope | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._provider = provider
        self._scope = scope or SessionScope()
        self._clock = clock
        self._generation = 0
        self._snapshots: dict[_CacheKey, _Entry] = {}
        self._negatives: dict[_CacheKey, float] = {}
        self._inflight: set[_CacheKey] = set()
        self._mu = threading.RLock()

    def set_scope(self, scope: SessionScope) -> None:
        with self._mu:
            self._set_scope_locked(scope)

    def _set_scope_locked(self, scope: SessionScope) -> None:
        if (
            self._scope.session_id is not None
            and scope.session_id != self._scope.session_id
        ):
            self._drop_all_locked()
        self._scope = scope

    def drop_all(self) -> None:
        with self._mu:
            self._drop_all_locked()

    def _drop_all_locked(self) -> None:
        self._snapshots.clear()
        self._negatives.clear()
        self._inflight.clear()
        self._generation += 1

    @property
    def generation(self) -> int:
        return self._generation

    def set_provider(self, provider: MetadataProvider | None) -> None:
        self._provider = provider

    def on_success(self, rendered_sql: str) -> None:
        """Patch or drop snapshots from one successful compiled statement."""
        parsed = _parse_success_sql(rendered_sql)
        if parsed is None:
            return
        with self._mu:
            self._on_success_locked(parsed)

    def _on_success_locked(self, parsed) -> None:
        op = parsed[0]
        if op == "drop_all":
            self._drop_all_locked()
            return
        if op == "drop_scope":
            self._drop_kinds(_SCOPE_KINDS)
            return
        if op == "add":
            _, kind, name, database, schema = parsed
            self._add_locked(kind, name, database=database, schema=schema)
            self._drop_columns(kind, name, database, schema)
            return
        if op == "remove":
            _, kind, name, database, schema = parsed
            self._remove_locked(kind, name, database=database, schema=schema)
            self._drop_descendants(kind, name, database, schema)
            return
        if op == "rename":
            _, kind, old_name, new_name, database, schema = parsed
            if not old_name or not new_name:
                self._drop_kinds({kind})
                if kind in {CompletionKind.TABLE, CompletionKind.VIEW}:
                    self._drop_kinds({CompletionKind.COLUMN})
                return
            self._remove_locked(kind, old_name, database=database, schema=schema)
            self._drop_descendants(kind, old_name, database, schema)
            self._add_locked(kind, new_name, database=database, schema=schema)

    def lookup(
        self,
        kind: CompletionKind,
        path: tuple[str, ...] = (),
        prefix: str = "",
        *,
        allow_empty_prefix: bool = False,
    ) -> tuple[str, ...]:
        if self._provider is None:
            return ()
        with self._mu:
            key = self._key(kind, path)
            cached = self._answer(key, prefix)
            if cached is not None:
                return cached
            if not prefix and not allow_empty_prefix:
                return ()
            if self._is_privilege_blocked(key):
                return ()
            if key in self._inflight:
                return ()
            generation = self._generation
            session_id = self._scope.session_id
            self._inflight.add(key)
        return self._fetch(key, kind, path, prefix, generation, session_id)

    def add(
        self,
        kind: CompletionKind,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        with self._mu:
            self._add_locked(kind, name, database=database, schema=schema)

    def _add_locked(
        self,
        kind: CompletionKind,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        key = self._key(kind, self._path_for_patch(kind, database, schema))
        entry = self._snapshots.get(key)
        if entry is None:
            return
        if any(existing.upper() == name.upper() for existing in entry.names):
            return
        self._snapshots[key] = _Entry(entry.names + (name,), entry.fetched_prefixes)

    def remove(
        self,
        kind: CompletionKind,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        with self._mu:
            self._remove_locked(kind, name, database=database, schema=schema)

    def _remove_locked(
        self,
        kind: CompletionKind,
        name: str,
        *,
        database: str | None = None,
        schema: str | None = None,
    ) -> None:
        key = self._key(kind, self._path_for_patch(kind, database, schema))
        entry = self._snapshots.get(key)
        if entry is None:
            return
        self._snapshots[key] = _Entry(
            tuple(
                existing for existing in entry.names if existing.upper() != name.upper()
            ),
            entry.fetched_prefixes,
        )

    def _fetch(
        self,
        key: _CacheKey,
        kind: CompletionKind,
        path: tuple[str, ...],
        prefix: str,
        generation: int,
        session_id: str | None,
    ) -> tuple[str, ...]:
        try:
            provider = self._provider
            if provider is None:
                with self._mu:
                    self._inflight.discard(key)
                return ()
            names = tuple(provider.lookup(kind, path, prefix))
        except MetadataError as error:
            with self._mu:
                self._inflight.discard(key)
                if error.error_class == "privilege":
                    self._negatives[key] = self._clock() + PRIVILEGE_TTL_SECONDS
            return ()
        except Exception:
            with self._mu:
                self._inflight.discard(key)
            raise
        with self._mu:
            self._inflight.discard(key)
            if self._generation != generation or self._scope.session_id != session_id:
                return ()
            previous = self._snapshots.get(key)
            merged = _merge(previous.names if previous else (), names)
            fetched = set(previous.fetched_prefixes if previous else ())
            fetched.add(prefix)
            self._snapshots[key] = _Entry(merged, frozenset(fetched))
            self._negatives.pop(key, None)
            return _filter(names, prefix)

    def _answer(self, key: _CacheKey, prefix: str) -> tuple[str, ...] | None:
        entry = self._snapshots.get(key)
        if entry is None:
            return None
        if prefix == "":
            if "" in entry.fetched_prefixes:
                return entry.names
            return None
        if prefix in entry.fetched_prefixes:
            return _filter(entry.names, prefix)
        for fetched in entry.fetched_prefixes:
            if fetched and prefix.lower().startswith(fetched.lower()):
                return _filter(entry.names, prefix)
        return None

    def _is_privilege_blocked(self, key: _CacheKey) -> bool:
        expiry = self._negatives.get(key)
        if expiry is None:
            return False
        if self._clock() < expiry:
            return True
        del self._negatives[key]
        return False

    def _key(self, kind: CompletionKind, path: tuple[str, ...]) -> _CacheKey:
        database, schema, object_name = self._scope_parts(kind, path)
        return _CacheKey(
            kind=kind,
            database=database,
            schema=schema,
            object_name=object_name,
            primary_role=self._scope.primary_role,
            session_generation=self._generation,
        )

    def _scope_parts(
        self, kind: CompletionKind, path: tuple[str, ...]
    ) -> tuple[str | None, str | None, str | None]:
        if kind is CompletionKind.DATABASE:
            return None, None, None
        if kind is CompletionKind.SCHEMA:
            return (path[0] if path else self._scope.database), None, None
        if kind is CompletionKind.COLUMN:
            if len(path) >= 3:
                return path[0], path[1], path[2]
            if len(path) == 2:
                return self._scope.database, path[0], path[1]
            if len(path) == 1:
                return self._scope.database, self._scope.schema, path[0]
            return self._scope.database, self._scope.schema, None
        if len(path) >= 2:
            return path[0], path[1], None
        if len(path) == 1:
            return path[0], self._scope.schema, None
        return self._scope.database, self._scope.schema, None

    def _path_for_patch(
        self, kind: CompletionKind, database: str | None, schema: str | None
    ) -> tuple[str, ...]:
        if kind is CompletionKind.DATABASE:
            return ()
        db = database if database is not None else self._scope.database
        if kind is CompletionKind.SCHEMA:
            return (db,) if db else ()
        sch = schema if schema is not None else self._scope.schema
        if db and sch:
            return (db, sch)
        if db:
            return (db,)
        return ()

    def _drop_kinds(
        self, kinds: frozenset[CompletionKind] | set[CompletionKind]
    ) -> None:
        self._snapshots = {
            key: entry
            for key, entry in self._snapshots.items()
            if key.kind not in kinds
        }
        self._negatives = {
            key: expiry
            for key, expiry in self._negatives.items()
            if key.kind not in kinds
        }

    def _drop_columns(
        self,
        kind: CompletionKind,
        name: str,
        database: str | None,
        schema: str | None,
    ) -> None:
        if kind not in {CompletionKind.TABLE, CompletionKind.VIEW}:
            return
        db = database if database is not None else self._scope.database
        sch = schema if schema is not None else self._scope.schema

        def stale(key: _CacheKey) -> bool:
            if key.kind is not CompletionKind.COLUMN:
                return False
            if not _ident_eq(key.object_name, name):
                return False
            if db is not None and not _ident_eq(key.database, db):
                return False
            return sch is None or _ident_eq(key.schema, sch)

        self._snapshots = {
            key: entry for key, entry in self._snapshots.items() if not stale(key)
        }
        self._negatives = {
            key: expiry for key, expiry in self._negatives.items() if not stale(key)
        }

    def _drop_descendants(
        self,
        kind: CompletionKind,
        name: str,
        database: str | None,
        schema: str | None = None,
    ) -> None:
        if kind is CompletionKind.DATABASE:

            def stale(key: _CacheKey) -> bool:
                return key.kind in _SCOPE_KINDS and _ident_eq(key.database, name)

        elif kind is CompletionKind.SCHEMA:
            db = database if database is not None else self._scope.database

            def stale(key: _CacheKey) -> bool:
                if key.kind not in {
                    CompletionKind.TABLE,
                    CompletionKind.VIEW,
                    CompletionKind.COLUMN,
                }:
                    return False
                if not _ident_eq(key.schema, name):
                    return False
                return db is None or _ident_eq(key.database, db)

        elif kind in {CompletionKind.TABLE, CompletionKind.VIEW}:
            self._drop_columns(kind, name, database, schema)
            return
        else:
            return
        self._snapshots = {
            key: entry for key, entry in self._snapshots.items() if not stale(key)
        }
        self._negatives = {
            key: expiry for key, expiry in self._negatives.items() if not stale(key)
        }


def _parse_ident(text: str) -> tuple[str | None, str]:
    text = text.lstrip()
    if not text:
        return None, text
    if text[0] == '"':
        chars: list[str] = []
        index = 1
        while index < len(text):
            if text[index] == '"':
                if index + 1 < len(text) and text[index + 1] == '"':
                    chars.append('"')
                    index += 2
                    continue
                return "".join(chars), text[index + 1 :]
            chars.append(text[index])
            index += 1
        return None, text
    match = re.match(r"[A-Za-z_][\w$]*", text)
    if match is None:
        return None, text
    return match.group(0), text[match.end() :]


def _parse_fqn(text: str) -> tuple[list[str], str]:
    parts: list[str] = []
    rest = text
    while True:
        ident, rest = _parse_ident(rest)
        if ident is None:
            break
        parts.append(ident)
        stripped = rest.lstrip()
        if stripped.startswith("."):
            rest = stripped[1:]
            continue
        rest = stripped
        break
    return parts, rest


def _object_parts(
    kind: CompletionKind, parts: list[str]
) -> tuple[str | None, str | None, str | None]:
    if not parts:
        return None, None, None
    if kind is CompletionKind.DATABASE:
        return parts[-1], None, None
    if kind is CompletionKind.SCHEMA:
        if len(parts) >= 2:
            return parts[-1], parts[-2], None
        return parts[-1], None, None
    if len(parts) >= 3:
        return parts[-1], parts[-3], parts[-2]
    if len(parts) == 2:
        return parts[-1], None, parts[0]
    return parts[-1], None, None


def _strip_leading_comments(text: str) -> str:
    while True:
        stripped = text.lstrip()
        if stripped.startswith("--"):
            newline = stripped.find("\n")
            if newline == -1:
                return ""
            text = stripped[newline + 1 :]
            continue
        if stripped.startswith("/*"):
            end = stripped.find("*/")
            if end == -1:
                return ""
            text = stripped[end + 2 :]
            continue
        return stripped


def _parse_success_sql(rendered_sql: str):
    text = _strip_leading_comments(rendered_sql)
    if not text:
        return None
    if text.endswith(";"):
        text = text[:-1].rstrip()
    upper = text.upper()
    if re.match(r"USE\s+ROLE\b", upper) or re.match(
        r"USE\s+SECONDARY\s+ROLES\b", upper
    ):
        return ("drop_all",)
    if re.match(r"USE\s+WAREHOUSE\b", upper):
        return None
    if re.match(r"USE\b", upper):
        return ("drop_scope",)
    created = re.match(
        r"CREATE\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?"
        r"(?:(?:LOCAL|GLOBAL)\s+)?"
        r"(?:(?:TEMP(?:ORARY)?|VOLATILE|TRANSIENT|ICEBERG|HYBRID|EXTERNAL|DYNAMIC)\s+)?"
        r"(TABLE|VIEW|SCHEMA|DATABASE)\s+(?:IF\s+NOT\s+EXISTS\s+)?",
        text,
        re.I,
    )
    if created:
        kind = _DDL_KINDS[created.group(1).upper()]
        name, database, schema = _object_parts(
            kind, _parse_fqn(text[created.end() :])[0]
        )
        if name is None:
            return None
        return ("add", kind, name, database, schema)
    dropped = re.match(
        r"(UN)?DROP\s+(TABLE|VIEW|SCHEMA|DATABASE)\s+(?:IF\s+EXISTS\s+)?",
        text,
        re.I,
    )
    if dropped:
        kind = _DDL_KINDS[dropped.group(2).upper()]
        name, database, schema = _object_parts(
            kind, _parse_fqn(text[dropped.end() :])[0]
        )
        if name is None:
            return None
        return (
            "add" if dropped.group(1) else "remove",
            kind,
            name,
            database,
            schema,
        )
    renamed = re.match(r"ALTER\s+(TABLE|VIEW|SCHEMA|DATABASE)\s+", text, re.I)
    if renamed:
        kind = _DDL_KINDS[renamed.group(1).upper()]
        old_parts, after_old = _parse_fqn(text[renamed.end() :])
        rename_to = re.match(r"RENAME\s+TO\s+", after_old, re.I)
        if rename_to is None or not old_parts:
            return ("rename", kind, None, None, None, None)
        new_parts, _ = _parse_fqn(after_old[rename_to.end() :])
        old_name, database, schema = _object_parts(kind, old_parts)
        new_name, new_database, new_schema = _object_parts(kind, new_parts)
        return (
            "rename",
            kind,
            old_name,
            new_name,
            database or new_database,
            schema or new_schema,
        )
    return None


def _ident_eq(left: str | None, right: str | None) -> bool:
    if left is None or right is None:
        return False
    return left.upper() == right.upper()


def _filter(names: tuple[str, ...], prefix: str) -> tuple[str, ...]:
    if not prefix:
        return names
    needle = prefix.lower()
    return tuple(name for name in names if name.lower().startswith(needle))


def _merge(existing: tuple[str, ...], incoming: tuple[str, ...]) -> tuple[str, ...]:
    seen = {name.upper() for name in existing}
    merged = list(existing)
    for name in incoming:
        if name.upper() not in seen:
            merged.append(name)
            seen.add(name.upper())
    return tuple(merged)
