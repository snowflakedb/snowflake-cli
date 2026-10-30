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

from logging import getLogger
from typing import Iterable, Iterator

from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.document import Document
from snowflake.cli._plugins.sql.completion.context import (
    CompletionKind,
    CompletionSlot,
    SuggestContext,
    classify,
)
from snowflake.cli._plugins.sql.completion.identifiers import format_catalog_name
from snowflake.cli._plugins.sql.completion.telemetry import (
    emit_completion,
    on_completion_accepted,
)
from snowflake.cli._plugins.sql.lexer.functions import FUNCTIONS
from snowflake.cli._plugins.sql.lexer.keywords import KEYWORDS
from snowflake.cli._plugins.sql.lexer.types import TYPES

log = getLogger(__name__)

_IDENTIFIER_SLOTS = frozenset({CompletionSlot.OBJECT, CompletionSlot.COLUMN})


class SqlReplCompleter(Completer):
    """Tab-only ranking completer for interactive ``snow sql``.

    Identifier slots ask the catalog on nonempty prefixes (and on column
    slots with a one-object path). Keyword and type slots use the static
    lexer lists. Inserted object names go through ``format_catalog_name``.
    """

    def __init__(self, catalog=None):
        self._catalog = catalog
        self._last_slot = "none"
        self._last_kind = "none"

    def get_completions(
        self, document: Document, complete_event
    ) -> Iterable[Completion]:
        try:
            ctx = classify(document.text, document.cursor_position)
        except Exception:
            _emit_internal_error("none", "none")
            return
        self._last_slot = ctx.slot.value
        self._last_kind = _kind(ctx)
        shown = False
        try:
            for completion in _completions_for(ctx, self._catalog):
                shown = True
                yield completion
        except Exception:
            _emit_internal_error(ctx.slot.value, _kind(ctx))
            return
        outcome = "shown" if shown else "empty"
        _safe_emit(
            event="requested",
            slot=ctx.slot.value,
            kind=_kind(ctx),
            cache="none",
            outcome=outcome,
        )
        if ctx.slot in _IDENTIFIER_SLOTS:
            _safe_emit(
                event="lookup",
                slot=ctx.slot.value,
                kind=_kind(ctx),
                cache="none",
                outcome=outcome,
            )

    def notify_accepted(self) -> None:
        on_completion_accepted(slot=self._last_slot, kind=self._last_kind)


def _safe_emit(**kwargs) -> None:
    try:
        emit_completion(**kwargs)
    except Exception:
        log.debug("REPL completion telemetry failed", exc_info=True)


def _emit_internal_error(slot: str, kind: str) -> None:
    log.debug("REPL completion failed", exc_info=True)
    _safe_emit(
        event="requested",
        slot=slot,
        kind=kind,
        cache="none",
        outcome="error",
        error_class="internal",
    )


def _kind(ctx: SuggestContext) -> str:
    return ctx.kinds[0].value if ctx.kinds else "none"


def _completions_for(ctx: SuggestContext, catalog) -> Iterator[Completion]:
    if ctx.slot is CompletionSlot.NONE:
        return
    if ctx.slot in _IDENTIFIER_SLOTS:
        yield from _catalog_completions(ctx, catalog)
        return
    if ctx.slot is CompletionSlot.TYPE:
        yield from _match(TYPES, ctx.prefix)
        return

    seen: set[str] = set()
    for completion in _match(KEYWORDS, ctx.prefix):
        seen.add(completion.text)
        yield completion
    for completion in _match(FUNCTIONS, ctx.prefix):
        if completion.text not in seen:
            yield completion


_CATALOG_KINDS = frozenset(
    {
        CompletionKind.DATABASE,
        CompletionKind.SCHEMA,
        CompletionKind.TABLE,
        CompletionKind.VIEW,
        CompletionKind.COLUMN,
    }
)


def _catalog_completions(ctx: SuggestContext, catalog) -> Iterator[Completion]:
    if catalog is None:
        return
    path = ctx.path
    if ctx.slot is CompletionSlot.COLUMN and not path and ctx.simple_relation:
        path = ctx.simple_relation
    allow_empty = ctx.slot is CompletionSlot.COLUMN
    seen: set[str] = set()
    for kind in ctx.kinds:
        if kind not in _CATALOG_KINDS:
            continue
        names = catalog.lookup(kind, path, ctx.prefix, allow_empty_prefix=allow_empty)
        for name in names:
            insert = (
                name.replace('"', '""') if ctx.quoted else format_catalog_name(name)
            )
            key = insert.upper()
            if key in seen:
                continue
            seen.add(key)
            yield Completion(insert, start_position=-len(ctx.prefix))


def _match(vocab: tuple[str, ...], prefix: str) -> Iterator[Completion]:
    needle = prefix.lower()
    start_position = -len(prefix)
    for word in vocab:
        if word.lower().startswith(needle):
            yield Completion(word, start_position=start_position)
