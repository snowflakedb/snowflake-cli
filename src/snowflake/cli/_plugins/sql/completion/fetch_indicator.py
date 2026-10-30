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

import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator, Sequence

from prompt_toolkit.formatted_text import StyleAndTextTuples
from snowflake.cli._plugins.sql.completion.context import CompletionKind
from snowflake.cli._plugins.sql.completion.introspection import MetadataProvider

_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
_TICK_SECONDS = 0.1
_STYLE = "fg:ansibrightblack"


class FetchIndicator:
    """Spinner text shown while a completion SHOW runs.

    ``invalidate`` must be thread-safe: lookups run in the completer thread
    and the ticker redraws the prompt from its own thread.
    """

    def __init__(
        self,
        invalidate: Callable[[], None],
        clock: Callable[[], float] = time.monotonic,
    ):
        self._invalidate = invalidate
        self._clock = clock
        self._running = 0
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        return self._running > 0

    def text(self) -> StyleAndTextTuples:
        if not self.is_running:
            return []
        frame = _FRAMES[int(self._clock() / _TICK_SECONDS) % len(_FRAMES)]
        return [(_STYLE, f"{frame} fetching names")]

    @contextmanager
    def running(self) -> Iterator[None]:
        done = threading.Event()
        with self._lock:
            self._running += 1
        self._invalidate()
        threading.Thread(target=self._tick, args=(done,), daemon=True).start()
        try:
            yield
        finally:
            done.set()
            with self._lock:
                self._running -= 1
            self._invalidate()

    def _tick(self, done: threading.Event) -> None:
        while not done.wait(_TICK_SECONDS):
            self._invalidate()


class IndicatingProvider:
    """Runs every provider lookup under a :class:`FetchIndicator`."""

    def __init__(self, provider: MetadataProvider, indicator: FetchIndicator):
        self._provider = provider
        self._indicator = indicator

    def lookup(
        self,
        kind: CompletionKind,
        path: tuple[str, ...],
        prefix: str,
    ) -> Sequence[str]:
        with self._indicator.running():
            return tuple(self._provider.lookup(kind, path, prefix))
