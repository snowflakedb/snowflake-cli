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

from prompt_toolkit.buffer import Buffer
from prompt_toolkit.completion import get_common_complete_suffix

log = getLogger(__name__)


class CompletionAcceptanceTracker:
    """Count one accepted completion when it is committed, not while browsing."""

    def __init__(self, buffer: Buffer, on_accepted) -> None:
        self._selected = False
        self._in_go_to = False
        self._on_accepted = on_accepted

        original_insert = buffer.insert_text
        original_go_to = buffer.go_to_completion
        original_apply = buffer.apply_completion

        def insert_text(data, *args, **kwargs):
            state = buffer.complete_state
            unique_hit = False
            if (
                state is not None
                and state.complete_index is None
                and len(state.completions) == 1
            ):
                suffix = get_common_complete_suffix(
                    state.original_document, state.completions
                )
                unique_hit = data == suffix
            result = original_insert(data, *args, **kwargs)
            if unique_hit:
                self._emit()
            return result

        def go_to_completion(index):
            self._in_go_to = True
            try:
                result = original_go_to(index)
                self._selected = index is not None
                return result
            finally:
                self._in_go_to = False

        def apply_completion(*args, **kwargs):
            self._selected = False
            result = original_apply(*args, **kwargs)
            self._emit()
            return result

        buffer.insert_text = insert_text
        buffer.go_to_completion = go_to_completion
        buffer.apply_completion = apply_completion
        buffer.on_text_changed += self._on_edit
        buffer.on_cursor_position_changed += self._on_edit

    def reset(self) -> None:
        self._selected = False

    def commit_on_submit(self) -> None:
        selected = self._selected
        self._selected = False
        if selected:
            self._emit()

    def _on_edit(self, _buffer: Buffer) -> None:
        if self._selected and not self._in_go_to:
            self._selected = False
            self._emit()

    def _emit(self) -> None:
        try:
            self._on_accepted()
        except Exception:
            log.debug("completion accept telemetry failed", exc_info=True)
