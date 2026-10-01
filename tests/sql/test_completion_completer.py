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

from unittest import mock

from prompt_toolkit.completion import CompleteEvent
from prompt_toolkit.document import Document
from snowflake.cli._plugins.sql.completion.completer import SqlReplCompleter


def _texts(buffer: str, cursor: int | None = None) -> list[str]:
    document = Document(
        buffer, cursor_position=len(buffer) if cursor is None else cursor
    )
    return [
        completion.text
        for completion in SqlReplCompleter().get_completions(
            document, CompleteEvent(completion_requested=True)
        )
    ]


def _completions(buffer: str):
    document = Document(buffer, cursor_position=len(buffer))
    return list(
        SqlReplCompleter().get_completions(
            document, CompleteEvent(completion_requested=True)
        )
    )


def test_keyword_slot_completes_sel_to_select():
    completions = _completions("SEL")
    texts = [completion.text for completion in completions]
    assert "SELECT" in texts
    select = next(c for c in completions if c.text == "SELECT")
    assert select.start_position == -3


def test_keyword_slot_matches_ignore_case_and_inserts_list_token():
    texts = _texts("sel")
    assert "SELECT" in texts
    assert '"SELECT"' not in texts


def test_keyword_slot_ranks_keywords_before_functions():
    texts = _texts("C")
    keyword_hits = [t for t in texts if t in {"CACHE", "CALL", "CASE", "CAST", "CHECK"}]
    function_hits = [t for t in texts if t in {"COUNT", "COALESCE", "CONVERT_TIMEZONE"}]
    assert keyword_hits
    assert function_hits
    assert texts.index(keyword_hits[0]) < texts.index(function_hits[0])


def test_from_empty_prefix_yields_no_keyword_flood():
    assert _texts("FROM ") == []


def test_identifier_prefix_len_one_yields_nothing_when_cold():
    assert _texts("FROM x") == []
    assert _texts("FROM F") == []


def test_identifier_longer_prefix_still_empty_without_catalog():
    assert _texts("FROM xy") == []


def test_column_slot_yields_nothing_when_cold():
    assert _texts("rel.") == []
    assert _texts("SELECT * FROM t WHERE ") == []


def test_type_slot_completes_static_types():
    texts = _texts("CAST(x AS var")
    assert "VARCHAR" in texts
    assert "VARIANT" in texts


def test_none_slot_yields_nothing():
    assert _texts("SELECT 'hello") == []
    assert _texts("!rehash") == []
    assert _texts("SELECT {{ name }}") == []


def test_statement_start_single_letter_still_offers_keywords():
    texts = _texts("F")
    assert "FROM" in texts
    assert _texts("FROM F") == []


def test_completer_swallows_classify_errors():
    completer = SqlReplCompleter()
    with mock.patch(
        "snowflake.cli._plugins.sql.completion.completer.classify",
        side_effect=RuntimeError("boom"),
    ):
        assert (
            list(
                completer.get_completions(
                    Document("SEL", 3), CompleteEvent(completion_requested=True)
                )
            )
            == []
        )
