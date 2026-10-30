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


def test_from_nonempty_prefix_uses_catalog_and_format():
    from snowflake.cli._plugins.sql.completion.catalog import (
        ObjectCatalog,
        SessionScope,
    )
    from snowflake.cli._plugins.sql.completion.context import CompletionKind

    class Provider:
        def __init__(self):
            self.calls = []

        def lookup(self, kind, path, prefix):
            self.calls.append((kind, path, prefix))
            if kind is CompletionKind.TABLE:
                return ["T_ONE", "T_mixed"]
            return []

    catalog = ObjectCatalog(
        Provider(),
        scope=SessionScope(database="DB", schema="SCH", session_id="s1"),
    )
    document = Document("FROM t", cursor_position=6)
    texts = [
        c.text
        for c in SqlReplCompleter(catalog=catalog).get_completions(
            document, CompleteEvent(completion_requested=True)
        )
    ]
    assert "T_ONE" in texts
    assert '"T_mixed"' in texts


def test_from_empty_prefix_does_not_call_catalog_provider():
    from snowflake.cli._plugins.sql.completion.catalog import (
        ObjectCatalog,
        SessionScope,
    )

    class Provider:
        def __init__(self):
            self.calls = []

        def lookup(self, kind, path, prefix):
            self.calls.append((kind, path, prefix))
            return ["SHOULD_NOT"]

    provider = Provider()
    catalog = ObjectCatalog(
        provider,
        scope=SessionScope(database="DB", schema="SCH", session_id="s1"),
    )
    document = Document("FROM ", cursor_position=5)
    texts = [
        c.text
        for c in SqlReplCompleter(catalog=catalog).get_completions(
            document, CompleteEvent(completion_requested=True)
        )
    ]
    assert texts == []
    assert provider.calls == []


def test_column_slot_empty_prefix_uses_catalog():
    from snowflake.cli._plugins.sql.completion.catalog import (
        ObjectCatalog,
        SessionScope,
    )
    from snowflake.cli._plugins.sql.completion.context import CompletionKind

    class Provider:
        def __init__(self):
            self.calls = []

        def lookup(self, kind, path, prefix):
            self.calls.append((kind, path, prefix))
            if kind is CompletionKind.COLUMN:
                return ["COL_A", "col mixed"]
            return []

    provider = Provider()
    catalog = ObjectCatalog(
        provider,
        scope=SessionScope(database="DB", schema="SCH", session_id="s1"),
    )
    document = Document("rel.", cursor_position=4)
    texts = [
        completion.text
        for completion in SqlReplCompleter(catalog=catalog).get_completions(
            document, CompleteEvent(completion_requested=True)
        )
    ]
    assert "COL_A" in texts
    assert '"col mixed"' in texts
    assert any(kind is CompletionKind.COLUMN for kind, _path, _prefix in provider.calls)


def test_unqualified_select_columns_use_simple_relation():
    from snowflake.cli._plugins.sql.completion.catalog import (
        ObjectCatalog,
        SessionScope,
    )
    from snowflake.cli._plugins.sql.completion.context import CompletionKind

    class Provider:
        def __init__(self):
            self.calls = []

        def lookup(self, kind, path, prefix):
            self.calls.append((kind, path, prefix))
            return ["ID"] if kind is CompletionKind.COLUMN else []

    provider = Provider()
    catalog = ObjectCatalog(
        provider,
        scope=SessionScope(database="DB", schema="SCH", session_id="s1"),
    )
    document = Document("SELECT  FROM only_rel", cursor_position=7)
    texts = [
        completion.text
        for completion in SqlReplCompleter(catalog=catalog).get_completions(
            document, CompleteEvent(completion_requested=True)
        )
    ]
    assert "ID" in texts
    column_calls = [call for call in provider.calls if call[0] is CompletionKind.COLUMN]
    assert column_calls
    assert column_calls[0][1] == ("ONLY_REL",)


def test_unqualified_columns_pass_fqn_as_identifier_segments():
    from snowflake.cli._plugins.sql.completion.catalog import (
        ObjectCatalog,
        SessionScope,
    )
    from snowflake.cli._plugins.sql.completion.context import CompletionKind

    class Provider:
        def __init__(self):
            self.calls = []

        def lookup(self, kind, path, prefix):
            self.calls.append((kind, path, prefix))
            return ["ID"] if kind is CompletionKind.COLUMN else []

    provider = Provider()
    catalog = ObjectCatalog(
        provider,
        scope=SessionScope(database="DB", schema="SCH", session_id="s1"),
    )
    document = Document("SELECT  FROM db.sch.t", cursor_position=7)
    texts = [
        completion.text
        for completion in SqlReplCompleter(catalog=catalog).get_completions(
            document, CompleteEvent(completion_requested=True)
        )
    ]
    assert "ID" in texts
    column_calls = [call for call in provider.calls if call[0] is CompletionKind.COLUMN]
    assert column_calls[0][1] == ("DB", "SCH", "T")


def test_next_tab_retries_after_timeout():
    from snowflake.cli._plugins.sql.completion.catalog import (
        ObjectCatalog,
        SessionScope,
    )
    from snowflake.cli._plugins.sql.completion.introspection import MetadataError

    class Provider:
        def __init__(self):
            self.calls = 0

        def lookup(self, kind, path, prefix):
            self.calls += 1
            raise MetadataError("timeout")

    provider = Provider()
    catalog = ObjectCatalog(
        provider,
        scope=SessionScope(database="DB", schema="SCH", session_id="s1"),
    )
    completer = SqlReplCompleter(catalog=catalog)
    document = Document("FROM t", cursor_position=6)
    event = CompleteEvent(completion_requested=True)
    assert list(completer.get_completions(document, event)) == []
    after_first = provider.calls
    assert after_first >= 1
    assert list(completer.get_completions(document, event)) == []
    assert provider.calls > after_first


def test_catalog_completions_yield_before_later_kinds():
    from snowflake.cli._plugins.sql.completion.context import CompletionKind

    seen: list = []

    class Catalog:
        def lookup(self, kind, path, prefix, allow_empty_prefix=False):
            seen.append(kind)
            return ["T1"] if kind is CompletionKind.TABLE else []

    document = Document("FROM db.sch.t", cursor_position=len("FROM db.sch.t"))
    generator = SqlReplCompleter(catalog=Catalog()).get_completions(
        document, CompleteEvent(completion_requested=True)
    )
    first = next(generator)
    assert first.text == "T1"
    assert seen == [CompletionKind.TABLE]
