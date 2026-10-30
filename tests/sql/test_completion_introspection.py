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

import pytest
from snowflake.cli._plugins.sql.completion.context import (
    CompletionKind,
    CompletionSlot,
    classify,
)
from snowflake.cli._plugins.sql.completion.introspection import (
    SHOW_TIMEOUT_SECONDS,
    MetadataError,
    SnowflakeMetadataProvider,
    build_show_sql,
)


class FakeCursor:
    def __init__(self, rows=None, description=None, error=None):
        self.rows = list(rows or [])
        self.description = description
        self.error = error
        self.closed = False
        self.execute_calls: list[tuple] = []

    def execute(self, sql, timeout=None):
        self.execute_calls.append((sql, timeout))
        if self.error is not None:
            raise self.error
        return self

    def __iter__(self):
        return iter(self.rows)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor, database="DB", schema="SCH"):
        self._cursor = cursor
        self.database = database
        self.schema = schema

    def cursor(self):
        return self._cursor


def _provider(cursor, *, busy=False, generation=None, database="DB", schema="SCH"):
    conn = FakeConnection(cursor, database=database, schema=schema)
    return SnowflakeMetadataProvider(
        connection_factory=lambda: conn,
        is_busy=lambda: busy,
        generation=generation,
    )


def test_build_show_sql_escapes_like_wildcards_and_quotes():
    sql = build_show_sql(CompletionKind.DATABASE, (), "O'B_r%")
    assert sql is not None
    assert "O'B_r%" not in sql
    assert "LIKE" in sql.upper()
    assert "SHOW DATABASES" in sql.upper()
    assert "SHOW USERS" not in sql.upper()


def test_build_show_sql_schema_and_objects_are_scoped():
    schema_sql = build_show_sql(CompletionKind.SCHEMA, ("MY_DB",), "s")
    assert schema_sql is not None
    assert "SHOW SCHEMAS" in schema_sql.upper()
    assert "LIKE" in schema_sql.upper()
    assert "IN DATABASE" in schema_sql.upper()

    objects_sql = build_show_sql(CompletionKind.TABLE, ("MY_DB", "MY_SCH"), "t")
    assert objects_sql is not None
    assert "SHOW OBJECTS" in objects_sql.upper()
    assert "LIKE" in objects_sql.upper()
    assert "IN SCHEMA" in objects_sql.upper()
    assert objects_sql.upper().index("LIKE") < objects_sql.upper().index("IN SCHEMA")


def test_empty_relation_prefix_builds_no_sql():
    assert build_show_sql(CompletionKind.DATABASE, (), "") is None
    assert build_show_sql(CompletionKind.TABLE, ("DB", "SCH"), "") is None
    assert build_show_sql(CompletionKind.SCHEMA, ("DB",), "") is None


def test_columns_sql_is_one_object_in_table():
    sql = build_show_sql(CompletionKind.COLUMN, ("DB", "SCH", "REL"), "")
    assert sql is not None
    assert sql.upper().startswith("SHOW COLUMNS IN ")
    assert "IN TABLE" not in sql.upper()
    assert "IN DATABASE" not in sql.upper()
    assert "IN SCHEMA" not in sql.upper()


def test_provider_empty_prefix_does_not_execute():
    cursor = FakeCursor()
    provider = _provider(cursor)
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "") == ()
    assert cursor.execute_calls == []


def test_provider_skips_when_user_sql_in_flight():
    cursor = FakeCursor(rows=[("ALPHA", "TABLE")], description=(("name",), ("kind",)))
    provider = _provider(cursor, busy=True)
    try:
        provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "A")
    except MetadataError as classified:
        assert classified.error_class == "busy"
    else:
        raise AssertionError("expected MetadataError")
    assert cursor.execute_calls == []


def test_provider_uses_timeout_one_and_folds_headers():
    cursor = FakeCursor(
        rows=[("Alpha", "TABLE"), ("BETA", "VIEW")],
        description=(("NAME",), ("KIND",)),
    )
    provider = _provider(cursor)
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "A") == ("Alpha",)
    assert cursor.execute_calls[0][1] == SHOW_TIMEOUT_SECONDS
    assert SHOW_TIMEOUT_SECONDS == 1
    assert cursor.closed


def test_provider_keeps_table_transient_and_view_kinds():
    cursor = FakeCursor(
        rows=[
            ("T1", "TABLE"),
            ("T2", "TRANSIENT"),
            ("T3", "TEMPORARY"),
            ("T4", "ICEBERG"),
            ("V1", "VIEW"),
            ("V2", "MATERIALIZED VIEW"),
        ],
        description=(("name",), ("kind",)),
    )
    provider = _provider(cursor)
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "T") == (
        "T1",
        "T2",
        "T3",
        "T4",
    )
    cursor2 = FakeCursor(
        rows=[("T1", "TABLE"), ("V1", "VIEW"), ("V2", "MATERIALIZED VIEW")],
        description=(("name",), ("kind",)),
    )
    provider2 = _provider(cursor2)
    assert provider2.lookup(CompletionKind.VIEW, ("DB", "SCH"), "V") == ("V1", "V2")


def test_generation_change_discards_late_rows():
    generation = {"n": 1}

    class BumpingCursor(FakeCursor):
        def execute(self, sql, timeout=None):
            generation["n"] += 1
            return super().execute(sql, timeout=timeout)

    cursor = BumpingCursor(
        rows=[("STALE", "TABLE")],
        description=(("name",), ("kind",)),
    )
    provider = _provider(cursor, generation=lambda: generation["n"])
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "S") == ()
    assert cursor.closed


def test_timeout_error_is_classified():
    error = RuntimeError("000604: timeout")
    error.errno = 604
    cursor = FakeCursor(error=error)
    provider = _provider(cursor)
    try:
        provider.lookup(CompletionKind.DATABASE, (), "A")
    except MetadataError as classified:
        assert classified.error_class == "timeout"
    else:
        raise AssertionError("expected MetadataError")


def test_never_emits_show_users_or_unscoped_schema_show():
    for kind, path, prefix in (
        (CompletionKind.DATABASE, (), "a"),
        (CompletionKind.SCHEMA, ("DB",), "a"),
        (CompletionKind.TABLE, ("DB", "SCH"), "a"),
        (CompletionKind.VIEW, ("DB", "SCH"), "a"),
        (CompletionKind.COLUMN, ("DB", "SCH", "T"), ""),
    ):
        sql = build_show_sql(kind, path, prefix)
        assert sql is not None
        assert "SHOW USERS" not in sql.upper()
        if "IN SCHEMA" in sql.upper():
            assert "LIKE" in sql.upper()


def test_columns_show_omits_table_and_view_keyword():
    sql = build_show_sql(CompletionKind.COLUMN, ("DB", "SCH", "MY_VIEW"), "c")
    assert sql is not None
    assert sql.upper().startswith("SHOW COLUMNS IN ")
    assert "IN TABLE" not in sql.upper()
    assert "IN VIEW" not in sql.upper()
    assert "IN DATABASE" not in sql.upper()
    assert "IN SCHEMA" not in sql.upper()


def test_provider_columns_empty_prefix_executes_one_object_show():
    cursor = FakeCursor(
        rows=[("COL_A",), ("COL_B",)],
        description=(("COLUMN_NAME",),),
    )
    provider = _provider(cursor)
    assert provider.lookup(CompletionKind.COLUMN, ("DB", "SCH", "REL"), "") == (
        "COL_A",
        "COL_B",
    )
    sql = cursor.execute_calls[0][0].upper()
    assert sql.startswith("SHOW COLUMNS IN ")
    assert "IN TABLE" not in sql
    assert "IN DATABASE" not in sql
    assert "IN SCHEMA" not in sql
    assert cursor.execute_calls[0][1] == 1


def test_provider_columns_without_fqn_does_not_execute():
    cursor = FakeCursor()
    provider = _provider(cursor)
    assert provider.lookup(CompletionKind.COLUMN, (), "") == ()
    assert cursor.execute_calls == []


def test_columns_sql_quotes_identifier_segments_not_joined_name():
    sql = build_show_sql(CompletionKind.COLUMN, ("DB", "SCH", "T"), "")
    assert sql is not None
    assert '"DB.SCH.T"' not in sql
    assert sql.count(".") == 2
    assert '"DB"."SCH"."T"' in sql

    dotted_name_sql = build_show_sql(CompletionKind.COLUMN, ("db.sch.t",), "")
    assert dotted_name_sql is not None
    assert '"db.sch.t"' in dotted_name_sql
    assert '"DB"."SCH"."T"' not in dotted_name_sql


def test_columns_sql_preserves_mixed_case_identifier():
    sql = build_show_sql(CompletionKind.COLUMN, ("MyDb", "MySch", "MyTable"), "")
    assert sql is not None
    assert '"MyDb"."MySch"."MyTable"' in sql
    assert '"MYTABLE"' not in sql


@pytest.mark.parametrize(
    "buffer,fqn",
    [
        ("SELECT * FROM db.sch.t WHERE ", '"DB"."SCH"."T"'),
        ("SELECT * FROM Db.Sch.MyTable WHERE ", '"DB"."SCH"."MYTABLE"'),
        ('SELECT * FROM "MyDb".sch."MyTable" WHERE ', '"MyDb"."SCH"."MyTable"'),
        ('SELECT * FROM "db"."sch"."t" WHERE ', '"db"."sch"."t"'),
        ('SELECT "MyDb".sch."MyTable".', '"MyDb"."SCH"."MyTable"'),
        ("SELECT db.sch.t.", '"DB"."SCH"."T"'),
    ],
)
def test_columns_sql_resolves_case_like_snowflake(buffer, fqn):
    ctx = classify(buffer)
    path = ctx.path or ctx.simple_relation
    assert ctx.slot is CompletionSlot.COLUMN
    assert build_show_sql(CompletionKind.COLUMN, path, "") == f"SHOW COLUMNS IN {fqn}"


def test_table_and_view_share_one_show_objects():
    cursor = FakeCursor(
        rows=[("T1", "TABLE"), ("T2", "TRANSIENT"), ("V1", "VIEW"), ("S1", "STAGE")],
        description=(("name",), ("kind",)),
    )
    provider = _provider(cursor)
    tables = provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "T")
    views = provider.lookup(CompletionKind.VIEW, ("DB", "SCH"), "T")
    assert tables == ("T1", "T2")
    assert views == ("V1",)
    assert len(cursor.execute_calls) == 1
    sql = cursor.execute_calls[0][0].upper()
    assert "SHOW OBJECTS" in sql
    assert "LIKE" in sql
    assert "SHOW TABLES" not in sql
    assert "SHOW VIEWS" not in sql


def test_generation_bump_does_not_reuse_stale_show_rows():
    generation = {"n": 0}
    cursor = FakeCursor(
        rows=[("OLD", "TABLE")],
        description=(("name",), ("kind",)),
    )
    provider = _provider(cursor, generation=lambda: generation["n"])
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "O") == ("OLD",)
    assert len(cursor.execute_calls) == 1

    generation["n"] += 1
    cursor.rows = [("NEW", "TABLE")]
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "O") == ("NEW",)
    assert len(cursor.execute_calls) == 2


def test_provider_busy_when_connection_lock_is_held():
    cursor = FakeCursor(rows=[("ALPHA", "TABLE")], description=(("name",), ("kind",)))
    lock = threading.Lock()
    lock.acquire()
    conn = FakeConnection(cursor)
    provider = SnowflakeMetadataProvider(
        connection_factory=lambda: conn,
        connection_lock=lock,
    )
    try:
        provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "A")
    except MetadataError as classified:
        assert classified.error_class == "busy"
    else:
        raise AssertionError("expected MetadataError")
    assert cursor.execute_calls == []
    lock.release()
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "A") == ("ALPHA",)


def test_row_parse_errors_are_not_network_errors_and_do_not_memoize_show():
    cursor = FakeCursor(rows=[("ALPHA", "TABLE")], description=(("name",), ("kind",)))
    provider = _provider(cursor)
    from snowflake.cli._plugins.sql.completion import introspection as module

    original = module._extract_names  # noqa: SLF001

    def boom(*_args, **_kwargs):
        raise ValueError("bad row")

    module._extract_names = boom  # noqa: SLF001
    try:
        try:
            provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "A")
        except ValueError:
            pass
        else:
            raise AssertionError("expected ValueError")
        assert not isinstance(cursor.error, MetadataError)
    finally:
        module._extract_names = original  # noqa: SLF001
    assert provider.lookup(CompletionKind.TABLE, ("DB", "SCH"), "A") == ("ALPHA",)
    assert len(cursor.execute_calls) == 2
