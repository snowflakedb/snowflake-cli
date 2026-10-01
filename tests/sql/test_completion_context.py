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

import pytest
from snowflake.cli._plugins.sql.completion.context import (
    CompletionKind,
    CompletionSlot,
    classify,
)

KEYWORD = (CompletionKind.KEYWORD,)
TYPE = (CompletionKind.TYPE,)
COLUMN = (CompletionKind.COLUMN,)
DATABASE = (CompletionKind.DATABASE,)
SCHEMA = (CompletionKind.SCHEMA,)
REL_FIRST = (
    CompletionKind.DATABASE,
    CompletionKind.SCHEMA,
    CompletionKind.TABLE,
    CompletionKind.VIEW,
)
REL_SECOND = (
    CompletionKind.SCHEMA,
    CompletionKind.TABLE,
    CompletionKind.VIEW,
)
REL_THIRD = (CompletionKind.TABLE, CompletionKind.VIEW)


@pytest.mark.parametrize(
    "buffer,cursor,slot,kinds,path,prefix,quoted,simple_relation",
    [
        # §3.1 keywords / statement start
        ("", None, CompletionSlot.KEYWORD, KEYWORD, (), "", False, None),
        ("SEL", None, CompletionSlot.KEYWORD, KEYWORD, (), "SEL", False, None),
        ("SELECT", None, CompletionSlot.KEYWORD, KEYWORD, (), "SELECT", False, None),
        ("   SEL", None, CompletionSlot.KEYWORD, KEYWORD, (), "SEL", False, None),
        # §3.2 relations — empty prefix
        ("FROM ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        ("JOIN ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        (
            "FROM DB.SCHEMA.",
            None,
            CompletionSlot.OBJECT,
            REL_THIRD,
            ("DB", "SCHEMA"),
            "",
            False,
            None,
        ),
        (
            "USE DATABASE ",
            None,
            CompletionSlot.OBJECT,
            DATABASE,
            (),
            "",
            False,
            None,
        ),
        ("USE SCHEMA ", None, CompletionSlot.OBJECT, SCHEMA, (), "", False, None),
        # §3.3 relations — nonempty prefix
        ("FROM x", None, CompletionSlot.OBJECT, REL_FIRST, (), "x", False, None),
        (
            "FROM DB.s",
            None,
            CompletionSlot.OBJECT,
            REL_SECOND,
            ("DB",),
            "s",
            False,
            None,
        ),
        (
            "FROM DB.SCHEMA.t",
            None,
            CompletionSlot.OBJECT,
            REL_THIRD,
            ("DB", "SCHEMA"),
            "t",
            False,
            None,
        ),
        (
            "USE DATABASE x",
            None,
            CompletionSlot.OBJECT,
            DATABASE,
            (),
            "x",
            False,
            None,
        ),
        ("USE SCHEMA x", None, CompletionSlot.OBJECT, SCHEMA, (), "x", False, None),
        ("UPDATE ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        ("INSERT INTO ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        ("DELETE FROM ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        ("MERGE INTO ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        ("DESC ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        ("DROP TABLE ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        ("DROP VIEW ", None, CompletionSlot.OBJECT, REL_FIRST, (), "", False, None),
        # USE without DATABASE/SCHEMA is still a keyword (DATABASE, SCHEMA, …)
        ("USE ", None, CompletionSlot.KEYWORD, KEYWORD, (), "", False, None),
        # §3.4 columns — dotted relation
        ("rel.", None, CompletionSlot.COLUMN, COLUMN, ("rel",), "", False, None),
        (
            "FROM rel.",
            None,
            CompletionSlot.OBJECT,
            REL_SECOND,
            ("rel",),
            "",
            False,
            None,
        ),
        (
            "JOIN rel.",
            None,
            CompletionSlot.OBJECT,
            REL_SECOND,
            ("rel",),
            "",
            False,
            None,
        ),
        (
            "db.sch.rel.",
            None,
            CompletionSlot.COLUMN,
            COLUMN,
            ("db", "sch", "rel"),
            "",
            False,
            None,
        ),
        (
            "FROM db.sch.rel.",
            None,
            CompletionSlot.COLUMN,
            COLUMN,
            ("db", "sch", "rel"),
            "",
            False,
            None,
        ),
        (
            "FROM db.sch.rel.col",
            None,
            CompletionSlot.COLUMN,
            COLUMN,
            ("db", "sch", "rel"),
            "col",
            False,
            None,
        ),
        # §3.5 unqualified columns when exactly one simple relation
        (
            "SELECT * FROM t WHERE ",
            None,
            CompletionSlot.COLUMN,
            COLUMN,
            (),
            "",
            False,
            ("t",),
        ),
        (
            "SELECT * FROM t WHERE x",
            None,
            CompletionSlot.COLUMN,
            COLUMN,
            (),
            "x",
            False,
            ("t",),
        ),
        (
            "SELECT  FROM t",
            7,
            CompletionSlot.COLUMN,
            COLUMN,
            (),
            "",
            False,
            ("t",),
        ),
        (
            "SELECT * FROM db.sch.t WHERE ",
            None,
            CompletionSlot.COLUMN,
            COLUMN,
            (),
            "",
            False,
            ("db", "sch", "t"),
        ),
        (
            'SELECT * FROM "db.sch.t" WHERE ',
            None,
            CompletionSlot.COLUMN,
            COLUMN,
            (),
            "",
            False,
            ("db.sch.t",),
        ),
        # no columns: missing relation, joins, aliases, CTEs
        ("SELECT ", None, CompletionSlot.KEYWORD, KEYWORD, (), "", False, None),
        (
            "SELECT * FROM t JOIN u WHERE ",
            None,
            CompletionSlot.KEYWORD,
            KEYWORD,
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT * FROM t alias WHERE ",
            None,
            CompletionSlot.KEYWORD,
            KEYWORD,
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT * FROM t AS a WHERE ",
            None,
            CompletionSlot.KEYWORD,
            KEYWORD,
            (),
            "",
            False,
            None,
        ),
        (
            "WITH c AS (SELECT 1) SELECT * FROM t WHERE ",
            None,
            CompletionSlot.KEYWORD,
            KEYWORD,
            (),
            "",
            False,
            None,
        ),
        # type slots
        ("CAST(x AS ", None, CompletionSlot.TYPE, TYPE, (), "", False, None),
        ("CAST(x AS var", None, CompletionSlot.TYPE, TYPE, (), "var", False, None),
        ("SELECT x::", None, CompletionSlot.TYPE, TYPE, (), "", False, None),
        ("SELECT x::int", None, CompletionSlot.TYPE, TYPE, (), "int", False, None),
        (
            "CREATE TABLE t (id ",
            None,
            CompletionSlot.TYPE,
            TYPE,
            (),
            "",
            False,
            None,
        ),
        (
            "CREATE TABLE t (id int",
            None,
            CompletionSlot.TYPE,
            TYPE,
            (),
            "int",
            False,
            None,
        ),
        (
            "CREATE OR REPLACE TABLE t (id ",
            None,
            CompletionSlot.TYPE,
            TYPE,
            (),
            "",
            False,
            None,
        ),
        (
            "CREATE TRANSIENT TABLE t (id ",
            None,
            CompletionSlot.TYPE,
            TYPE,
            (),
            "",
            False,
            None,
        ),
        (
            "CREATE TEMP TABLE t (id ",
            None,
            CompletionSlot.TYPE,
            TYPE,
            (),
            "",
            False,
            None,
        ),
        (
            "CREATE TABLE IF NOT EXISTS t (id ",
            None,
            CompletionSlot.TYPE,
            TYPE,
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT 1 AS foo",
            None,
            CompletionSlot.KEYWORD,
            KEYWORD,
            (),
            "foo",
            False,
            None,
        ),
        # quoted identifier
        ('FROM "foo', None, CompletionSlot.OBJECT, REL_FIRST, (), "foo", True, None),
        (
            'FROM "My Db".s',
            None,
            CompletionSlot.OBJECT,
            REL_SECOND,
            ("My Db",),
            "s",
            False,
            None,
        ),
        # safety → none
        (
            "SELECT 'hello",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT -- comment",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT /* block",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT $$body",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "!rehash",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "  !source foo",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT {{ name }}",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT {% if t %}",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
        (
            "SELECT <% db %>",
            None,
            CompletionSlot.NONE,
            (CompletionKind.NONE,),
            (),
            "",
            False,
            None,
        ),
    ],
    ids=[
        "empty_statement_start",
        "sel_keyword",
        "select_keyword",
        "indented_sel",
        "from_empty",
        "join_empty",
        "from_db_schema_dot",
        "use_database_empty",
        "use_schema_empty",
        "from_prefix",
        "from_db_s",
        "from_db_schema_t",
        "use_database_x",
        "use_schema_x",
        "update_empty",
        "insert_into_empty",
        "delete_from_empty",
        "merge_into_empty",
        "desc_empty",
        "drop_table_empty",
        "drop_view_empty",
        "use_keyword",
        "rel_dot_column",
        "from_rel_dot_object",
        "join_rel_dot_object",
        "fqn_dot_column",
        "from_fqn_dot_column",
        "from_fqn_column_prefix",
        "where_unqualified_empty",
        "where_unqualified_prefix",
        "select_list_one_relation",
        "where_fqn_relation",
        "where_quoted_ident_with_dots",
        "select_no_from",
        "where_join_no_columns",
        "where_alias_no_columns",
        "where_as_alias_no_columns",
        "where_cte_no_columns",
        "cast_as_empty",
        "cast_as_prefix",
        "double_colon_empty",
        "double_colon_prefix",
        "create_table_type_empty",
        "create_table_type_prefix",
        "create_or_replace_table_type",
        "create_transient_table_type",
        "create_temp_table_type",
        "create_table_if_not_exists_type",
        "as_alias_not_type",
        "quoted_from_prefix",
        "quoted_path_segment",
        "inside_string",
        "line_comment",
        "block_comment",
        "dollar_quote",
        "bang_rehash",
        "bang_source",
        "jinja_mustache",
        "jinja_block",
        "legacy_template",
    ],
)
def test_classify_slots(
    buffer, cursor, slot, kinds, path, prefix, quoted, simple_relation
):
    ctx = classify(buffer, cursor)
    assert ctx.slot == slot
    assert ctx.kinds == kinds
    assert ctx.path == path
    assert ctx.prefix == prefix
    assert ctx.quoted is quoted
    assert ctx.simple_relation == simple_relation


@pytest.mark.parametrize(
    "modifiers",
    [
        "",
        "OR REPLACE ",
        "OR ALTER ",
        "TRANSIENT ",
        "TEMP ",
        "TEMPORARY ",
        "LOCAL TEMPORARY ",
        "GLOBAL TEMPORARY ",
        "VOLATILE ",
        "OR REPLACE TRANSIENT ",
    ],
)
@pytest.mark.parametrize("if_not_exists", ["", "IF NOT EXISTS "])
@pytest.mark.parametrize("name", ["t", "db.sch.t", '"My T"'])
@pytest.mark.parametrize(
    "columns", ["(id ", "(id INT, name ", "(\n  id ", "(id VARCHAR(10), name "]
)
def test_create_table_modifiers_keep_type_slot(modifiers, if_not_exists, name, columns):
    ctx = classify(f"CREATE {modifiers}TABLE {if_not_exists}{name} {columns}")
    assert ctx.slot == CompletionSlot.TYPE


@pytest.mark.parametrize(
    "buffer",
    [
        "CREATE TABLE t AS SELECT coalesce(a, b ",
        "CREATE TABLE t CLUSTER BY (a, b ",
        "CREATE TABLE t (id INT) CLUSTER BY (a, b ",
        "CREATE TABLE t (id NUMBER(10, s ",
        "CREATE VIEW v AS SELECT f(a, b ",
    ],
    ids=[
        "ctas_function_args",
        "cluster_by_without_columns",
        "cluster_by_after_columns",
        "type_arguments",
        "create_view_function_args",
    ],
)
def test_parenthesized_lists_outside_column_definitions_are_not_types(buffer):
    assert classify(buffer).slot != CompletionSlot.TYPE


@pytest.mark.parametrize(
    "buffer,path",
    [
        ("FROM rel.", ("rel",)),
        ("JOIN rel.", ("rel",)),
        ("SELECT * FROM a, rel.", ("rel",)),
        ("SELECT * FROM a x, rel.", ("rel",)),
        ("SELECT * FROM a JOIN b, rel.", ("rel",)),
        ("SELECT * FROM (SELECT 1) s, rel.", ("rel",)),
        ("UPDATE t SET a = 1 FROM a, rel.", ("rel",)),
        ("SELECT * FROM a, ", ()),
        ("DROP TABLE rel.", ("rel",)),
        ("DROP TABLE IF EXISTS rel.", ("rel",)),
        ("DROP VIEW IF EXISTS rel.", ("rel",)),
        ("DROP TABLE IF EXISTS ", ()),
    ],
)
def test_relation_position_wins_over_dotted_column_path(buffer, path):
    ctx = classify(buffer)
    assert ctx.slot == CompletionSlot.OBJECT
    assert ctx.path == path


@pytest.mark.parametrize(
    "buffer",
    [
        "SELECT a, rel.",
        "SELECT * FROM t WHERE f(a, rel.",
        "SELECT * FROM TABLE(f(a, rel.",
        "SELECT * FROM t WHERE a = 1 AND x IN (1, rel.",
        "SELECT * FROM a WHERE x = 1, rel.",
    ],
)
def test_commas_outside_from_list_keep_column_path(buffer):
    assert classify(buffer).slot == CompletionSlot.COLUMN


def test_classify_statement_starts_after_unquoted_semicolon():
    buffer = "SELECT 1; FROM x"
    ctx = classify(buffer)
    assert ctx.statement_start == buffer.rfind(";") + 1
    assert ctx.statement_end == len(buffer)
    assert ctx.slot == CompletionSlot.OBJECT
    assert ctx.prefix == "x"


def test_classify_ignores_semicolon_inside_string():
    buffer = "SELECT 'a;b' FROM x"
    ctx = classify(buffer)
    assert ctx.statement_start == 0
    assert ctx.slot == CompletionSlot.OBJECT
    assert ctx.prefix == "x"


def test_classify_multiline_after_semicolon():
    buffer = "SELECT 1;\nFROM x"
    ctx = classify(buffer)
    assert ctx.statement_start == buffer.rfind(";") + 1
    assert ctx.slot == CompletionSlot.OBJECT
    assert ctx.prefix == "x"


def test_classify_mid_buffer_cursor_uses_current_statement():
    buffer = "SELECT 1; FROM x; SELECT 2"
    cursor = buffer.index("x") + 1
    ctx = classify(buffer, cursor)
    assert ctx.statement_start == buffer.index(" FROM")
    assert ctx.statement_end == cursor
    assert ctx.slot == CompletionSlot.OBJECT
    assert ctx.prefix == "x"


def test_classify_bang_after_semicolon_is_none():
    ctx = classify("SELECT 1;\n!rehash")
    assert ctx.slot == CompletionSlot.NONE


def test_classify_template_in_prior_statement_does_not_poison_next():
    ctx = classify("SELECT {{ x }}; FROM t")
    assert ctx.slot == CompletionSlot.OBJECT
    assert ctx.prefix == "t"
