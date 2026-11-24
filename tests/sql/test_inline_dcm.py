import io
from unittest import mock

from snowflake.cli._plugins.sql.inline_dcm import (
    is_inline_dcm_statement,
    split_sql_statements,
)
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli._plugins.sql.statement_reader import (
    CompiledStatement,
    compile_statements,
    files_reader,
    query_reader,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.util_text import split_statements


def _identity(statement: str) -> str:
    return statement


_OPERATORS = (_identity,)

DOCUMENTED_PLAN = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE SCHEMA BART_TEST_DB.DCM_LITE_PROBE;
  DEFINE TABLE BART_TEST_DB.DCM_LITE_PROBE.PROBE (id NUMBER);
  GRANT USAGE ON SCHEMA BART_TEST_DB.DCM_LITE_PROBE TO ROLE PUBLIC;
END;
"""


def _compiled(sql: str, *, remove_comments: bool = False):
    errors, count, compiled = compile_statements(
        query_reader(sql, _OPERATORS, remove_comments=remove_comments)
    )
    assert errors == []
    return count, compiled


def _texts(compiled) -> list[str]:
    return [stmt.statement for stmt in compiled]


def _connector_split(sql: str, remove_comments: bool = False):
    return list(split_statements(io.StringIO(sql), remove_comments=remove_comments))


def test_documented_plan_is_one_statement():
    count, compiled = _compiled(DOCUMENTED_PLAN)
    assert count == 1
    assert len(compiled) == 1
    text = compiled[0].statement
    assert text.startswith("EXECUTE DCM PLAN FROM")
    assert "DEFINE SCHEMA BART_TEST_DB.DCM_LITE_PROBE;" in text
    assert "DEFINE TABLE BART_TEST_DB.DCM_LITE_PROBE.PROBE (id NUMBER);" in text
    assert "GRANT USAGE ON SCHEMA BART_TEST_DB.DCM_LITE_PROBE TO ROLE PUBLIC;" in text
    assert text.rstrip().endswith("END;")
    assert "EXECUTE IMMEDIATE" not in text
    assert "$$" not in text
    # The connector still splits the block. The reader must not.
    assert len(_connector_split(DOCUMENTED_PLAN)) > 1


def test_documented_plan_from_file(tmp_path):
    path = tmp_path / "plan.sql"
    path.write_text(DOCUMENTED_PLAN)
    errors, count, compiled = compile_statements(
        files_reader((SecurePath(path),), _OPERATORS)
    )
    assert errors == []
    assert count == 1
    text = compiled[0].statement
    assert "DEFINE SCHEMA BART_TEST_DB.DCM_LITE_PROBE;" in text
    assert "DEFINE TABLE BART_TEST_DB.DCM_LITE_PROBE.PROBE (id NUMBER);" in text


def test_two_plans_are_two_statements():
    sql = (
        DOCUMENTED_PLAN
        + """
EXECUTE DCM DEPLOY FROM
BEGIN
  DEFINE SCHEMA BART_TEST_DB.DCM_LITE_OTHER;
  DEFINE TABLE BART_TEST_DB.DCM_LITE_OTHER.PROBE (id NUMBER);
END;
"""
    )
    count, compiled = _compiled(sql)
    assert count == 2
    first, second = _texts(compiled)
    assert "DCM_LITE_PROBE" in first
    assert "DCM_LITE_OTHER" not in first
    assert first.rstrip().endswith("END;")
    assert "DEFINE SCHEMA BART_TEST_DB.DCM_LITE_OTHER;" in second
    assert "DEFINE TABLE BART_TEST_DB.DCM_LITE_OTHER.PROBE (id NUMBER);" in second
    assert second.rstrip().endswith("END;")


def test_trailing_line_comment_follows_split_statements():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE SCHEMA a.b;
END; -- tail
SELECT 1;
"""
    kept = _compiled(sql, remove_comments=False)
    assert kept[0] == 2
    assert "-- tail" in kept[1][0].statement
    assert kept[1][1].statement == "SELECT 1;"

    removed = _compiled(sql, remove_comments=True)
    assert removed[0] == 2
    assert "-- tail" not in removed[1][0].statement
    assert removed[1][1].statement == "SELECT 1;"


def test_sourced_file_stays_one_statement(tmp_path):
    path = tmp_path / "body.sql"
    path.write_text(DOCUMENTED_PLAN)
    sql = f"SELECT 1;\n!source {path.as_posix()};\nSELECT 2;\n"
    count, compiled = _compiled(sql)
    assert count == 3
    assert [stmt.statement for stmt in compiled][0] == "SELECT 1;"
    assert "DEFINE SCHEMA BART_TEST_DB.DCM_LITE_PROBE;" in compiled[1].statement
    assert (
        "DEFINE TABLE BART_TEST_DB.DCM_LITE_PROBE.PROBE (id NUMBER);"
        in compiled[1].statement
    )
    assert compiled[2].statement == "SELECT 2;"


def test_plan_followed_by_select():
    sql = DOCUMENTED_PLAN + "SELECT 1;\n"
    count, compiled = _compiled(sql)
    assert count == 2
    assert compiled == [
        CompiledStatement(statement=DOCUMENTED_PLAN.strip()),
        CompiledStatement(statement="SELECT 1;"),
    ]


def test_lowercase_keywords():
    sql = """\
execute dcm deploy from
begin
  define schema a.b;
  define table a.b.t (id number);
end;
"""
    count, compiled = _compiled(sql)
    assert count == 1
    text = compiled[0].statement
    assert "define schema a.b;" in text
    assert "define table a.b.t (id number);" in text
    assert text.rstrip().endswith("end;")


def test_comments_and_whitespace_inside_the_block_are_kept():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  -- keep me
  DEFINE SCHEMA a.b; /* keep; */
END;
"""
    count, compiled = _compiled(sql, remove_comments=True)
    assert count == 1
    text = compiled[0].statement
    assert "-- keep me" in text
    assert "/* keep; */" in text
    assert "DEFINE SCHEMA a.b;" in text


def test_end_inside_string_dollar_body_case_and_nested_begin_does_not_close():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE a.b.t (c VARCHAR DEFAULT 'END');
  DEFINE VIEW a.b.v AS SELECT CASE WHEN 1 = 1 THEN 1 ELSE 0 END;
  DEFINE VIEW a.b.w AS SELECT (CASE WHEN 1 THEN 1 END);
  DEFINE PROCEDURE a.b.p() RETURNS NUMBER LANGUAGE SQL AS
  BEGIN
    IF (TRUE) THEN
      RETURN 1;
    END IF;
  END;
  DEFINE PROCEDURE a.b.q() RETURNS VARCHAR LANGUAGE SQL AS
  $$
  BEGIN
    RETURN 'END';
  END;
  $$;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    text = compiled[0].statement
    assert "DEFAULT 'END'" in text
    assert "ELSE 0 END" in text
    assert "END IF;" in text
    assert "RETURN 'END'" in text
    assert "$$;" in text
    assert text.rstrip().endswith("END;")
    assert compiled[1].statement == "SELECT 1;"


def test_begin_transaction_does_not_swallow_the_next_statement():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE PROCEDURE db.sch.p() RETURNS NUMBER LANGUAGE SQL AS
  BEGIN
    BEGIN TRANSACTION;
    RETURN 1;
  END;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert "BEGIN TRANSACTION;" in compiled[0].statement
    assert compiled[0].statement.rstrip().endswith("END;")
    assert compiled[1].statement == "SELECT 1;"


def test_other_begin_statements_are_not_blocks():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  BEGIN WORK;
  BEGIN NAME t1;
  BEGIN;
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert "BEGIN WORK;" in compiled[0].statement
    assert "BEGIN NAME t1;" in compiled[0].statement
    assert compiled[1].statement == "SELECT 1;"


def test_block_comment_stops_at_the_first_closer():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  /* start
  DEFINE TABLE t (id INT); /* old */
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert "DEFINE SCHEMA a.b;" in compiled[0].statement
    assert compiled[1].statement == "SELECT 1;"


def test_tagged_dollar_is_not_a_here_string():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE PROCEDURE p() AS $body$
  BEGIN
    RETURN 1;
  END;
  $$;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert "$body$" in compiled[0].statement
    assert compiled[0].statement.rstrip().endswith("END;")
    assert compiled[1].statement == "SELECT 1;"


def test_dollar_number_is_not_a_here_string():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE t (id NUMBER DEFAULT $1$);
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert "DEFAULT $1$);" in compiled[0].statement
    assert compiled[1].statement == "SELECT 1;"


def test_backtick_identifier_is_not_a_keyword():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE `END` (id NUMBER);
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert "`END`" in compiled[0].statement
    assert "DEFINE SCHEMA a.b;" in compiled[0].statement
    assert compiled[1].statement == "SELECT 1;"


def test_double_quoted_name_does_not_treat_backslash_as_escape():
    sql = r"""
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE "C:\load\" (id NUMBER);
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert r'"C:\load\"' in compiled[0].statement
    assert "DEFINE SCHEMA a.b;" in compiled[0].statement
    assert compiled[1].statement == "SELECT 1;"


def test_doubled_double_quote_stays_one_identifier():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE "END""X" (id NUMBER);
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert '"END""X"' in compiled[0].statement
    assert "DEFINE SCHEMA a.b;" in compiled[0].statement
    assert compiled[1].statement == "SELECT 1;"


def test_single_quoted_backslash_still_escapes():
    sql = r"""
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE t (c VARCHAR DEFAULT 'it\'s END');
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert "DEFINE SCHEMA a.b;" in compiled[0].statement
    assert compiled[1].statement == "SELECT 1;"


def test_escaped_space_keeps_begin_and_end_inside_the_name():
    begin_sql = r"""
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE my\ BEGIN (id NUMBER);
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    end_sql = r"""
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE TABLE my\ END (id NUMBER);
  DEFINE SCHEMA a.b;
END;
SELECT 1;
"""
    for sql, name in ((begin_sql, r"my\ BEGIN"), (end_sql, r"my\ END")):
        count, compiled = _compiled(sql)
        assert count == 2, name
        assert name in compiled[0].statement
        assert "DEFINE SCHEMA a.b;" in compiled[0].statement
        assert compiled[1].statement == "SELECT 1;"


def test_as_and_ignore_assignments_stay_one_statement():
    sql = """\
EXECUTE DCM PLAN IGNORE_ASSIGNMENTS AS
BEGIN
  DEFINE SCHEMA a.b;
END IGNORE;
"""
    count, compiled = _compiled(sql)
    assert count == 1
    text = compiled[0].statement
    assert "IGNORE_ASSIGNMENTS" in text
    assert text.rstrip().endswith("END IGNORE;")


def test_keywords_may_be_separated_by_comments():
    sql = """\
EXECUTE /* c */ DCM
PLAN
FROM
BEGIN
  DEFINE SCHEMA a.b;
END;
"""
    count, compiled = _compiled(sql)
    assert count == 1
    assert "DEFINE SCHEMA a.b;" in compiled[0].statement


def test_leading_comment_stays_with_the_block():
    sql = """\
SELECT 1; -- stays here
-- header
/* note */
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE SCHEMA a.b;
END;
"""
    count, compiled = _compiled(sql)
    assert count == 2
    assert compiled[0].statement == "SELECT 1; -- stays here"
    assert compiled[1].statement.startswith("-- header")
    assert "-- header" in compiled[1].statement
    assert "/* note */" in compiled[1].statement
    assert "DEFINE SCHEMA a.b;" in compiled[1].statement


def test_execute_dcm_project_and_ordinary_scripts_match_the_connector():
    samples = [
        "SELECT 1; SELECT 2;",
        "SELECT 1;\nSELECT 2;",
        "SELECT 'a;b'; SELECT 2;",
        'SELECT "a;b"; SELECT 2;',
        "SELECT $$ a; b $$; SELECT 2;",
        "SELECT 1; -- semi; still comment\nSELECT 2;",
        "SELECT 1; /* ; */\nSELECT 2;",
        "SELECT '-- not a comment; still one';\nSELECT 2;",
        "SELECT $$-- not a comment\n;$$;\nSELECT 2;",
        "!queries amount=3; select 3;",
        "EXECUTE DCM PROJECT p PLAN;\nSELECT 1;",
        "SELECT 'EXECUTE DCM PLAN FROM BEGIN\nDEFINE X;\nEND;';\nSELECT 2;",
        "SELECT $$ EXECUTE DCM PLAN FROM BEGIN\nDEFINE X;\nEND; $$;\nSELECT 2;",
    ]
    for sql in samples:
        for remove_comments in (False, True):
            assert list(split_sql_statements(sql, remove_comments)) == _connector_split(
                sql, remove_comments
            ), sql


def test_async_suffix_still_marks_one_statement():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE SCHEMA a.b;
END;>
"""
    count, compiled = _compiled(sql)
    assert count == 0
    assert len(compiled) == 1
    assert compiled[0].execute_async is True
    assert "DEFINE SCHEMA a.b;" in compiled[0].statement
    assert not compiled[0].statement.endswith(";>")


def test_manager_sends_the_block_in_one_execute_and_leaves_select_split():
    sql = DOCUMENTED_PLAN + "SELECT 1;\n"
    cursor = mock.Mock()
    connection = mock.Mock()
    connection.cursor.return_value = cursor
    manager = SqlManager(connection=connection)

    with mock.patch.object(
        SqlManager, "_execute_string", return_value=iter(())
    ) as execute_string:
        _count, results = manager.execute(query=sql, files=None, std_in=False)
        list(results)

    cursor.execute.assert_called_once()
    sent = cursor.execute.call_args.args[0]
    assert "DEFINE SCHEMA BART_TEST_DB.DCM_LITE_PROBE;" in sent
    assert "DEFINE TABLE BART_TEST_DB.DCM_LITE_PROBE.PROBE (id NUMBER);" in sent
    assert "GRANT USAGE ON SCHEMA BART_TEST_DB.DCM_LITE_PROBE TO ROLE PUBLIC;" in sent
    assert "num_statements" not in cursor.execute.call_args.kwargs
    execute_string.assert_called_once()
    assert execute_string.call_args.args[0].strip() == "SELECT 1;"


def test_manager_does_not_use_execute_string_for_a_lone_plan():
    connection = mock.Mock()
    connection.cursor.return_value = mock.Mock()
    manager = SqlManager(connection=connection)

    with mock.patch.object(
        SqlManager, "_execute_string", return_value=iter(())
    ) as execute_string:
        _count, results = manager.execute(
            query=DOCUMENTED_PLAN, files=None, std_in=False
        )
        list(results)

    execute_string.assert_not_called()
    connection.cursor.return_value.execute.assert_called_once()
    assert is_inline_dcm_statement(DOCUMENTED_PLAN)


def test_two_plans_are_two_cursor_executes():
    sql = """\
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE SCHEMA a.b;
END;
EXECUTE DCM PLAN FROM
BEGIN
  DEFINE SCHEMA a.c;
END;
"""
    cursor = mock.Mock()
    connection = mock.Mock()
    connection.cursor.return_value = cursor
    manager = SqlManager(connection=connection)

    with mock.patch.object(SqlManager, "_execute_string", return_value=iter(())):
        _count, results = manager.execute(query=sql, files=None, std_in=False)
        list(results)

    assert cursor.execute.call_count == 2
    first = cursor.execute.call_args_list[0].args[0]
    second = cursor.execute.call_args_list[1].args[0]
    assert "DEFINE SCHEMA a.b;" in first
    assert "DEFINE SCHEMA a.c;" in second
    assert "a.c" not in first
