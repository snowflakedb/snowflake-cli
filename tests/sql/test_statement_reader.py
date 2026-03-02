from functools import partial

import pytest
from jinja2 import UndefinedError
from pytest_httpserver import HTTPServer
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli._plugins.sql.statement_reader import (
    CompiledStatement,
    ParsedStatement,
    StatementType,
    _protect_sql_comments,
    compile_statements,
    files_reader,
    parse_statement,
    query_reader,
)
from snowflake.cli.api.rendering.sql_templates import (
    SQLTemplateSyntaxConfig,
    snowflake_sql_jinja_render,
)
from snowflake.cli.api.secure_path import SecurePath


def successful_sql_operator(statement: str) -> str:
    return statement


def failing_sql_operator(statement: str) -> str:
    raise UndefinedError(f"aaa for {statement}")


WORKING_OPERATOR_FUNCS = (successful_sql_operator,)
FAILING_OPERATOR_FUNCS = (failing_sql_operator,)


def test_mixed_recursion(
    tmp_path_factory: pytest.TempPathFactory,
    httpserver: HTTPServer,
):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"

    httpserver.expect_request("/u1.sql").respond_with_data(
        f"select 1; !source {f1.as_posix()}; select 1a;"
    )
    url = httpserver.url_for("/u1.sql")

    f1.write_text(f"select 2; !source {url}; select 2a;")

    source = query_reader(
        f"select 0; !source {url}; select 0a;",
        WORKING_OPERATOR_FUNCS,
    )
    errors, cnt, compiled = compile_statements(source)

    assert len(errors) == 1
    assert errors[0].startswith("Recursion detected:")
    assert cnt == 6
    assert compiled == [
        CompiledStatement(statement="select 0;"),
        CompiledStatement(statement="select 1;"),
        CompiledStatement(statement="select 2;"),
        CompiledStatement(statement="select 2a;"),
        CompiledStatement(statement="select 1a;"),
        CompiledStatement(statement="select 0a;"),
    ]


def test_recursion_from_url(httpserver: HTTPServer):
    httpserver.expect_request("/f1.sql").respond_with_data(
        f"select 1; !source {httpserver.url_for('f2.sql')}; select 1a;"
    )
    httpserver.expect_request("/f2.sql").respond_with_data(
        f"select 2; !source {httpserver.url_for('f1.sql')}; select 2a;"
    )

    query = f"select 0;\n!source {httpserver.url_for('f1.sql')};\nselect 0a;"
    source = query_reader(query, WORKING_OPERATOR_FUNCS)
    errors, cnt, compiled = compile_statements(source)

    assert len(errors)
    assert errors[0].startswith("Recursion detected:")
    assert cnt == 6
    assert compiled == [
        CompiledStatement(statement="select 0;"),
        CompiledStatement(statement="select 1;"),
        CompiledStatement(statement="select 2;"),
        CompiledStatement(statement="select 2a;"),
        CompiledStatement(statement="select 1a;"),
        CompiledStatement(statement="select 0a;"),
    ]


def test_recursion_multiple_files(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"
    f2 = tmp_path_factory.mktemp("a") / "f2.sql"
    f3 = tmp_path_factory.mktemp("a") / "f3.sql"

    f1.write_text(
        f"select 1; !source {f2.as_posix()}; select 1a; !source {f3.as_posix()}; select 1b;"
    )
    f2.write_text(f"select 2; !source {f1.as_posix()}; select 2a;")
    f3.write_text(f"select 3; !source {f1.as_posix()}; select 3a;")

    source = files_reader(
        (SecurePath(f1),),
        WORKING_OPERATOR_FUNCS,
    )
    errors, cnt, compiled = compile_statements(source)

    assert len(errors) == 2
    first_error, second_error = errors
    assert first_error.startswith("Recursion detected:")
    assert second_error.startswith("Recursion detected:")

    assert cnt == 7
    assert compiled == [
        CompiledStatement(statement="select 1;"),
        CompiledStatement(statement="select 2;"),
        CompiledStatement(statement="select 2a;"),
        CompiledStatement(statement="select 1a;"),
        CompiledStatement(statement="select 3;"),
        CompiledStatement(statement="select 3a;"),
        CompiledStatement(statement="select 1b;"),
    ]


def test_recusion_detection_from_files(tmp_path_factory: pytest.TempPathFactory):
    f1 = SecurePath(tmp_path_factory.mktemp("a") / "f1.sql")
    f2 = tmp_path_factory.mktemp("a") / "f2.sql"

    f1.write_text(f"select 1; !source {f2.as_posix()};")
    f2.write_text(f"select 2; !source {f1.as_posix()};")

    source = files_reader((f1,), WORKING_OPERATOR_FUNCS)
    errors, cnt, compiled = compile_statements(source)
    assert errors[0].startswith("Recursion detected:")
    assert cnt == 2
    assert compiled == [
        CompiledStatement(statement="select 1;"),
        CompiledStatement(statement="select 2;"),
    ]


def test_source_missing_url(httpserver: HTTPServer):
    httpserver.expect_request("/f1.sql").respond_with_data("select 1;")

    query = f"!source {httpserver.url_for('missing.sql')};"
    source = query_reader(query, WORKING_OPERATOR_FUNCS)
    errors, cnt, compiled = compile_statements(source)
    assert errors
    assert errors[0].startswith("Could not fetch")
    assert cnt == 0
    assert not compiled


def test_read_query():
    query = "select 1;"
    errors, cnt, compiled = compile_statements(
        query_reader(query, WORKING_OPERATOR_FUNCS),
    )
    assert not errors, errors
    assert cnt == 1
    assert compiled == [
        CompiledStatement(statement="select 1;"),
    ]


def test_read_files(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"
    f1.write_text("select 1;")
    f2 = tmp_path_factory.mktemp("a") / "f2.sql"
    f2.write_text("select 2;")

    files = (SecurePath(f1), SecurePath(f2))
    errors, cnt, compiled = compile_statements(
        files_reader(files, WORKING_OPERATOR_FUNCS),
    )

    assert not errors, errors
    assert cnt == 2
    assert compiled == [
        CompiledStatement(statement="select 1;"),
        CompiledStatement(statement="select 2;"),
    ]


def test_parsed_source_repr():
    query = "select 1;"

    source = ParsedStatement(query, StatementType.QUERY, None)
    assert (
        str(source)
        == "ParsedStatement(statement_type=StatementType.QUERY, source_path=None, error=None)"
    )


def test_parse_source_url(httpserver: HTTPServer):
    httpserver.expect_request("/f1.sql").respond_with_data("select 1;")

    query = f"!source {httpserver.url_for('f1.sql')};"
    source = parse_statement(query, WORKING_OPERATOR_FUNCS)

    assert source.statement_type == StatementType.URL
    assert source.source_path and source.source_path == httpserver.url_for("f1.sql")
    assert source.source_path.startswith("http://localhost:")
    assert source.source_path.endswith("/f1.sql")
    assert source.statement.read() == "select 1;"
    assert source.error is None


def test_parse_source_invalid_url(httpserver: HTTPServer):
    httpserver.expect_request("/f1.sql").respond_with_data("select 1;")

    invalid_url = httpserver.url_for("invalid.sql")
    invalid_source = f"!source {invalid_url};"

    source = parse_statement(invalid_source, WORKING_OPERATOR_FUNCS)

    assert source.statement_type == StatementType.URL
    assert source.source_path == invalid_source
    assert source.statement.read() == invalid_url
    assert source.error and source.error.startswith("Could not fetch")


def test_parse_source_just_query():
    source = parse_statement("select 1;", WORKING_OPERATOR_FUNCS)
    expected = ParsedStatement("select 1;", StatementType.QUERY, None, None)
    assert source == expected


@pytest.mark.parametrize(
    "statement, expected",
    (
        pytest.param(
            "!source {path};",
            ParsedStatement(
                "select 73;",
                StatementType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!SoUrCe {path};",
            ParsedStatement(
                "select 73;",
                StatementType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!SOURCE {path};",
            ParsedStatement(
                "select 73;",
                StatementType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!load {path};",
            ParsedStatement(
                "select 73;",
                StatementType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!LoaD {path};",
            ParsedStatement(
                "select 73;",
                StatementType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!LOAD {path};",
            ParsedStatement(
                "select 73;",
                StatementType.FILE,
                "path",
            ),
        ),
    ),
)
def test_parse_source_command_detection(
    statement, expected, tmp_path_factory: pytest.TempPathFactory
):
    path = tmp_path_factory.mktemp("data") / "f.sql"
    path.write_text("select 73;")
    expected.source_path = path.as_posix()

    source = parse_statement(statement.format(path=path), WORKING_OPERATOR_FUNCS)
    assert source == expected


def test_parse_source_file(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"
    f1.write_text("select 1;")

    query = f"!source {f1.as_posix()};"
    source = parse_statement(query, WORKING_OPERATOR_FUNCS)

    assert source.statement_type == StatementType.FILE
    assert source.source_path
    assert source.source_path == f1.as_posix()
    assert source.statement.read() == "select 1;"
    assert source.error is None


def test_parse_source_invalid_file(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"

    invalid_path = f"{f1.as_posix()}_suffix"
    invalid_source = f"!source {invalid_path};"
    source = parse_statement(invalid_source, WORKING_OPERATOR_FUNCS)

    assert source.statement_type == StatementType.FILE
    assert source.source_path == invalid_source
    assert source.statement.read() == invalid_path
    assert source.error and source.error.startswith("Could not read")


def test_allow_comments_at_source_file(tmp_path_factory: pytest.TempPathFactory):
    expected_content = "select 73"

    f1 = tmp_path_factory.mktemp("a") / "f1.sql"
    f1.write_text(expected_content)

    source_with_comment = f"!source {f1.as_posix()} --noqa: XXX"
    source = parse_statement(source_with_comment, WORKING_OPERATOR_FUNCS)
    assert source.statement_type == StatementType.FILE
    assert source.source_path and source.source_path == f1.as_posix()
    assert source.statement.read() == expected_content
    assert source.error is None


def test_allow_comments_at_source_url(httpserver: HTTPServer):
    expected_content = "select 73"
    httpserver.expect_request("/f1.sql").respond_with_data(expected_content)

    query = f"!source {httpserver.url_for('f1.sql')} --noqa: XXX"
    source = parse_statement(query, WORKING_OPERATOR_FUNCS)

    assert source.statement_type == StatementType.URL
    assert source.source_path and source.source_path == httpserver.url_for("f1.sql")
    assert source.source_path.startswith("http://localhost:")
    assert source.source_path.endswith("/f1.sql")
    assert source.statement.read() == expected_content
    assert source.error is None


@pytest.mark.parametrize("command", ["source", "load"])
def test_parse_source_default_fallback(command):
    path = "s3://bucket/path/file.sql"
    unknown_source = f"!{command} {path};"

    source = parse_statement(unknown_source, WORKING_OPERATOR_FUNCS)

    assert not source
    assert source.statement_type == StatementType.UNKNOWN
    assert source.source_path == path
    assert source.statement.read() == unknown_source
    assert source.error and source.error.startswith("Unknown source")


@pytest.mark.parametrize(
    "query",
    [
        "!source &{ aaa }.&{ bbb }",
        "!source &aaa.&bbb",
        "!source &aaa.&{ bbb }",
        "!source <% aaa %>.<% bbb %>",
    ],
)
@pytest.mark.usefixtures("runner")
def test_rendering_of_sql_with_commands(query):
    stmt_operators = (
        transpile_snowsql_templates,
        partial(
            snowflake_sql_jinja_render,
            data={"aaa": "foo", "bbb": "bar"},
            template_syntax_config=SQLTemplateSyntaxConfig(),
        ),
    )
    parsed_source = parse_statement(query, stmt_operators)
    assert parsed_source.statement_type == StatementType.FILE
    assert parsed_source.source_path == "!source foo.bar"
    assert parsed_source.statement.read() == "foo.bar"
    assert parsed_source.error == "Could not read: foo.bar"


def test_detect_async_queries():
    queries = """select 1;>
    select -1;
    select 2;>
    select -2;
    select 3;>
    """
    errors, expected_results, compiled_statements = compile_statements(
        query_reader(queries, [])
    )
    assert errors == []
    assert expected_results == 2
    assert list(compiled_statements) == [
        CompiledStatement(statement="select 1", execute_async=True, command=None),
        CompiledStatement(statement="select -1;", execute_async=False, command=None),
        CompiledStatement(statement="select 2", execute_async=True, command=None),
        CompiledStatement(statement="select -2;", execute_async=False, command=None),
        CompiledStatement(statement="select 3", execute_async=True, command=None),
    ]


def test_source_file_receives_pre_render(tmp_path_factory: pytest.TempPathFactory):
    """pre_render must be applied to files loaded via !source, not just the top-level query."""
    f1 = tmp_path_factory.mktemp("jinja_source") / "sourced.sql"
    f1.write_text("{% if flag %}SELECT 42;{% endif %}")

    rendered_calls: list[str] = []

    def pre_render(content: str) -> str:
        from snowflake.cli.api.rendering.sql_templates import (
            SQLTemplateSyntaxConfig,
            snowflake_sql_jinja_render,
        )

        rendered_calls.append(content)
        return snowflake_sql_jinja_render(
            content,
            template_syntax_config=SQLTemplateSyntaxConfig(
                enable_legacy_syntax=False,
                enable_standard_syntax=False,
                enable_jinja_syntax=True,
            ),
            data={"flag": True},
        )

    source = query_reader(
        f"!source {f1.as_posix()};",
        operators=[],
        pre_render=pre_render,
    )
    errors, cnt, compiled = compile_statements(source)

    assert errors == [], errors
    assert cnt == 1
    assert compiled[0].statement == "SELECT 42;"
    # pre_render must have been called for the sourced file
    assert any("{% if flag %}" in c for c in rendered_calls)


@pytest.mark.parametrize("command", ["queries", "abort", "result", "AbOrT"])
def test_parse_command(command):
    query = f"!{command} args k1=v1 k2=v2;"
    parsed_statement = parse_statement(query, [])
    assert parsed_statement.statement_type == StatementType.REPL_COMMAND
    assert parsed_statement.statement.read() == query
    assert parsed_statement.source_path is None
    assert parsed_statement.error is None


def test_parse_unknown_command():
    query = "!unknown_cmd a=b c d"
    parsed_statement = parse_statement(query, [])
    assert parsed_statement.statement_type == StatementType.UNKNOWN
    assert parsed_statement.statement.read() == query
    assert parsed_statement.source_path is None
    assert parsed_statement.error == "Unknown command: unknown_cmd"


# ---------------------------------------------------------------------------
# _protect_sql_comments tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # Line comment
        "SELECT 1; -- a comment\nSELECT 2;",
        # Inline block comment
        "SELECT /* inline */ 1;",
        # Multi-line block comment
        "SELECT\n/* line1\nline2\n*/\n1;",
        # Nested block comment (Snowflake extension)
        "SELECT /* outer /* inner */ still outer */ 1;",
        # Jinja-like syntax in line comment
        "-- {{ var }}\nSELECT 1;",
        # Jinja-like syntax in block comment
        "/* {{ var }} */\nSELECT 1;",
        # {% endraw %} inside a comment — the edge case that breaks {% raw %} wrapping
        "-- {% endraw %} trick\nSELECT 1;",
        # Single-quoted string with comment-like content (must be untouched)
        "SELECT '-- not a comment';",
        # Single-quoted string with '' escape
        "SELECT 'it''s fine -- not a comment';",
        # Dollar-quoted string with comment-like content (must be untouched)
        "SELECT $$-- not a comment\n{{var}}$$;",
        # No comments — input should round-trip unchanged
        "SELECT 1; SELECT 2;",
        # Unterminated block comment
        "SELECT 1; /* no end",
        # Unterminated line comment (no trailing newline)
        "SELECT 1; -- no newline",
    ],
)
def test_protect_sql_comments_roundtrip(sql):
    """protect → restore must always produce the original SQL."""
    placeholder_sql, saved = _protect_sql_comments(sql)
    assert saved.restore(placeholder_sql) == sql


def test_protect_sql_comments_hides_jinja_syntax():
    """Jinja-like syntax in comments must not survive into the placeholder SQL."""
    sql = "-- {{ secret }}\nSELECT /* {% if x %} */ 1;"
    placeholder_sql, _ = _protect_sql_comments(sql)
    assert "{{" not in placeholder_sql
    assert "{%" not in placeholder_sql


def test_protect_sql_comments_nested_block():
    """Nested block comment must be captured as a single placeholder."""
    sql = "SELECT /* outer /* inner */ still outer */ 1;"
    placeholder_sql, saved = _protect_sql_comments(sql)
    # Entire nested comment replaced by a single placeholder
    assert "/*" not in placeholder_sql
    assert saved.restore(placeholder_sql) == sql


def test_protect_comments_roundtrip_through_jinja():
    """Comments must survive a full protect → Jinja-render → restore cycle."""
    from snowflake.cli.api.rendering.sql_templates import (
        SQLTemplateSyntaxConfig,
        snowflake_sql_jinja_render,
    )

    sql = "-- {{ not_a_var }}\nSELECT /* {{ also_not }} */ 1 WHERE x = {{ x }};"
    placeholder_sql, saved = _protect_sql_comments(sql)
    rendered = snowflake_sql_jinja_render(
        placeholder_sql,
        template_syntax_config=SQLTemplateSyntaxConfig(enable_jinja_syntax=True),
        data={"x": 42},
    )
    restored = saved.restore(rendered)
    assert restored == "-- {{ not_a_var }}\nSELECT /* {{ also_not }} */ 1 WHERE x = 42;"


def test_files_reader_utf8_content(tmp_path_factory, monkeypatch):
    """SQL files with non-ASCII UTF-8 content should be readable."""
    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", "utf-8")
    f1 = tmp_path_factory.mktemp("enc") / "japanese.sql"
    f1.write_text(
        "-- テスト用SQLファイル\nSELECT 1;\n-- データベース確認\nSELECT 2;\n",
        encoding="utf-8",
    )
    source = files_reader((SecurePath(f1),), WORKING_OPERATOR_FUNCS)
    errors, cnt, compiled = compile_statements(source)
    assert not errors
    assert cnt == 2
    assert compiled == [
        CompiledStatement(statement="-- テスト用SQLファイル\nSELECT 1;"),
        CompiledStatement(statement="-- データベース確認\nSELECT 2;"),
    ]


def test_from_file_utf8_content(tmp_path_factory, monkeypatch):
    """ParsedStatement.from_file with non-ASCII UTF-8 file requires proper encoding configuration.

    This test simulates a Windows cp1252 environment and demonstrates that UTF-8
    encoding must be explicitly configured to correctly read files with non-ASCII characters.
    """
    # Simulate Windows cp1252 environment where platform default would fail for UTF-8
    monkeypatch.setattr("locale.getpreferredencoding", lambda: "cp1252")
    monkeypatch.setattr("sys.getfilesystemencoding", lambda: "cp1252")
    monkeypatch.setattr("sys.getdefaultencoding", lambda: "cp1252")

    # Write UTF-8 file with Japanese characters that CANNOT be represented in cp1252
    f1 = tmp_path_factory.mktemp("enc") / "japanese.sql"
    expected_content = "-- 日本語コメント\nSELECT 1;\n"
    f1.write_text(expected_content, encoding="utf-8")

    monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", "utf-8")

    # Now reading should work because UTF-8 encoding is properly configured
    result = ParsedStatement.from_file(str(f1), f"!source {f1};")

    # Verify no error occurred
    assert result.error is None, f"Expected no error but got: {result.error}"
    assert result.statement_type == StatementType.FILE

    # Verify the Japanese characters were actually read correctly
    # This assertion proves that UTF-8 encoding configuration is working
    actual_content = result.statement.read()
    result.statement.seek(0)  # Reset for potential reuse

    assert "日本語コメント" in actual_content, (
        f"Japanese characters not found in content. "
        f"This indicates the file was not read with UTF-8 encoding. "
        f"Expected to find '日本語コメント' but got: {actual_content!r}"
    )
    assert actual_content == expected_content, (
        f"Content mismatch. Expected:\n{expected_content!r}\n"
        f"But got:\n{actual_content!r}"
    )
