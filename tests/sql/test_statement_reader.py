from functools import partial

import pytest
from jinja2 import UndefinedError
from pytest_httpserver import HTTPServer
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli._plugins.sql.statement_reader import (
    CompiledStatement,
    ParsedStatement,
    StatementType,
    compile_statements,
    files_reader,
    parse_statement,
    query_reader,
)
from snowflake.cli.api.rendering.sql_templates import snowflake_sql_jinja_render
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
        partial(snowflake_sql_jinja_render, data={"aaa": "foo", "bbb": "bar"}),
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
    parsed_statements = query_reader(queries, [])
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


@pytest.mark.parametrize("command", ["queries", "abort", "result", "AbOrT"])
def test_parse_command(command):
    query = f"!{command} args k1=v1 k2=v2;"
    parsed_statement = parse_statement(query, [])
    assert parsed_statement.statement_type == StatementType.SNOWSQL_COMMAND
    assert parsed_statement.statement.read() == query
    assert parsed_statement.source_path is None
    assert parsed_statement.error is None


def test_parse_unknown_command():
    query = f"!unknown_cmd a=b c d"
    parsed_statement = parse_statement(query, [])
    assert parsed_statement.statement_type == StatementType.UNKNOWN
    assert parsed_statement.statement.read() == query
    assert parsed_statement.source_path is None
    assert parsed_statement.error == "Unknown command: unknown_cmd"
