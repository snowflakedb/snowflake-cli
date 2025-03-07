from functools import partial

import pytest
from jinja2 import UndefinedError
from pytest_httpserver import HTTPServer
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli._plugins.sql.source_reader import (
    ParsedSource,
    SourceType,
    compile_statements,
    files_reader,
    parse_source,
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
        "select 0;",
        "select 1;",
        "select 2;",
        "select 2a;",
        "select 1a;",
        "select 0a;",
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
        "select 0;",
        "select 1;",
        "select 2;",
        "select 2a;",
        "select 1a;",
        "select 0a;",
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
        "select 1;",
        "select 2;",
        "select 2a;",
        "select 1a;",
        "select 3;",
        "select 3a;",
        "select 1b;",
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
    assert compiled == ["select 1;", "select 2;"]


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
        "select 1;",
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
        "select 1;",
        "select 2;",
    ]


def test_parsed_source_repr():
    query = "select 1;"

    source = ParsedSource(query, SourceType.QUERY, None)
    assert (
        str(source)
        == "ParsedSource(source_type=SourceType.QUERY, source_path=None, error=None)"
    )


def test_parse_source_url(httpserver: HTTPServer):
    httpserver.expect_request("/f1.sql").respond_with_data("select 1;")

    query = f"!source {httpserver.url_for('f1.sql')};"
    source = parse_source(query, WORKING_OPERATOR_FUNCS)

    assert source.source_type == SourceType.URL
    assert source.source_path and source.source_path == httpserver.url_for("f1.sql")
    assert source.source_path.startswith("http://localhost:")
    assert source.source_path.endswith("/f1.sql")
    assert source.source.read() == "select 1;"
    assert source.error is None


def test_parse_source_invalid_url(httpserver: HTTPServer):
    httpserver.expect_request("/f1.sql").respond_with_data("select 1;")

    invalid_url = httpserver.url_for("invalid.sql")
    invalid_source = f"!source {invalid_url};"

    source = parse_source(invalid_source, WORKING_OPERATOR_FUNCS)

    assert source.source_type == SourceType.URL
    assert source.source_path == invalid_source
    assert source.source.read() == invalid_url
    assert source.error and source.error.startswith("Could not fetch")


def test_parse_source_just_query():
    source = parse_source("select 1;", WORKING_OPERATOR_FUNCS)
    expected = ParsedSource("select 1;", SourceType.QUERY, None, None)
    assert source == expected


@pytest.mark.parametrize(
    "statement, expected",
    (
        pytest.param(
            "!source {path};",
            ParsedSource(
                "select 73;",
                SourceType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!SoUrCe {path};",
            ParsedSource(
                "select 73;",
                SourceType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!SOURCE {path};",
            ParsedSource(
                "select 73;",
                SourceType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!load {path};",
            ParsedSource(
                "select 73;",
                SourceType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!LoaD {path};",
            ParsedSource(
                "select 73;",
                SourceType.FILE,
                "path",
            ),
        ),
        pytest.param(
            "!LOAD {path};",
            ParsedSource(
                "select 73;",
                SourceType.FILE,
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

    source = parse_source(statement.format(path=path), WORKING_OPERATOR_FUNCS)
    assert source == expected


def test_parse_source_file(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"
    f1.write_text("select 1;")

    query = f"!source {f1.as_posix()};"
    source = parse_source(query, WORKING_OPERATOR_FUNCS)

    assert source.source_type == SourceType.FILE
    assert source.source_path
    assert source.source_path == f1.as_posix()
    assert source.source.read() == "select 1;"
    assert source.error is None


def test_parse_source_invalid_file(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"

    invalid_path = f"{f1.as_posix()}_suffix"
    invalid_source = f"!source {invalid_path};"
    source = parse_source(invalid_source, WORKING_OPERATOR_FUNCS)

    assert source.source_type == SourceType.FILE
    assert source.source_path == invalid_source
    assert source.source.read() == invalid_path
    assert source.error and source.error.startswith("Could not read")


def test_parse_source_default_fallback():
    path = "s3://bucket/path/file.sql"
    unknown_source = f"!load {path};"

    source = parse_source(unknown_source, WORKING_OPERATOR_FUNCS)

    assert not source
    assert source.source_type == SourceType.UNKNOWN
    assert source.source_path == unknown_source
    assert source.source.read() == path
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
    parsed_source = parse_source(query, stmt_operators)
    assert parsed_source.source_type == SourceType.FILE
    assert parsed_source.source_path == "!source foo.bar"
    assert parsed_source.source.read() == "foo.bar"
    assert parsed_source.error == "Could not read: foo.bar"
