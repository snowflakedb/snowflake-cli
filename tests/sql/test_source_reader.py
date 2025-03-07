import pytest
from pytest_httpserver import HTTPServer
from snowflake.cli._plugins.sql.source_reader import (
    ParsedSource,
    SourceType,
    parse_source,
)


def test_parsed_source_repr():
    query = "select 1;"

    source = ParsedSource(query, SourceType.QUERY, None)
    assert (
        str(source)
        == "ParsedSource(source_type=SourceType.QUERY, source_path=None, error=None)"
    )


def test_parse_source_url(httpserver: HTTPServer):
    httpserver.expect_request("/f1.sql").respond_with_data("select 1;")

    source = parse_source(f"!source {httpserver.url_for('f1.sql')};")

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

    source = parse_source(invalid_source)

    assert source.source_type == SourceType.URL
    assert source.source_path == invalid_source
    assert source.source.read() == invalid_url
    assert source.error and source.error.startswith("Could not fetch")


def test_parse_source_just_query():
    source = parse_source("select 1;")
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

    source = parse_source(statement.format(path=path))
    assert source == expected


def test_parse_source_file(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"
    f1.write_text("select 1;")

    source = parse_source(f"!source {f1.as_posix()};")

    assert source.source_type == SourceType.FILE
    assert source.source_path
    assert source.source_path == f1.as_posix()
    assert source.source.read() == "select 1;"
    assert source.error is None


def test_parse_source_invalid_file(tmp_path_factory: pytest.TempPathFactory):
    f1 = tmp_path_factory.mktemp("a") / "f1.sql"

    invalid_path = f"{f1.as_posix()}_suffix"
    invalid_source = f"!source {invalid_path};"
    source = parse_source(invalid_source)

    assert source.source_type == SourceType.FILE
    assert source.source_path == invalid_source
    assert source.source.read() == invalid_path
    assert source.error and source.error.startswith("Could not read")


def test_parse_source_default_fallback():
    path = "s3://bucket/path/file.sql"
    unknown_source = f"!load {path};"

    source = parse_source(unknown_source)

    assert not source
    assert source.source_type == SourceType.UNKNOWN
    assert source.source_path == unknown_source
    assert source.source.read() == path
    assert source.error and source.error.startswith("Unknown source")
