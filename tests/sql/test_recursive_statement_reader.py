import pytest
from click import ClickException
from snowflake.cli._plugins.sql.reader import (
    SQLReader,
    StatementType,
)
from snowflake.cli.api.rendering.sql_templates import snowflake_sql_jinja_render


def test_statement_source_is_required():
    with pytest.raises(ClickException):
        SQLReader(query=None, files=None)


def test_empty_generator_from_files():
    reader = SQLReader("a", [])

    with pytest.raises(StopIteration):
        next(reader._file_reader)  # noqa: SLF001


def test_source_recursion_detection_from_files(recursive_source_includes):
    reader = SQLReader(None, recursive_source_includes)
    raw_statements = reader._recursive_file_reader(  # noqa: SLF001
        recursive_source_includes, set()
    )

    assert next(raw_statements) == (None, StatementType.STATEMENT, "1;")
    assert next(raw_statements) == (None, StatementType.STATEMENT, "2;")
    assert next(raw_statements) == (None, StatementType.STATEMENT, "3;")

    error, accumulator, statement = next(raw_statements)
    assert isinstance(error, str)
    assert error.startswith("Recursion detected for file")
    assert accumulator == StatementType.COMMAND
    assert statement is None

    assert next(raw_statements) == (None, StatementType.STATEMENT, "FINAL;")

    with pytest.raises(StopIteration):
        next(raw_statements)


def test_source_recursion_detection_from_query(recursive_source_includes):
    query = f"select 1; !source {recursive_source_includes.path.as_posix()};"
    reader = SQLReader(query=query, files=None)
    raw_statements = reader._input_reader  # noqa: SLF001

    assert next(raw_statements) == (None, StatementType.STATEMENT, "select 1;")
    assert next(raw_statements) == (None, StatementType.STATEMENT, "1;")
    assert next(raw_statements) == (None, StatementType.STATEMENT, "2;")
    assert next(raw_statements) == (None, StatementType.STATEMENT, "3;")

    error, accumulator, statement = next(raw_statements)
    assert isinstance(error, str)
    assert error.startswith("Recursion detected for file")
    assert accumulator == StatementType.COMMAND

    assert next(raw_statements) == (None, StatementType.STATEMENT, "FINAL;")

    assert statement is None

    with pytest.raises(StopIteration):
        next(raw_statements)


def test_source_dispatcher_statement():
    statement = "select 1;"

    reader = SQLReader(query="n/a", files=None)
    dispatcher = reader._command_dispatcher(statement)  # noqa: SLF001

    assert next(dispatcher) == (None, StatementType.STATEMENT, "select 1;")

    with pytest.raises(StopIteration):
        next(dispatcher)


def test_source_dispatcher_with_source_from_input(recursive_source_includes):
    statement = f"select 1; !source {recursive_source_includes.path.as_posix()};"

    reader = SQLReader(query=statement, files=None)._input_reader  # noqa: SLF001

    assert next(reader) == (None, StatementType.STATEMENT, "select 1;")
    assert next(reader) == (None, StatementType.STATEMENT, "1;")
    assert next(reader) == (None, StatementType.STATEMENT, "2;")
    assert next(reader) == (None, StatementType.STATEMENT, "3;")

    error, accumulator, statement = next(reader)
    assert isinstance(error, str) and error.startswith("Recursion detected for file")
    assert statement is None
    assert accumulator == StatementType.COMMAND

    assert next(reader) == (None, StatementType.STATEMENT, "FINAL;")

    with pytest.raises(StopIteration):
        next(reader)


def test_compilation_success_no_source():
    query = "select 1; select 2; select 3;"

    reader = SQLReader(query=query, files=None)

    errors, stmt_count, compiled_statemetns = reader.compile_statements([])

    assert not errors, errors
    assert stmt_count == 3, stmt_count
    assert compiled_statemetns == ["select 1;", "select 2;", "select 3;"]


def test_compilation_empty_input():
    query = " "
    reader = SQLReader(query=query, files=None)
    errors, stmt_count, compiled_statements = reader.compile_statements([])
    assert not errors, errors
    assert stmt_count == 0
    assert not compiled_statements, compiled_statements


def test_compilation_success_with_source(no_recursion_includes):
    reader = SQLReader(query=None, files=no_recursion_includes)
    errors, stmt_count, compiled_statements = reader.compile_statements([])

    assert not errors
    assert stmt_count == 3
    assert compiled_statements == ["select 1;", "select 2;", "FINAL;"]


def test_comilation_success_with_source(single_select_1_file):
    query = f"select 73; !source {single_select_1_file.as_posix()}; select 42;"

    reader = SQLReader(query=query, files=None)

    errors, stmt_count, compiled_statements = reader.compile_statements([])
    assert not errors
    assert stmt_count == 3
    assert compiled_statements == ["select 73;", "select 1;", "select 42;"]


def test_compilation_recursion_error(recursive_source_includes):
    query = f"select 1; !source {recursive_source_includes.path.as_posix()}"

    reader = SQLReader(query=query, files=None)

    errors, stmt_count, _ = reader.compile_statements([])
    assert errors is not None
    assert all(e.startswith("Recursion detected for file") for e in errors)
    assert stmt_count == 5


@pytest.mark.usefixtures("cli_context_for_sql_compilation")
def test_compilation_operators_error():
    query = "select 1; select &{foo}; select 3;"
    reader = SQLReader(query=query, files=None)
    errors, stmt_count, compiled_statements = reader.compile_statements(
        (snowflake_sql_jinja_render,)
    )
    assert errors == ["SQL template rendering error: 'foo' is undefined"], errors
    assert stmt_count == 3
    assert compiled_statements == ["select 1;", "select 3;"]


def test_stmt_count_with_errors(tmp_path_factory):
    f = tmp_path_factory.mktemp("data") / "f.sql"
    query = f"select 1; !source {f};"
    f.write_text(query)

    reader = SQLReader(query=query + query, files=None)
    errors, stmt_count, _ = reader.compile_statements(())
    assert errors, errors
    assert len(errors) == 2
    assert stmt_count == 4


@pytest.mark.parametrize(
    "source, expected",
    (
        pytest.param("!source", True, id="lowercase command"),
        pytest.param("!SOURCE", True, id="uppercase command"),
        pytest.param("!sOuRCe", True, id="mixed case command"),
        pytest.param(" !source", True, id="leading spaces"),
        pytest.param("! source", False, id="space between ! and command"),
    ),
)
def test_source_command_detection(source, expected, single_select_1_file):
    statement = f"{source} {single_select_1_file.as_posix()}"
    is_source, _ = SQLReader._check_for_source_command(statement)  # noqa: SLF001

    assert is_source is expected
