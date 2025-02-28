import pytest
from snowflake.cli._plugins.sql.manager import (
    IS_COMMAND,
    IS_STATEMENT,
    SQLReader,
)


def test_source_recursion_detection_from_files(recursive_source_includes):
    reader = SQLReader(None, recursive_source_includes)
    raw_statements = reader._recursive_file_reader(  # noqa: SLF001
        recursive_source_includes, set()
    )

    assert next(raw_statements) == (None, IS_STATEMENT, "1;")
    assert next(raw_statements) == (None, IS_STATEMENT, "2;")
    assert next(raw_statements) == (None, IS_STATEMENT, "3;")

    error, accumulator, statement = next(raw_statements)
    assert isinstance(error, str)
    assert error.startswith("Recursion detected for file")
    assert accumulator == IS_COMMAND
    assert statement is None

    assert next(raw_statements) == (None, IS_STATEMENT, "FINAL;")

    with pytest.raises(StopIteration):
        next(raw_statements)


def test_source_recursion_detection_from_query(recursive_source_includes):
    query = f"select 1; !source {recursive_source_includes.path.as_posix()};"
    reader = SQLReader(query=query, files=None)
    raw_statements = reader._input_reader  # noqa: SLF001

    assert next(raw_statements) == (None, IS_STATEMENT, "select 1;")
    assert next(raw_statements) == (None, IS_STATEMENT, "1;")
    assert next(raw_statements) == (None, IS_STATEMENT, "2;")
    assert next(raw_statements) == (None, IS_STATEMENT, "3;")

    error, accumulator, statement = next(raw_statements)
    assert isinstance(error, str)
    assert error.startswith("Recursion detected for file")
    assert accumulator == IS_COMMAND

    assert next(raw_statements) == (None, IS_STATEMENT, "FINAL;")

    assert statement is None

    with pytest.raises(StopIteration):
        next(raw_statements)


def test_source_dispatcher_statement():
    statement = "select 1;"

    reader = SQLReader(query="n/a", files=None)
    dispatcher = reader._command_dispatcher(statement)  # noqa: SLF001

    assert next(dispatcher) == (None, IS_STATEMENT, "select 1;")

    with pytest.raises(StopIteration):
        next(dispatcher)


def test_source_dispatcher_with_source_from_input(recursive_source_includes):
    statement = f"select 1; !source {recursive_source_includes.path.as_posix()};"

    reader = SQLReader(query=statement, files=None)._input_reader  # noqa: SLF001

    assert next(reader) == (None, IS_STATEMENT, "select 1;")
    assert next(reader) == (None, IS_STATEMENT, "1;")
    assert next(reader) == (None, IS_STATEMENT, "2;")
    assert next(reader) == (None, IS_STATEMENT, "3;")

    error, accumulator, statement = next(reader)
    assert isinstance(error, str) and error.startswith("Recursion detected for file")
    assert statement is None
    assert accumulator == IS_COMMAND

    assert next(reader) == (None, IS_STATEMENT, "FINAL;")

    with pytest.raises(StopIteration):
        next(reader)


def test_compilation_success_no_source():
    query = "select 1; select 2; select 3;"

    reader = SQLReader(query=query, files=None)

    errors, stmt_count, compiled_statemetns = reader.compile_statements([])

    assert not errors, errors
    assert stmt_count == 3, stmt_count
    assert compiled_statemetns == ["select 1;", "select 2;", "select 3;"]


def test_comilation_success_with_source(single_select_1_file):
    query = f"select 73; !source {single_select_1_file.as_posix()}; select 42;"

    reader = SQLReader(query=query, files=None)

    errors, stmt_count, compiled_statements = reader.compile_statements([])
    assert not errors
    assert stmt_count == 3
    assert compiled_statements == ["select 73;", "select 1;", "select 42;"]


def test_compilation_error(recursive_source_includes):
    query = f"select 1; !source {recursive_source_includes.path.as_posix()}"

    reader = SQLReader(query=query, files=None)

    errors, stmt_count, compiled_statements = reader.compile_statements([])
    assert errors is not None
    assert stmt_count == 5


def test_rec_count(tmp_path_factory):
    f = tmp_path_factory.mktemp("data") / "f.sql"
    query = f"select 1; !source {f};"
    f.write_text(query)

    reader = SQLReader(query=query + query, files=None)
    errors, stmt_count, compiled_statements = reader.compile_statements(())
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
