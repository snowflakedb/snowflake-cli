from textwrap import dedent

import pytest
from snowflake.cli._plugins.sql.manager import SqlManager, SQLReader


def test_source_recursion_detection_from_files(recursive_source_includes):
    reader = SQLReader(None, recursive_source_includes)
    raw_statements = reader._recursive_file_reader(  # noqa: SLF001
        recursive_source_includes, set()
    )

    assert next(raw_statements) == (None, "1")
    assert next(raw_statements) == (None, "2")
    assert next(raw_statements) == (None, "3")

    error, statement = next(raw_statements)
    assert isinstance(error, str)
    assert error.startswith("Recursion detected for file")
    assert statement is None

    with pytest.raises(StopIteration):
        next(raw_statements)


def test_source_recursion_detection_from_query(recursive_source_includes):
    query = f"select 1; !source {recursive_source_includes.path.as_posix()}"
    reader = SQLReader(query=query, files=None)
    raw_statements = reader._input_reader  # noqa: SLF001

    assert next(raw_statements) == (None, "select 1;")
    assert next(raw_statements) == (None, "1")
    assert next(raw_statements) == (None, "2")
    assert next(raw_statements) == (None, "3")

    error, statement = next(raw_statements)
    assert isinstance(error, str)
    assert error.startswith("Recursion detected for file")
    assert statement is None

    with pytest.raises(StopIteration):
        next(raw_statements)


def test_source_dispatcher_statement():
    statement = "select 1;"
    reader = SQLReader(query="n/a", files=None,)._command_dispatcher(  # noqa: SLF001
        statement,
    )

    assert next(reader) == (None, "select 1;")

    with pytest.raises(StopIteration):
        next(reader)


def test_source_dispatcher2_with_command(recursive_source_includes):
    statement = f"select 1; !source {recursive_source_includes.path.as_posix()}"

    reader = SqlManager.input_reader(statement)

    assert next(reader) == "select 1;"
    assert next(reader) == "1"
    assert next(reader) == "2"
    assert next(reader) == "3"

    with pytest.raises(RecursionError):
        next(reader)


def test_source_dispatcher_with_command(recursive_source_includes):
    statement = dedent(
        f"""select 1;
            !source {recursive_source_includes.path.as_posix()}
        """
    )

    reader = SqlManager.input_reader(statement)

    assert next(reader) == "select 1;"
    assert next(reader) == "1"
    assert next(reader) == "2"
    assert next(reader) == "3"

    with pytest.raises(RecursionError):
        next(reader)


def test_source_recursion_detection_from_input(recursive_source_includes):
    statement = dedent(
        f"""select 1;
            !source {recursive_source_includes.path.as_posix()}
        """
    )

    reader = SqlManager.input_reader(statement)

    assert next(reader) == "select 1;"
    assert next(reader) == "1"
    assert next(reader) == "2"
    assert next(reader) == "3"

    with pytest.raises(RecursionError):
        next(reader)


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
    is_source, _ = SqlManager.check_for_source_command(statement)

    assert is_source is expected
