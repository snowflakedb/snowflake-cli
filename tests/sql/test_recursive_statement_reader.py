from textwrap import dedent

import pytest
from snowflake.cli._plugins.sql.manager import SqlManager


def test_source_recursion_detection(recursive_source_includes):
    reader = SqlManager._recursive_file_reader(  # noqa: SLF001
        recursive_source_includes, set()
    )

    assert next(reader) == "1"
    assert next(reader) == "2"
    assert next(reader) == "3"

    with pytest.raises(RecursionError):
        next(reader)


def test_source_dispatcher_statement():
    statement = "select 1;"

    reader = SqlManager.source_dispatcher(statement, set())

    assert next(reader) == "select 1;"

    with pytest.raises(StopIteration):
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
