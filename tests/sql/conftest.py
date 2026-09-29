import typing as t
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli._plugins.sql.repl import Repl
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.models import ProjectEnvironment


@pytest.fixture(name="cli_context_for_sql_compilation")
def make_cli_context():
    with mock.patch(
        "snowflake.cli.api.rendering.sql_templates.get_cli_context"
    ) as cli_context:
        cli_context().template_context = {
            "ctx": {"env": ProjectEnvironment(default_env={}, override_env={})}
        }
        yield cli_context()


@pytest.fixture(name="no_command_files")
def make_no_commands_files(
    tmp_path_factory,
) -> t.Generator[tuple[Path, Path], None, None]:
    """f1 does not include any other files. f2 does not include any other files."""
    tdf = tmp_path_factory.mktemp("no_commands")

    fh1 = tdf / "f1.sql"
    fh2 = tdf / "f2.sql"

    fh1.write_text(
        dedent(
            """
            f1: select 1;
            f1: select 2;
        """
        )
    )
    fh2.write_text(
        dedent(
            """
            f2: select 1;
            f2: select 2;
        """
        )
    )

    yield (fh1, fh2)


@pytest.fixture(name="recursive_source_includes")
def make_recursive_source_includes(
    tmp_path_factory,
) -> t.Generator[SecurePath, None, None]:
    """f1 includes f2. f2 includes f3. f3 includes f1."""
    f1 = tmp_path_factory.mktemp("data") / ("f1.txt")
    f2 = tmp_path_factory.mktemp("data") / ("f2.txt")
    f3 = tmp_path_factory.mktemp("data") / ("f3.txt")

    f1.write_text(f"1; !source {f2};")
    f2.write_text(f"2; !source {f3};")
    f3.write_text(f"3; !source {f1}; FINAL;")

    yield SecurePath(f1)


@pytest.fixture(name="no_recursion_includes")
def make_no_recursion_includes(tmp_path_factory):
    """f1 includes f2."""
    f1 = tmp_path_factory.mktemp("data") / ("f1.txt")
    f2 = tmp_path_factory.mktemp("data") / ("f2.txt")

    f1.write_text(f"select 1; !source {f2}; FINAL;")
    f2.write_text("select 2;")

    yield (f1,)


@pytest.fixture(name="single_select_1_file")
def make_single_select_1_file(tmp_path_factory) -> t.Generator[Path, None, None]:
    fh = tmp_path_factory.mktemp("data") / "single_select_1.sql"
    fh.write_text("select 1;")
    yield fh


@pytest.fixture(name="repl")
def make_repl(mock_cursor):
    mocked_cursor = [
        mock_cursor(
            rows=[("1",)],
            columns=["1"],
        ),
    ]

    with mock.patch.object(SqlManager, "_execute_string", return_value=mocked_cursor):
        manager = SqlManager()
        repl = Repl(manager)

        setattr(repl, "_history", mock.Mock())
        repl.history.get_strings.return_value = ["SELECT 1;", "SELECT 2;"]

        repl.session.prompt = mock.Mock(return_value="mocked_prompt_result")

        yield repl


@pytest.fixture(name="compiling_repl")
def make_compiling_repl(mock_cursor):
    """REPL whose SqlManager compiles input for real but never talks to Snowflake.

    Needed for cases where the expected results count must come from the real
    statement compiler instead of a stubbed `_execute`.
    """
    mocked_cursors = [mock_cursor(rows=[("1",)], columns=["1"])]
    connection = mock.MagicMock()
    connection.cursor.return_value.sfqid = "01b0-async"

    with mock.patch.object(SqlManager, "_execute_string", return_value=mocked_cursors):
        repl = Repl(SqlManager(connection=connection))
        repl.session.prompt = mock.Mock()

        yield repl


def run_repl(repl, user_inputs, monotonic_values=(0.0, 0.5)):
    """Drives one REPL session over `user_inputs` with a deterministic clock."""
    with mock.patch.object(repl, "_initialize_connection"), mock.patch(
        "snowflake.cli._plugins.sql.repl.time.monotonic",
        side_effect=monotonic_values,
    ), mock.patch.object(repl.session, "prompt", side_effect=iter(user_inputs)):
        repl.run()


@contextmanager
def sql_output_settings(output_format, silent):
    """Applies output settings to the CLI context and restores the previous ones."""
    manager = get_cli_context_manager()
    previous_format = manager.output_format
    previous_silent = manager.silent
    manager.output_format = output_format
    manager.silent = silent
    try:
        yield
    finally:
        manager.output_format = previous_format
        manager.silent = previous_silent
