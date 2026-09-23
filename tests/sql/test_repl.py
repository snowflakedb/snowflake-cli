import os
from textwrap import dedent
from types import SimpleNamespace
from unittest import mock

import pytest
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.keys import Keys
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli._plugins.sql.repl import Repl, _print_sql_elapsed
from snowflake.cli._plugins.sql.repl_commands import EditCommand
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.formats import OutputFormat

from tests.sql.conftest import run_repl, sql_output_settings


def test_execute_returns_expected_results_count_with_cursors(repl):
    expected_results_cnt, cursors = repl._execute("select 1;")  # noqa: SLF001

    assert expected_results_cnt == 1
    assert list(cursors)


def test_repl_input_handling(repl, capsys, os_agnostic_snapshot):
    user_inputs = iter(("select 1;", "exit", "y"))

    with mock.patch.object(
        repl.session,
        "prompt",
        side_effect=user_inputs,
    ), mock.patch(
        "snowflake.cli._plugins.sql.repl.time.monotonic",
        side_effect=(0.0, 0.123),
    ):
        repl.run()

    output = capsys.readouterr().out
    os_agnostic_snapshot.assert_match(output)


def test_repl_prints_one_elapsed_footer_per_input(repl, capsys, mock_cursor):
    """One input holding several statements is timed as a whole, not per statement."""
    cursors = [
        mock_cursor(rows=[("1",)], columns=["1"]),
        mock_cursor(rows=[("2",)], columns=["2"]),
    ]

    with mock.patch.object(repl, "_initialize_connection"), mock.patch.object(
        repl, "_execute", return_value=(len(cursors), cursors)
    ), mock.patch(
        "snowflake.cli._plugins.sql.repl.time.monotonic", side_effect=(0.0, 0.5)
    ), mock.patch.object(
        repl.session, "prompt", side_effect=iter(("select 1; select 2;", "exit", "y"))
    ):
        repl.run()

    output = capsys.readouterr().out
    assert output.count("Time Elapsed") == 1
    assert "Time Elapsed: 0.500s" in output


def test_repl_prints_elapsed_after_result_rendering_error(repl, capsys):
    """Statements ran before rendering blew up, so the timing is still reported."""
    with mock.patch(
        "snowflake.cli._plugins.sql.repl.print_result",
        side_effect=Exception("query failed"),
    ):
        run_repl(repl, ("select 1;", "exit", "y"))

    output = capsys.readouterr().out
    assert "Error occurred: query failed" in output
    assert "Time Elapsed: 0.500s" in output
    assert output.index("query failed") < output.index("Time Elapsed")


def test_repl_prints_elapsed_after_execution_error(repl, capsys):
    """Statements ran until execution blew up, so the warning is printed and then the timing."""

    def failing_cursors():
        raise Exception("execution failed")
        yield

    with mock.patch.object(repl, "_execute", return_value=(1, failing_cursors())):
        run_repl(repl, ("select 1;", "exit", "y"))

    output = capsys.readouterr().out
    assert "Error occurred: execution failed" in output
    assert "Time Elapsed: 0.500s" in output
    assert output.index("execution failed") < output.index("Time Elapsed")


def test_repl_skips_elapsed_when_compilation_fails(repl, capsys):
    """Compilation fails before a results count exists — nothing was timed."""
    run_repl(repl, ("select <% missing %>;", "exit", "y"))

    output = capsys.readouterr().out
    assert "Error occurred: SQL rendering error" in output
    assert "Time Elapsed" not in output


def test_repl_skips_elapsed_when_no_statements_found(repl, capsys):
    """A comment-only submission compiles to nothing — no query time to report."""
    run_repl(repl, ("-- just a comment", "exit", "y"))

    output = capsys.readouterr().out
    assert "Error occurred: No SQL statements found to execute." in output
    assert "Time Elapsed" not in output


def test_repl_skips_elapsed_for_empty_input(repl, capsys):
    """Empty and whitespace-only submissions are skipped before timing starts — nothing was timed."""
    run_repl(repl, ("", "   ", "exit", "y"))

    output = capsys.readouterr().out
    assert "Time Elapsed" not in output


def test_repl_skips_elapsed_for_repl_command_only_input(compiling_repl, capsys):
    """A `!command` runs no SQL, so there is no query time to report."""
    run_repl(compiling_repl, ("!queries help", "exit", "y"))

    assert "Time Elapsed" not in capsys.readouterr().out


def test_repl_skips_elapsed_for_async_only_input(compiling_repl, capsys):
    """An async statement only schedules work — the REPL never waits for it."""
    run_repl(compiling_repl, ("select 1;>", "exit", "y"))

    output = capsys.readouterr().out
    assert "01b0-async" in output
    assert "Time Elapsed" not in output


def test_repl_prints_one_elapsed_footer_for_mixed_sync_and_command_input(
    compiling_repl, capsys
):
    """One synchronous statement alongside a `!command` yields one footer."""
    run_repl(compiling_repl, ("select 1; !queries help;", "exit", "y"))

    output = capsys.readouterr().out
    assert output.count("Time Elapsed") == 1
    assert "Time Elapsed: 0.500s" in output


def test_repl_prints_one_elapsed_footer_for_mixed_sync_and_async_input(
    compiling_repl, capsys
):
    """A sync statement plus an async one still yields one footer — only sync waits."""
    run_repl(compiling_repl, ("select 1; select 2;>", "exit", "y"))

    output = capsys.readouterr().out
    assert "01b0-async" in output
    assert output.count("Time Elapsed") == 1
    assert "Time Elapsed: 0.500s" in output


def test_repl_times_only_the_query_that_survives_an_interrupt(
    repl, capsys, mock_cursor
):
    """Ctrl-C mid-query reports no time, and the next query is still timed."""
    cursors = [mock_cursor(rows=[("2",)], columns=["2"])]

    with mock.patch.object(
        repl, "_execute", side_effect=(KeyboardInterrupt, (len(cursors), cursors))
    ) as mocked_execute:
        run_repl(
            repl,
            ("select 1;", "select 2;", "exit", "y"),
            # The interrupted submission only consumes its start reading, so a
            # footer timed from it would read 10.000s rather than 0.250s.
            monotonic_values=(0.0, 10.0, 10.25),
        )

    output = capsys.readouterr().out
    assert mocked_execute.call_count == 2
    assert output.count("Time Elapsed") == 1
    assert "Time Elapsed: 0.250s" in output


def test_repl_prints_independent_elapsed_footers_for_successive_queries(repl, capsys):
    """Each submission gets its own clock; the second does not inherit the first."""
    run_repl(
        repl,
        ("select 1;", "select 2;", "exit", "y"),
        monotonic_values=(0.0, 0.1, 1.0, 1.4),
    )

    output = capsys.readouterr().out
    assert output.count("Time Elapsed") == 2
    assert "Time Elapsed: 0.100s" in output
    assert "Time Elapsed: 0.400s" in output
    assert output.index("Time Elapsed: 0.100s") < output.index("Time Elapsed: 0.400s")


def test_repl_skips_elapsed_when_rendering_is_interrupted(repl, capsys, mock_cursor):
    """Ctrl-C during result printing is not Exception, so no footer for that query."""
    cursors = [mock_cursor(rows=[("2",)], columns=["2"])]

    with mock.patch(
        "snowflake.cli._plugins.sql.repl.print_result",
        side_effect=(KeyboardInterrupt, None),
    ), mock.patch.object(repl, "_execute", return_value=(len(cursors), cursors)):
        run_repl(
            repl,
            ("select 1;", "select 2;", "exit", "y"),
            monotonic_values=(0.0, 10.0, 10.25),
        )

    output = capsys.readouterr().out
    assert output.count("Time Elapsed") == 1
    assert "Time Elapsed: 0.250s" in output


def test_repl_skips_elapsed_on_ctrl_d(repl, capsys):
    """Ctrl-D at the prompt is EOFError, so the loop prints no elapsed footer."""
    with mock.patch.object(repl, "_initialize_connection"), mock.patch.object(
        repl.session, "prompt", side_effect=EOFError
    ):
        repl.run()

    output = capsys.readouterr().out
    assert "Time Elapsed" not in output
    assert "Leaving REPL" in output


@pytest.mark.parametrize(
    "user_inputs",
    (
        pytest.param(("exit", "y"), id="exit"),
        pytest.param(("quit", "y"), id="quit"),
        pytest.param(("exit", "n", "exit", "y"), id="hesistate on exit"),
        pytest.param(("quit", "n", "quit", "y"), id="hesistate on quit"),
    ),
)
def test_exit_sequence(user_inputs, repl, os_agnostic_snapshot, capsys):
    user_inputs = iter(user_inputs)

    with mock.patch.object(
        repl.session,
        "prompt",
        side_effect=user_inputs,
    ):
        repl.run()

    output = capsys.readouterr().out
    os_agnostic_snapshot.assert_match(output)


def test_repl_clears_cached_failure_after_query_error(repl):
    """A transient connect failure mid-REPL must not poison the cache for
    the remainder of the session — otherwise every subsequent query would
    re-raise the cached exception until the user relaunches `snow sql`.
    """
    cache = get_cli_context_manager().connection_cache
    with mock.patch.object(repl, "_initialize_connection"), mock.patch.object(
        cache, "clear_failures"
    ) as mock_clear_failures, mock.patch.object(
        repl, "_execute", side_effect=Exception("transient connect blip")
    ), mock.patch.object(
        repl.session, "prompt", side_effect=iter(("select 1;", "exit", "y"))
    ):
        repl.run()

    mock_clear_failures.assert_called_once()


def test_repl_full_app(runner, os_agnostic_snapshot, mock_cursor):
    user_inputs = iter(("exit", "y"))
    mocked_cursor = [
        mock_cursor(
            rows=[("1",)],
            columns=("1",),
        ),
    ]

    repl_prompt = "snowflake.cli._plugins.sql.repl.PromptSession"
    repl_execute = "snowflake.cli._plugins.sql.repl.Repl._execute"

    with mock.patch(repl_prompt) as mock_prompt:
        mock_instance = mock.MagicMock()
        mock_instance.prompt.side_effect = user_inputs
        mock_prompt.return_value = mock_instance

        with mock.patch(repl_execute, return_value=(1, mocked_cursor)):
            result = runner.invoke(("sql",))
            assert result.exit_code == 0
            os_agnostic_snapshot.assert_match(result.output)


class TestEditCommand:
    """Test cases for the !edit REPL command."""

    @pytest.fixture
    def mock_connection(self):
        """Mock Snowflake connection."""
        return mock.Mock()

    @pytest.fixture
    def edit_command(self):
        """Create EditCommand instance."""
        return EditCommand()

    @pytest.fixture
    def edit_command_with_content(self):
        """Create EditCommand instance with SQL content."""
        return EditCommand(sql_content="SELECT * FROM users;")

    @pytest.fixture
    def setup_repl_context(self, repl):
        """Set up REPL context for testing."""
        context_manager = get_cli_context_manager()

        original_is_repl = context_manager.is_repl
        original_repl_instance = context_manager.repl_instance

        context_manager.is_repl = True
        context_manager.repl_instance = repl

        yield

        context_manager.is_repl = original_is_repl
        context_manager.repl_instance = original_repl_instance

    def test_edit_command_requires_repl_mode(self, edit_command, mock_connection):
        """Test that !edit command raises error when not in REPL mode."""
        context_manager = get_cli_context_manager()
        context_manager.is_repl = False

        with pytest.raises(CliError, match="can only be used in interactive mode"):
            edit_command.execute(mock_connection)

    def test_edit_command_requires_editor_env_var(
        self, edit_command, mock_connection, setup_repl_context
    ):
        """Test that !edit command raises error when EDITOR env var is not set."""
        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(CliError, match="No editor is set"):
                edit_command.execute(mock_connection)

    @mock.patch("click.edit")
    def test_edit_command_with_provided_content(
        self,
        mock_click_edit,
        edit_command_with_content,
        mock_connection,
        setup_repl_context,
        repl,
    ):
        """Test !edit command with provided SQL content."""
        mock_click_edit.return_value = "SELECT * FROM updated_users;"

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command_with_content.execute(mock_connection)

        mock_click_edit.assert_called_once_with(
            text="SELECT * FROM users;",
            editor="vim",
            extension=".sql",
            require_save=False,
        )
        assert repl.next_input == "SELECT * FROM updated_users;"

    @mock.patch("click.edit")
    def test_edit_command_with_history_fallback(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command falls back to history when no content provided."""
        repl.history.get_strings.return_value = [
            "!queries",
            "SELECT * FROM products;",
            "SELECT * FROM users;",
        ]
        mock_click_edit.return_value = "SELECT * FROM updated_history;"

        with mock.patch.dict(os.environ, {"EDITOR": "nano"}):
            edit_command.execute(mock_connection)

        mock_click_edit.assert_called_once_with(
            text="SELECT * FROM users;",
            editor="nano",
            extension=".sql",
            require_save=False,
        )
        assert repl.next_input == "SELECT * FROM updated_history;"

    @mock.patch("click.edit")
    def test_edit_command_skips_repl_commands_in_history(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command skips REPL commands when searching history."""
        repl.history.get_strings.return_value = [
            "!edit",
            "!queries",
            "SELECT * FROM actual_sql;",
            "!result",
        ]
        mock_click_edit.return_value = "SELECT * FROM edited_sql;"

        with mock.patch.dict(os.environ, {"EDITOR": "code"}):
            edit_command.execute(mock_connection)

        mock_click_edit.assert_called_once_with(
            text="SELECT * FROM actual_sql;",
            editor="code",
            extension=".sql",
            require_save=False,
        )

    @mock.patch("click.edit")
    def test_edit_command_empty_history(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command with empty history."""
        repl.history.get_strings.return_value = []
        mock_click_edit.return_value = "SELECT 1;"

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command.execute(mock_connection)

        mock_click_edit.assert_called_once_with(
            text="", editor="vim", extension=".sql", require_save=False
        )

    @mock.patch("click.edit")
    def test_edit_command_editor_returns_none(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command when editor is closed without changes."""
        mock_click_edit.return_value = None

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command.execute(mock_connection)

        assert repl.next_input is None

    @mock.patch("click.edit")
    def test_edit_command_editor_returns_empty_string(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command when editor returns empty content."""
        mock_click_edit.return_value = "   \n\n  "

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command.execute(mock_connection)

        assert repl.next_input is None

    @mock.patch("click.edit")
    def test_edit_command_strips_whitespace(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command strips leading/trailing whitespace."""
        mock_click_edit.return_value = "\n\n  SELECT * FROM test;  \n\n"

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command.execute(mock_connection)

        assert repl.next_input == "SELECT * FROM test;"

    def test_edit_command_from_args_with_content(self):
        """Test EditCommand.from_args with SQL content."""
        result = EditCommand.from_args("SELECT * FROM table;")

        assert result.command is not None
        assert result.command.sql_content == "SELECT * FROM table;"
        assert result.error_message is None

    def test_edit_command_from_args_no_content(self):
        """Test EditCommand.from_args with no content."""
        result = EditCommand.from_args("")

        assert result.command is not None
        assert result.command.sql_content == ""
        assert result.error_message is None

    def test_edit_command_from_args_multiple_words(self):
        """Test EditCommand.from_args with multiple words."""
        result = EditCommand.from_args("SELECT col1, col2 FROM table WHERE id > 1;")

        assert result.command is not None
        assert (
            result.command.sql_content == "SELECT col1, col2 FROM table WHERE id > 1;"
        )

    def test_edit_command_from_args_with_equals_in_sql(self):
        """Test EditCommand.from_args with SQL containing equals (should work with custom parser)."""
        result = EditCommand.from_args("SELECT col1, col2 FROM table WHERE id = 1;")

        # This should now work because EditCommand has custom parsing that doesn't treat '=' as key=value
        assert result.command is not None
        assert (
            result.command.sql_content == "SELECT col1, col2 FROM table WHERE id = 1;"
        )
        assert result.error_message is None

    @mock.patch("click.edit")
    def test_edit_command_integration_with_repl_prompt(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test integration: !edit command sets next input which is used by repl_prompt."""
        mock_click_edit.return_value = "SELECT * FROM integration_test;"

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command.execute(mock_connection)

        # Verify that next_input was set correctly
        assert repl.next_input == "SELECT * FROM integration_test;"

        # Mock the session.prompt to return the default text
        with mock.patch.object(repl.session, "prompt") as mock_prompt:
            mock_prompt.return_value = "SELECT * FROM integration_test;"
            repl.repl_prompt("test > ")

        # Verify prompt was called with the correct default
        mock_prompt.assert_called_once()
        call_kwargs = mock_prompt.call_args[1]
        assert call_kwargs["default"] == "SELECT * FROM integration_test;"

        # Verify _next_input is cleared after use
        assert repl.next_input is None


class TestReplPasteHandling:
    """Test cases for REPL paste handling functionality."""

    @pytest.fixture
    def mock_app_buffer(self):
        """Create a mock application with a buffer for testing key bindings."""
        buffer = Buffer()
        app = mock.MagicMock()
        app.current_buffer = buffer
        return app, buffer

    def _find_bracketed_paste_handler(self, key_bindings):
        """Find the bracketed paste handler from key bindings."""
        for binding in key_bindings.bindings:
            if binding.keys == (Keys.BracketedPaste,):
                return binding.handler
        raise AssertionError("BracketedPaste handler not found")

    def _find_enter_handler(self, key_bindings):
        """Find the Enter key handler from key bindings."""
        for binding in key_bindings.bindings:
            if binding.keys == (Keys.Enter,) and hasattr(binding, "filter"):
                return binding.handler
        raise AssertionError("Enter handler not found")

    def test_bracketed_paste_strips_trailing_newlines(self, repl, mock_app_buffer):
        """Test that bracketed paste strips trailing newlines from pasted content."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        sql_with_trailing_newlines = "SELECT * FROM table;\n\n\n"
        paste_event = mock.MagicMock()
        paste_event.app = app
        paste_event.data = sql_with_trailing_newlines

        paste_handler = self._find_bracketed_paste_handler(key_bindings)
        paste_handler(paste_event)

        expected_clean_sql = "SELECT * FROM table;"
        assert buffer.text == expected_clean_sql
        assert not buffer.text.endswith("\n")

    def test_bracketed_paste_handles_mixed_line_endings(self, repl, mock_app_buffer):
        """Test that bracketed paste handles both \\n and \\r\\n line endings."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        sql_with_mixed_endings = "SELECT 1;\r\nSELECT 2;\n\r\n\n"
        paste_event = mock.MagicMock()
        paste_event.app = app
        paste_event.data = sql_with_mixed_endings

        paste_handler = self._find_bracketed_paste_handler(key_bindings)
        paste_handler(paste_event)

        expected_normalized_sql = "SELECT 1;\nSELECT 2;"
        assert buffer.text == expected_normalized_sql
        assert not buffer.text.endswith(("\n", "\r"))

    def test_bracketed_paste_preserves_internal_newlines(self, repl, mock_app_buffer):
        """Test that internal newlines in pasted content are preserved."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        multiline_sql_with_trailing_newlines = (
            "SELECT\n  column1,\n  column2\nFROM table;\n\n"
        )
        paste_event = mock.MagicMock()
        paste_event.app = app
        paste_event.data = multiline_sql_with_trailing_newlines

        paste_handler = self._find_bracketed_paste_handler(key_bindings)
        paste_handler(paste_event)

        expected_multiline_sql = "SELECT\n  column1,\n  column2\nFROM table;"
        assert buffer.text == expected_multiline_sql

    def test_bracketed_paste_handles_carriage_return_only(self, repl, mock_app_buffer):
        """Test that bracketed paste handles \\r (carriage return only) line endings."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        sql_with_cr_endings = "SELECT 1;\rSELECT 2;\r\r"
        paste_event = mock.MagicMock()
        paste_event.app = app
        paste_event.data = sql_with_cr_endings

        paste_handler = self._find_bracketed_paste_handler(key_bindings)
        paste_handler(paste_event)

        expected_normalized_sql = "SELECT 1;\nSELECT 2;"
        assert buffer.text == expected_normalized_sql

    def test_enter_key_with_semicolon_at_meaningful_end(self, repl, mock_app_buffer):
        """Test Enter key behavior when cursor is at meaningful content end with semicolon."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        sql_with_trailing_whitespace = "SELECT 1;   \n  "
        meaningful_content = "SELECT 1;"
        buffer.text = sql_with_trailing_whitespace
        buffer.cursor_position = len(meaningful_content)
        buffer.validate_and_handle = mock.MagicMock()

        enter_event = mock.MagicMock()
        enter_event.app = app

        enter_handler = self._find_enter_handler(key_bindings)
        enter_handler(enter_event)

        buffer.validate_and_handle.assert_called_once()

    def test_enter_key_without_semicolon_adds_newline(self, repl, mock_app_buffer):
        """Test Enter key adds newline when no semicolon at meaningful content end."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        incomplete_sql = "SELECT 1"
        buffer.text = incomplete_sql
        buffer.cursor_position = len(buffer.text)

        enter_event = mock.MagicMock()
        enter_event.app = app

        enter_handler = self._find_enter_handler(key_bindings)
        enter_handler(enter_event)

        expected_sql_with_newline = "SELECT 1\n"
        assert buffer.text == expected_sql_with_newline

    def test_enter_key_with_cursor_in_middle_adds_newline(self, repl, mock_app_buffer):
        """Test Enter key adds newline when cursor is not at meaningful content end."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        complete_sql = "SELECT 1;"
        cursor_in_middle_position = 3
        buffer.text = complete_sql
        buffer.cursor_position = cursor_in_middle_position

        enter_event = mock.MagicMock()
        enter_event.app = app

        enter_handler = self._find_enter_handler(key_bindings)
        enter_handler(enter_event)

        expected_sql_with_newline_in_middle = "SEL\nECT 1;"
        assert buffer.text == expected_sql_with_newline_in_middle

    def test_enter_key_handles_exit_keywords(self, repl, mock_app_buffer):
        """Test Enter key handles exit keywords correctly."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        exit_command = "exit"
        buffer.text = exit_command
        buffer.cursor_position = len(buffer.text)
        buffer.validate_and_handle = mock.MagicMock()

        enter_event = mock.MagicMock()
        enter_event.app = app

        enter_handler = self._find_enter_handler(key_bindings)
        enter_handler(enter_event)

        buffer.validate_and_handle.assert_called_once()

    @pytest.mark.parametrize("exit_keyword", ["exit", "quit", "EXIT", "QUIT"])
    def test_enter_key_handles_all_exit_keywords(
        self, exit_keyword, repl, mock_app_buffer
    ):
        """Test Enter key handles all exit keywords case-insensitively."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        buffer.text = exit_keyword
        buffer.cursor_position = len(buffer.text)
        buffer.validate_and_handle = mock.MagicMock()

        enter_event = mock.MagicMock()
        enter_event.app = app

        enter_handler = self._find_enter_handler(key_bindings)
        enter_handler(enter_event)

        buffer.validate_and_handle.assert_called_once()

    def test_paste_and_enter_integration(self, repl, mock_app_buffer):
        """Test integration of paste handling followed by Enter key."""
        app, buffer = mock_app_buffer
        key_bindings = repl._setup_key_bindings()  # noqa: SLF001

        sql_query_with_trailing_newlines = "SELECT * FROM users WHERE id = 1;\n\n\n"
        paste_event = mock.MagicMock()
        paste_event.app = app
        paste_event.data = sql_query_with_trailing_newlines

        paste_handler = self._find_bracketed_paste_handler(key_bindings)
        paste_handler(paste_event)

        expected_clean_query = "SELECT * FROM users WHERE id = 1;"
        assert buffer.text == expected_clean_query

        buffer.cursor_position = len(buffer.text)
        buffer.validate_and_handle = mock.MagicMock()

        enter_event = mock.MagicMock()
        enter_event.app = app

        enter_handler = self._find_enter_handler(key_bindings)
        enter_handler(enter_event)

        buffer.validate_and_handle.assert_called_once()


@pytest.mark.parametrize(
    "command_args, env_var, config_value, ask_yn_called",
    [
        # test command arg
        (["--no-prompt-exit-repl"], {}, {}, False),
        # test env var
        ([], {"SNOWFLAKE_CLI_NO_PROMPT_EXIT_REPL": "true"}, {}, False),
        # test config value
        ([], {}, {"no_prompt_exit_repl": "true"}, False),
        # test default
        ([], {}, {}, True),
    ],
)
@mock.patch("snowflake.cli.api.config.get_config_section")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl.ask_yn")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl.repl_prompt")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl._initialize_connection")
def test_no_prompt_exit_repl(
    mock__initialize_connection,
    mock_repl_prompt,
    mock_ask_yn,
    mock_get_config_section,
    runner,
    command_args,
    env_var,
    config_value,
    ask_yn_called,
):
    mock_repl_prompt.side_effect = EOFError
    mock_ask_yn.return_value = True
    mock_get_config_section.return_value = config_value

    with mock.patch.dict(os.environ, env_var):
        runner.invoke(["sql", *command_args])

    assert mock_ask_yn.called is ask_yn_called


def test_repl_keeps_historical_default_prompt(repl):
    repl.session.prompt = mock.Mock(return_value="exit")
    repl.repl_prompt()
    assert repl.session.prompt.call_args.args[0] == " > "


def _config_with_prompt_format(prompt_format: str | None) -> str:
    config = dedent(
        """\
        [connections.default]
        database = "db_for_test"
        schema = "test_public"
        role = "test_role"
        warehouse = "xs"
        password = "dummy_password"
        """
    )
    if prompt_format is not None:
        config += f"\n[cli]\nprompt_format = {prompt_format}\n"
    return config


@pytest.mark.parametrize(
    "command_args, config_prompt_format, env_prompt_format, expected",
    [
        (["--prompt-format", "[user]>"], None, None, "[user]>"),
        ([], '"[database]>"', None, "[database]>"),
        (["--prompt-format", "[user]>"], '"[database]>"', None, "[user]>"),
        ([], None, None, None),
        # This option deliberately has no environment variable, so the
        # environment must never win over config.toml or supply a value.
        ([], '"[database]>"', "[schema]>", "[database]>"),
        ([], None, "[schema]>", None),
        (["--prompt-format", "[user]>"], None, "[schema]>", "[user]>"),
    ],
)
@mock.patch("snowflake.cli._plugins.sql.repl.Repl")
def test_prompt_format_sources(
    mock_repl_cls,
    runner,
    config_file,
    monkeypatch,
    command_args,
    config_prompt_format,
    env_prompt_format,
    expected,
):
    mock_repl_cls.return_value.run.return_value = None
    if env_prompt_format is None:
        monkeypatch.delenv("SNOWFLAKE_CLI_PROMPT_FORMAT", raising=False)
    else:
        monkeypatch.setenv("SNOWFLAKE_CLI_PROMPT_FORMAT", env_prompt_format)

    with config_file(_config_with_prompt_format(config_prompt_format)) as cfg:
        result = runner.invoke_with_config_file(cfg, ["sql", *command_args])

    assert result.exit_code == 0, result.output
    assert mock_repl_cls.call_args.kwargs["prompt_format"] == expected


def _connection_for_prompt(**overrides):
    values = dict(
        user="alice",
        host="host.example",
        account="acct",
        role="SYSADMIN",
        warehouse="WH",
        database="DB1",
        schema="PUBLIC",
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_repl_expands_configured_prompt_format(mock_cursor):
    mocked_cursor = [
        mock_cursor(rows=[("1",)], columns=["1"]),
    ]
    connection = _connection_for_prompt()
    with mock.patch.object(SqlManager, "_execute_string", return_value=mocked_cursor):
        repl = Repl(
            SqlManager(connection=connection), prompt_format="[user]@[database]>"
        )
        repl.session.prompt = mock.Mock(side_effect=["exit", "y"])
        repl.run()
    prompt_messages = [call.args[0] for call in repl.session.prompt.call_args_list]
    assert "alice@DB1>" in prompt_messages


@pytest.mark.parametrize(
    "attribute, statement, updated_value",
    [
        ("database", "USE DATABASE DB2;", "DB2"),
        ("warehouse", "USE WAREHOUSE WH2;", "WH2"),
        ("role", "USE ROLE SECURITYADMIN;", "SECURITYADMIN"),
        ("schema", "USE SCHEMA PRIVATE;", "PRIVATE"),
    ],
)
def test_next_repl_prompt_reflects_use_statement(
    mock_cursor, attribute, statement, updated_value
):
    mocked_cursor = [
        mock_cursor(rows=[("1",)], columns=["1"]),
    ]
    connection = _connection_for_prompt()
    original = getattr(connection, attribute)

    def execute_and_update_connection(sql_text, **kwargs):
        if "use " in sql_text.lower():
            setattr(connection, attribute, updated_value)
        return mocked_cursor

    with mock.patch.object(
        SqlManager, "_execute_string", side_effect=execute_and_update_connection
    ):
        repl = Repl(SqlManager(connection=connection), prompt_format=f"[{attribute}]>")
        repl.session.prompt = mock.Mock(side_effect=[statement, "exit", "y"])
        repl.run()

    prompt_messages = [call.args[0] for call in repl.session.prompt.call_args_list]
    assert prompt_messages[0] == f"{original}>"
    assert prompt_messages[1] == f"{updated_value}>"


def test_repl_prompt_uses_connection_from_latest_successful_query(mock_cursor):
    first = _connection_for_prompt(database="OLD")
    second = _connection_for_prompt(database="NEW")

    class _Manager:
        def __init__(self):
            self.connection = first
            self._calls = 0

        def execute(self, **kwargs):
            self._calls += 1
            if self._calls > 1:
                self.connection = second
            return 1, iter([mock_cursor(rows=[("1",)], columns=["1"])])

    repl = Repl(_Manager(), prompt_format="[database]>")
    repl.session.prompt = mock.Mock(side_effect=["select 1;", "exit", "y"])
    repl.run()

    prompt_messages = [call.args[0] for call in repl.session.prompt.call_args_list]
    assert prompt_messages[0] == "OLD>"
    assert prompt_messages[1] == "NEW>"


def test_prompt_format_rejects_non_string_config(runner, config_file):
    config = dedent(
        """\
        [connections.default]
        database = "db_for_test"
        schema = "test_public"
        role = "test_role"
        warehouse = "xs"
        password = "dummy_password"

        [cli]
        prompt_format = true
        """
    )
    with config_file(config) as cfg:
        result = runner.invoke_with_config_file(cfg, ["sql"])

    assert result.exit_code != 0
    assert "Expected a string for cli.prompt_format" in result.output


@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager")
def test_prompt_format_invalid_config_ignored_for_one_shot_inputs(
    mock_manager, runner, config_file, named_temporary_file
):
    mock_manager().execute.return_value = (0, [])
    config = dedent(
        """\
        [connections.default]
        database = "db_for_test"
        schema = "test_public"
        role = "test_role"
        warehouse = "xs"
        password = "dummy_password"

        [cli]
        prompt_format = true
        """
    )
    with named_temporary_file() as sql_file:
        sql_file.write_text("select 1")
        with config_file(config) as cfg:
            query_result = runner.invoke_with_config_file(
                cfg, ["sql", "-q", "select 1"]
            )
            file_result = runner.invoke_with_config_file(
                cfg, ["sql", "-f", str(sql_file)]
            )
            stdin_result = runner.invoke_with_config_file(
                cfg, ["sql", "--stdin"], input="select 1"
            )

    assert query_result.exit_code == 0, query_result.output
    assert file_result.exit_code == 0, file_result.output
    assert stdin_result.exit_code == 0, stdin_result.output


@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager")
def test_prompt_format_flag_ignored_for_one_shot_query(mock_manager, runner):
    mock_manager().execute.return_value = (0, [])
    result = runner.invoke(
        ["sql", "-q", "select 1", "--prompt-format", "[#ff00ff][user]>"]
    )
    assert result.exit_code == 0, result.output
    assert "not supported in this version" not in result.output


@mock.patch("snowflake.cli._plugins.sql.repl.PromptSession")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl._execute")
def test_prompt_format_uses_cli_connection_name(
    mock_execute, mock_prompt_session, runner, mock_cursor
):
    mock_execute.return_value = (1, iter([mock_cursor(["1"], ["1"])]))
    mock_prompt = mock.MagicMock()
    mock_prompt.prompt.side_effect = iter(("exit", "y"))
    mock_prompt_session.return_value = mock_prompt

    result = runner.invoke(["sql", "-c", "full", "--prompt-format", "[connection]>"])

    assert result.exit_code == 0, result.output
    assert mock_prompt.prompt.call_args_list[0].args[0] == "full>"


@pytest.mark.parametrize("colour_token", ["[#ff00ff]", "[bg:#00ff00]"])
@mock.patch("snowflake.cli._plugins.sql.repl.PromptSession")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl._execute")
def test_prompt_format_warns_and_drops_colour_token_in_repl(
    mock_execute, mock_prompt_session, runner, mock_cursor, colour_token
):
    mock_execute.return_value = (1, iter([mock_cursor(["1"], ["1"])]))
    mock_prompt = mock.MagicMock()
    mock_prompt.prompt.side_effect = iter(("exit", "y"))
    mock_prompt_session.return_value = mock_prompt

    result = runner.invoke(
        ["sql", "-c", "full", "--prompt-format", f"{colour_token}[connection]>"]
    )

    assert result.exit_code == 0, result.output
    assert colour_token in result.output
    assert "not supported in this version" in result.output
    assert mock_prompt.prompt.call_args_list[0].args[0] == "full>"


@mock.patch("snowflake.cli._plugins.sql.repl.PromptSession")
@mock.patch("snowflake.cli._plugins.sql.repl.Repl._execute")
def test_prompt_format_warns_and_drops_unknown_token_in_repl(
    mock_execute, mock_prompt_session, runner, mock_cursor
):
    mock_execute.return_value = (1, iter([mock_cursor(["1"], ["1"])]))
    mock_prompt = mock.MagicMock()
    mock_prompt.prompt.side_effect = iter(("exit", "y"))
    mock_prompt_session.return_value = mock_prompt

    result = runner.invoke(
        ["sql", "-c", "full", "--prompt-format", "[future-token][connection]>"]
    )

    assert result.exit_code == 0, result.output
    assert "[future-token]" in result.output
    assert mock_prompt.prompt.call_args_list[0].args[0] == "full>"


@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager")
def test_prompt_format_unknown_token_warning_skipped_for_one_shot(mock_manager, runner):
    mock_manager().execute.return_value = (0, [])
    result = runner.invoke(
        ["sql", "-q", "select 1", "--prompt-format", "[future-token]>"]
    )
    assert result.exit_code == 0, result.output
    assert "future-token" not in result.output


def test_print_sql_elapsed_table_not_silent(capsys):
    with sql_output_settings(OutputFormat.TABLE, silent=False):
        _print_sql_elapsed(0.123)

    assert "Time Elapsed: 0.123s" in capsys.readouterr().out


@pytest.mark.parametrize(
    "output_format",
    (OutputFormat.JSON, OutputFormat.JSON_EXT, OutputFormat.CSV),
)
def test_print_sql_elapsed_skips_non_table_formats(output_format, capsys):
    with sql_output_settings(output_format, silent=False):
        _print_sql_elapsed(0.123)

    assert "Time Elapsed" not in capsys.readouterr().out


def test_print_sql_elapsed_table_silent(capsys):
    with sql_output_settings(OutputFormat.TABLE, silent=True):
        _print_sql_elapsed(0.123)

    assert "Time Elapsed" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "output_format",
    (OutputFormat.JSON, OutputFormat.JSON_EXT, OutputFormat.CSV),
)
def test_repl_skips_elapsed_for_structured_output(output_format, repl, capsys):
    """A footer would corrupt machine-readable output, so the loop prints none."""
    with sql_output_settings(output_format, silent=False):
        run_repl(repl, ("select 1;", "exit", "y"), monotonic_values=(0.0, 0.123))

    assert "Time Elapsed" not in capsys.readouterr().out


def test_repl_skips_elapsed_when_silent(repl, capsys):
    """Silent mode hides the footer even though the table format allows it."""
    with sql_output_settings(OutputFormat.TABLE, silent=True):
        run_repl(repl, ("select 1;", "exit", "y"), monotonic_values=(0.0, 0.123))

    assert "Time Elapsed" not in capsys.readouterr().out
