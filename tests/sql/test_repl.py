import os
from unittest import mock

import pytest
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli._plugins.sql.repl import Repl
from snowflake.cli._plugins.sql.repl_commands import EditCommand
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError


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

        repl._history = mock.Mock()
        repl._history.get_strings.return_value = ["SELECT 1;", "SELECT 2;"]

        repl.session.prompt = mock.Mock(return_value="mocked_prompt_result")

        yield repl


def test_repl_input_handling(repl, capsys, os_agnostic_snapshot):
    user_inputs = iter(("select 1;", "exit", "y"))

    with mock.patch.object(
        repl.session,
        "prompt",
        side_effect=user_inputs,
    ):
        repl.run()

    output = capsys.readouterr().out
    os_agnostic_snapshot.assert_match(output)


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

        with mock.patch(repl_execute, return_value=mocked_cursor):
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
        context_manager.is_repl = True
        context_manager._repl_instance = repl
        yield
        context_manager.is_repl = False
        context_manager._repl_instance = None

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
        assert repl._next_input == "SELECT * FROM updated_users;"

    @mock.patch("click.edit")
    def test_edit_command_with_history_fallback(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command falls back to history when no content provided."""
        repl._history.get_strings.return_value = [
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
        assert repl._next_input == "SELECT * FROM updated_history;"

    @mock.patch("click.edit")
    def test_edit_command_skips_repl_commands_in_history(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command skips REPL commands when searching history."""
        repl._history.get_strings.return_value = [
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
        repl._history.get_strings.return_value = []
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

        assert repl._next_input is None

    @mock.patch("click.edit")
    def test_edit_command_editor_returns_empty_string(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command when editor returns empty content."""
        mock_click_edit.return_value = "   \n\n  "

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command.execute(mock_connection)

        assert repl._next_input is None

    @mock.patch("click.edit")
    def test_edit_command_strips_whitespace(
        self, mock_click_edit, edit_command, mock_connection, setup_repl_context, repl
    ):
        """Test !edit command strips leading/trailing whitespace."""
        mock_click_edit.return_value = "\n\n  SELECT * FROM test;  \n\n"

        with mock.patch.dict(os.environ, {"EDITOR": "vim"}):
            edit_command.execute(mock_connection)

        assert repl._next_input == "SELECT * FROM test;"

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

        # Verify that _next_input was set correctly
        assert repl._next_input == "SELECT * FROM integration_test;"

        # Mock the session.prompt to return the default text
        with mock.patch.object(repl.session, "prompt") as mock_prompt:
            mock_prompt.return_value = "SELECT * FROM integration_test;"
            repl.repl_propmpt("test > ")

        # Verify prompt was called with the correct default
        mock_prompt.assert_called_once()
        call_kwargs = mock_prompt.call_args[1]
        assert call_kwargs["default"] == "SELECT * FROM integration_test;"

        # Verify _next_input is cleared after use
        assert repl._next_input is None
