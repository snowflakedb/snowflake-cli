from logging import getLogger
from typing import Iterable

from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding.key_bindings import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.lexers import PygmentsLexer
from snowflake.cli._app.printing import print_result
from snowflake.cli._plugins.sql.lexer import CliLexer, cli_completer
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import MultipleResults, QueryResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.cursor import SnowflakeCursor

log = getLogger(__name__)

HISTORY_FILE = SecurePath("~/.snowflake/repl_history").path.expanduser()
EXIT_KEYWORDS = ("exit", "quit")

log.debug("setting history file to: %s", HISTORY_FILE.as_posix())

repl_key_bindings = KeyBindings()


@repl_key_bindings.add(Keys.Enter)
def _(event):
    """Handle Enter key press in REPL.

    Detects `;` in multiline mode.
    """
    buffer = event.app.current_buffer
    log.debug("original REPL buffer content: %r", buffer.text)
    stripped_buffer = buffer.text.strip()

    if stripped_buffer:
        cursor_position = buffer.cursor_position
        ends_with_semicolon = buffer.text.rstrip().endswith(";")

        if stripped_buffer.lower() in EXIT_KEYWORDS:
            log.debug("exit keyword detected")
            buffer.validate_and_handle()

        elif ends_with_semicolon and cursor_position >= len(stripped_buffer):
            log.debug("Semicolon detected, executing query")
            buffer.validate_and_handle()

        else:
            log.debug("Adding empty line")
            buffer.insert_text("\n")
    else:
        buffer.validate_and_handle()


@repl_key_bindings.add(Keys.ControlJ)
def _(event):
    """Control+J (and alias for Control + Enter) always inserts a new line."""
    event.app.current_buffer.insert_text("\n")


yn_key_bindings = KeyBindings()


@yn_key_bindings.add(Keys.Enter)
def _(event):
    """Handle Enter key press in Yes/No prompt."""
    event.app.exit(result="y")


@yn_key_bindings.add("c-c")
def _(event):
    raise KeyboardInterrupt


@yn_key_bindings.add("y")
def _(event):
    event.app.exit(result="y")


@yn_key_bindings.add("n")
def _(event):
    event.app.exit(result="n")


class Repl:
    """Basic REPL implementation for the Snowflake CLI."""

    def __init__(
        self,
        sql_manager: SqlManager,
        data: dict | None = None,
        retain_comments: bool = False,
    ):
        """Requires a `SqlManager` instance to execute queries.

        'pass through' variables for SqlManager.execute method:
        `data` should contain the variablees used for template processing,
        `retain_comments` how to handle comments in queries
        """
        super().__init__()
        setattr(get_cli_context_manager(), "is_repl", True)
        self._sql_manager = sql_manager
        self._data = data or {}
        self._retain_comments = retain_comments
        self._session = PromptSession(
            history=FileHistory(HISTORY_FILE),
            lexer=PygmentsLexer(CliLexer),
            completer=cli_completer,
            multiline=True,
            wrap_lines=True,
            key_bindings=repl_key_bindings,
        )

    @property
    def session(self) -> PromptSession:
        return self._session

    def repl_propmpt(self, msg: str = " > ") -> str:
        return self.session.prompt(
            msg,
            lexer=PygmentsLexer(CliLexer),
            completer=cli_completer,
            multiline=True,
            wrap_lines=True,
            key_bindings=repl_key_bindings,
        )

    def yn_prompt(self, msg: str) -> str:
        return self.session.prompt(
            msg,
            lexer=None,
            completer=None,
            multiline=False,
            wrap_lines=False,
            key_bindings=yn_key_bindings,
        )

    @property
    def _welcome_banner(self) -> str:
        return f"Welcome to Snowflake-CLI REPL PoC\nType 'exit' or 'quit' to leave"

    def _initialize_connection(self):
        cursor = self._execute("select current_version();")
        res = next(iter(cursor))
        log.debug("REPL: Snowflake version: %s", res.fetchall()[0][0])

    def _execute(self, user_input: str) -> Iterable[SnowflakeCursor]:
        """Executes a query and returns a list of cursors."""
        # TODO: refactor SqlManager for different cursor types. VerboseCursor adds extre output to console
        _, cursors = self._sql_manager.execute(
            query=user_input,
            files=None,
            std_in=False,
            data=self._data,
            retain_comments=self._retain_comments,
        )
        return cursors

    def run(self):
        try:
            cli_console.panel(self._welcome_banner)
            self._initialize_connection()
            self._repl_loop()
        except (KeyboardInterrupt, EOFError):
            cli_console.message("\n[bold orange_red1]Leaving REPL, bye ...")

    def _repl_loop(self):
        """Main REPL loopl. Handles input and query execution.

        Sets up prompt session with history and key bindings.
        Honors Ctrl-C and Ctrl-D in REPL loop.
        """
        while True:
            try:
                user_input = self.repl_propmpt().strip()
                # log.debug("REPL user input: %r", user_input)

                if not user_input:
                    continue

                if user_input.lower() in EXIT_KEYWORDS:
                    log.debug("REPL exit keyword detected")
                    raise EOFError

                try:
                    log.debug("REPL: executing query")
                    cursors = self._execute(user_input)
                    print_result(MultipleResults(QueryResult(c) for c in cursors))

                except Exception as e:
                    log.debug("REPL: error occurred: %s", e)
                    cli_console.warning(f"\nError occurred: {e}")

            except KeyboardInterrupt:  # a.k.a Ctrl-C
                log.debug("REPL: user interrupted with Ctrl-C")
                continue

            except EOFError:  # a.k.a Ctrl-D
                log.debug("REPL: user interrupted with Ctrl-D")
                should_exit = self.ask_yn("Do you want to leave?")
                log.debug("User answered: %r", should_exit)
                if should_exit:
                    raise EOFError
                continue

            except Exception as e:
                cli_console.warning(f"\nError occurred: {e}")

    def ask_yn(self, question: str) -> bool:
        """Asks user a Yes/No question."""
        try:
            while True:
                log.debug("Asking user: %s", question)
                answer = self.yn_prompt(f"{question} (y/n): ")
                log.debug("User answered: %s", answer)

                return answer == "y"

        except KeyboardInterrupt:
            log.debug("User interrupted with Ctrl-C. Returning to REPL")
            return False
