from logging import getLogger
from typing import Iterable

from prompt_toolkit import PromptSession
from prompt_toolkit.filters import Condition, is_done, is_searching
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
from snowflake.connector.config_manager import CONFIG_MANAGER
from snowflake.connector.cursor import SnowflakeCursor

log = getLogger(__name__)

HISTORY_FILE = SecurePath(
    CONFIG_MANAGER.file_path.parent / "repl_history"
).path.expanduser()
EXIT_KEYWORDS = ("exit", "quit")

log.debug("setting history file to: %s", HISTORY_FILE.as_posix())


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
        `data` should contain the variables used for template processing,
        `retain_comments` how to handle comments in queries
        """
        super().__init__()
        setattr(get_cli_context_manager(), "is_repl", True)
        self._data = data or {}
        self._retain_comments = retain_comments
        self._history = FileHistory(HISTORY_FILE)
        self._lexer = PygmentsLexer(CliLexer)
        self._completer = cli_completer
        self._repl_key_bindings = self._setup_key_bindings()
        self._yes_no_keybindings = self._setup_yn_key_bindings()
        self._sql_manager = sql_manager
        self.session = PromptSession(history=self._history)

    def _setup_key_bindings(self) -> KeyBindings:
        """Key bindings for repl. Helps detecting ; at end of buffer."""
        kb = KeyBindings()

        @Condition
        def not_searching():
            return not is_searching()

        @kb.add(Keys.Enter, filter=not_searching)
        def _(event):
            """Handle Enter key press."""
            buffer = event.app.current_buffer
            stripped_buffer = buffer.text.strip()

            if stripped_buffer:
                log.debug("evaluating repl input")
                cursor_position = buffer.cursor_position
                ends_with_semicolon = buffer.text.endswith(";")

                if stripped_buffer.lower() in EXIT_KEYWORDS:
                    log.debug("exit keyword detected %r", stripped_buffer)
                    buffer.validate_and_handle()

                elif ends_with_semicolon and cursor_position >= len(stripped_buffer):
                    log.debug("semicolon detected, submitting input")
                    buffer.validate_and_handle()

                else:
                    log.debug("adding new line")
                    buffer.insert_text("\n")
            else:
                log.debug("empty input")

        @kb.add(Keys.ControlJ)
        def _(event):
            """Control+J (and alias for Control + Enter) always inserts a new line."""
            event.app.current_buffer.insert_text("\n")

        return kb

    def _setup_yn_key_bindings(self) -> KeyBindings:
        """Key bindings for easy handling yes/no prompt."""
        kb = KeyBindings()

        @kb.add(Keys.Enter, filter=~is_done)
        @kb.add("c-d", filter=~is_done)
        @kb.add("y", filter=~is_done)
        def _(event):
            event.app.exit(result="y")

        @kb.add("n", filter=~is_done)
        @kb.add("c-c", filter=~is_done)
        def _(event):
            event.app.exit(result="n")

        @kb.add(Keys.Any)
        def _(event):
            pass

        return kb

    def repl_propmpt(self, msg: str = " > ") -> str:
        """Regular repl prompt."""
        return self.session.prompt(
            msg,
            lexer=self._lexer,
            completer=self._completer,
            multiline=True,
            wrap_lines=True,
            key_bindings=self._repl_key_bindings,
        )

    def yn_prompt(self, msg: str) -> str:
        """Yes/No prompt."""
        return self.session.prompt(
            msg,
            lexer=None,
            completer=None,
            multiline=False,
            wrap_lines=False,
            key_bindings=self._yes_no_keybindings,
        )

    @property
    def _welcome_banner(self) -> str:
        return f"Welcome to Snowflake-CLI REPL\nType 'exit' or 'quit' to leave"

    def _initialize_connection(self):
        """Early connection for possible fast fail."""
        cursor = self._execute("select current_version();")
        res = next(iter(cursor))
        log.debug("REPL: Snowflake version: %s", res.fetchall()[0][0])

    def _execute(self, user_input: str) -> Iterable[SnowflakeCursor]:
        """Executes a query and returns a list of cursors."""
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
        """Main REPL loop. Handles input and query execution.

        Sets up prompt session with history and key bindings.
        Honors Ctrl-C and Ctrl-D in REPL loop.
        """
        while True:
            try:
                user_input = self.repl_propmpt().strip()

                if not user_input:
                    continue

                if user_input.lower() in EXIT_KEYWORDS:
                    raise EOFError

                try:
                    log.debug("executing query")
                    cursors = self._execute(user_input)
                    print_result(MultipleResults(QueryResult(c) for c in cursors))

                except Exception as e:
                    log.debug("error occurred: %s", e)
                    cli_console.warning(f"\nError occurred: {e}")

            except KeyboardInterrupt:  # a.k.a Ctrl-C
                log.debug("user interrupted with Ctrl-C")
                continue

            except EOFError:  # a.k.a Ctrl-D
                log.debug("user interrupted with Ctrl-D")
                should_exit = self.ask_yn("Do you want to leave?")
                log.debug("user answered: %r", should_exit)
                if should_exit:
                    raise EOFError
                continue

            except Exception as e:
                cli_console.warning(f"\nError occurred: {e}")

    def ask_yn(self, question: str) -> bool:
        """Asks user a Yes/No question."""
        try:
            while True:
                log.debug("asking user: %s", question)
                answer = self.yn_prompt(f"{question} (y/n): ")
                log.debug("user answered: %s", answer)

                return answer == "y"

        except KeyboardInterrupt:
            log.debug("user interrupted with Ctrl-C returning to REPL")
            return False
