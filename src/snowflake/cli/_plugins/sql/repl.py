import sys
from contextlib import contextmanager
from io import StringIO
from logging import getLogger
from pathlib import Path
from typing import IO, Iterable, TextIO

from prompt_toolkit import PromptSession
from prompt_toolkit.filters import Condition, is_done, is_searching
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding.key_bindings import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.lexers import PygmentsLexer
from snowflake.cli._app.printing import print_result
from snowflake.cli._plugins.sql.lexer import CliLexer, cli_completer
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli._plugins.sql.repl_commands import detect_command
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.config import get_config_manager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import MultipleResults, QueryResult
from snowflake.cli.api.rendering.sql_templates import SQLTemplateSyntaxConfig
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.cursor import SnowflakeCursor

log = getLogger(__name__)


def _get_history_file():
    """Get history file path with lazy evaluation to avoid circular imports."""
    return SecurePath(
        get_config_manager().file_path.parent / "repl_history"
    ).path.expanduser()


HISTORY_FILE = None  # Will be set lazily
EXIT_KEYWORDS = ("exit", "quit")

# History file path will be set when REPL is initialized


@contextmanager
def repl_context(repl_instance):
    """Context manager for REPL execution that handles CLI context registration."""
    context_manager = get_cli_context_manager()
    context_manager.is_repl = True
    context_manager.repl_instance = repl_instance

    try:
        yield
    finally:
        # Clean up REPL context
        context_manager.is_repl = False
        context_manager.repl_instance = None


class _TeeWriter:
    """Write to both original stream and a capture buffer."""

    def __init__(self, original: TextIO, capture: StringIO):
        self.original = original
        self.capture = capture

    def write(self, text: str) -> int:
        self.original.write(text)
        return self.capture.write(text)

    def flush(self) -> None:
        self.original.flush()


class Repl:
    """Basic REPL implementation for the Snowflake CLI."""

    def __init__(
        self,
        sql_manager: SqlManager,
        data: dict | None = None,
        retain_comments: bool = False,
        template_syntax_config: SQLTemplateSyntaxConfig = SQLTemplateSyntaxConfig(),
    ):
        """Requires a `SqlManager` instance to execute queries.

        'pass through' variables for SqlManager.execute method:
        `data` should contain the variables used for template processing,
        `retain_comments` how to handle comments in queries
        """
        super().__init__()
        self._data = data or {}
        self._retain_comments = retain_comments
        self._template_syntax_config = template_syntax_config
        self._history = FileHistory(_get_history_file())
        self._lexer = PygmentsLexer(CliLexer)
        self._completer = cli_completer
        self._repl_key_bindings = self._setup_key_bindings()
        self._yes_no_keybindings = self._setup_yn_key_bindings()
        self._sql_manager = sql_manager
        self.session = PromptSession(history=self._history)
        self._next_input: str | None = None
        self._spool_file: IO[str] | None = None
        self._spool_path: Path | None = None

    def _setup_key_bindings(self) -> KeyBindings:
        """Key bindings for repl. Helps detecting ; at end of buffer."""
        kb = KeyBindings()

        @Condition
        def not_searching():
            return not is_searching()

        @kb.add(Keys.BracketedPaste)
        def _(event):
            """Handle bracketed paste - normalize line endings and strip trailing whitespace."""
            pasted_data = event.data
            # Normalize line endings: \r\n -> \n, \r -> \n
            normalized_data = pasted_data.replace("\r\n", "\n").replace("\r", "\n")
            # Strip trailing whitespace
            cleaned_data = normalized_data.rstrip()
            buffer = event.app.current_buffer
            buffer.insert_text(cleaned_data)
            log.debug(
                "handled paste operation, normalized line endings and stripped trailing whitespace"
            )

        @kb.add(Keys.Enter, filter=not_searching)
        def _(event):
            """Handle Enter key press with intelligent execution logic.

            Execution priority:
            1. Exit keywords (exit, quit) - execute immediately
            2. REPL commands (starting with !) - execute immediately
            3. SQL with trailing semicolon - execute immediately
            4. All other input - add new line for multi-line editing
            """
            buffer = event.app.current_buffer
            buffer_text = buffer.text
            stripped_text = buffer_text.strip()

            if stripped_text:
                log.debug("evaluating repl input")
                cursor_position = buffer.cursor_position
                ends_with_semicolon = stripped_text.endswith(";")
                is_command = detect_command(stripped_text) is not None

                meaningful_content_end = len(buffer_text.rstrip())
                cursor_at_meaningful_end = cursor_position >= meaningful_content_end

                if stripped_text.lower() in EXIT_KEYWORDS:
                    log.debug("exit keyword detected %r", stripped_text)
                    buffer.validate_and_handle()

                elif is_command:
                    log.debug("command detected, submitting input")
                    buffer.validate_and_handle()

                elif ends_with_semicolon and cursor_at_meaningful_end:
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

    def repl_prompt(self, msg: str = " > ") -> str:
        """Regular repl prompt with support for pre-filled input.

        Checks for queued input from commands like !edit and uses it as
        default text in the prompt. The queued input is cleared after use.
        """
        default_text = self._next_input

        try:
            return self.session.prompt(
                msg,
                lexer=self._lexer,
                completer=self._completer,
                multiline=True,
                wrap_lines=True,
                key_bindings=self._repl_key_bindings,
                default=default_text or "",
            )
        finally:
            if self._next_input == default_text:
                self._next_input = None

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
        return "Welcome to Snowflake-CLI REPL\nType 'exit' or 'quit' to leave"

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
            template_syntax_config=self._template_syntax_config,
        )
        return cursors

    def _print_and_spool(self, result: MultipleResults, user_input: str) -> None:
        """Print results to console and write to spool file if active."""
        if self.is_spooling:
            captured = StringIO()
            tee = _TeeWriter(sys.stdout, captured)
            sys.stdout = tee
            try:
                print_result(result)
            finally:
                sys.stdout = tee.original

            self.write_to_spool(f"\n-- Query: {user_input}\n")
            self.write_to_spool(captured.getvalue())
        else:
            print_result(result)

    def run(self):
        with repl_context(self):
            try:
                cli_console.panel(self._welcome_banner)
                self._initialize_connection()
                self._repl_loop()
            except (KeyboardInterrupt, EOFError):
                cli_console.message("\n[bold orange_red1]Leaving REPL, bye ...")
            finally:
                self.stop_spool()

    def _repl_loop(self):
        """Main REPL loop. Handles input and query execution.

        Sets up prompt session with history and key bindings.
        Honors Ctrl-C and Ctrl-D in REPL loop.
        """
        while True:
            try:
                user_input = self.repl_prompt().strip()

                if not user_input:
                    continue

                if user_input.lower() in EXIT_KEYWORDS:
                    raise EOFError

                try:
                    log.debug("executing query")
                    cursors = self._execute(user_input)
                    result = MultipleResults(QueryResult(c) for c in cursors)
                    self._print_and_spool(result, user_input)

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

    def set_next_input(self, text: str) -> None:
        """Set the text that will be used as the next REPL input."""
        self._next_input = text
        log.debug("Next input has been set")

    @property
    def next_input(self) -> str | None:
        """Get the next input text that will be used in the prompt."""
        return self._next_input

    @property
    def history(self) -> FileHistory:
        """Get the FileHistory instance used by the REPL."""
        return self._history

    @property
    def spool_path(self) -> Path | None:
        """Get the current spool file path, or None if not spooling."""
        return self._spool_path

    @property
    def is_spooling(self) -> bool:
        """Check if output is currently being spooled to a file."""
        return self._spool_file is not None

    def start_spool(self, path: Path) -> None:
        """Start spooling output to the specified file.

        If already spooling, closes the current file first.
        """
        if self._spool_file:
            self.stop_spool()

        secure_path = SecurePath(path)
        if not secure_path.exists():
            secure_path.touch() 
        self._spool_file = open(path, "w", encoding="utf-8")
        self._spool_path = path
        log.debug("Started spooling to: %s", path)

    def stop_spool(self) -> None:
        """Stop spooling output and close the spool file."""
        if self._spool_file:
            self._spool_file.close()
            log.debug("Stopped spooling to: %s", self._spool_path)
            self._spool_file = None
            self._spool_path = None

    def write_to_spool(self, text: str) -> None:
        """Write text to the spool file if spooling is active."""
        if self._spool_file:
            self._spool_file.write(text)
            self._spool_file.flush()

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
