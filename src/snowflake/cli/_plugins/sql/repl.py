from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding.key_bindings import KeyBindings
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from snowflake.cli._app.printing import print_result
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli.api.output.types import MultipleResults, QueryResult
from snowflake.cli.api.secure_path import SecurePath

from .lexer import SQL_KEYWORDS

HISTORY_FILE = SecurePath("~/.snowflake/repl_history").path.expanduser()
EXIT_KEYWORDS = ("exit", "quit")

console = Console()
exit_prompt = Prompt("[bold red]Do you want to exit?", choices=["y", "n"])
sql_completer = WordCompleter(SQL_KEYWORDS, ignore_case=True)

key_bindings = KeyBindings()


@key_bindings.add("enter")
def _(event):
    buffer = event.app.current_buffer
    if buffer.text:
        if not buffer.text.endswith("\n"):
            buffer.insert_text("\n")
        if buffer.text.strip().endswith(";"):
            event.app.exit(result=buffer.text.strip())
    else:
        event.app.exit(result=buffer.text)


class Repl:
    prompt_session: PromptSession
    sql_manager: SqlManager
    data: dict
    retain_comments: bool

    def __init__(
        self,
        sql_manager: SqlManager,
        data: dict | None = None,
        retain_comments: bool = False,
    ):
        super().__init__()
        self.sql_manager = sql_manager
        self.data = data or {}
        self.retain_comments = retain_comments

    @property
    def _welcome_banner(self) -> Panel:
        return Panel(
            f"[bold cyan]Welcome to Snowflake-CLI REPL PoC\n[italic teal]Type 'exit' or 'quit' to leave",
            style="bold",
        )

    def run(self):
        try:
            self.prompt_session = PromptSession(
                history=FileHistory(HISTORY_FILE),
                lexer=PygmentsLexer(SqlLexer),
                completer=sql_completer,
                multiline=True,
                wrap_lines=True,
                key_bindings=key_bindings,
            )
            console.print(self._welcome_banner)
            self._repl_loop()
        except (KeyboardInterrupt, EOFError):
            console.print("\n[bold orange]Leaving REPL bye ...")

    def _repl_loop(self):
        while True:
            try:
                user_input = self.prompt_session.prompt(" > ").strip()

                if not user_input.strip():
                    continue

                if user_input.lower() in EXIT_KEYWORDS:
                    break

            except KeyboardInterrupt:  # a.k.a Ctrl-C
                continue

            except EOFError:  # a.k.a Ctrl-D
                if exit_prompt() == "y":
                    break

            except Exception as e:
                console.print(f"[bold orange] Error occurred: {e}")

            else:
                _, cursors = self.sql_manager.execute(
                    query=user_input,
                    files=None,
                    std_in=False,
                    data=self.data,
                    retain_comments=self.retain_comments,
                )
                print_result(MultipleResults(QueryResult(c) for c in cursors))
