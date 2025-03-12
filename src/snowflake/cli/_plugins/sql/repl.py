from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.syntax import Syntax
from snowflake.cli.api.secure_path import SecurePath

from .lexer import SQL_KEYWORDS

HISTORY_FILE = SecurePath("~/.snowflake/repl_history").path.expanduser()
EXIT_KEYWORDS = ("exit", "quit")

console = Console()
exit_prompt = Prompt("[bold red]Do you want to exit?", choices=["y", "n"])
sql_completer = WordCompleter(SQL_KEYWORDS, ignore_case=True)


class Repl:
    prompt_session: PromptSession

    def run(self):
        try:
            self.prompt_session = PromptSession(
                history=FileHistory(HISTORY_FILE),
                completer=sql_completer,
            )
            self._repl_loop()
        except (KeyboardInterrupt, EOFError):
            console.print("\n[bold orange]Leaving REPL bye ...")

    @property
    def _welcome_banner(self) -> Panel:
        return Panel(
            f"[bold cyan]Welcome to Snowflake-CLI REPL PoC\n[italic teal]Type 'exit' or 'quit' to leave",
            style="bold",
        )

    def _repl_loop(self):
        console.print(self._welcome_banner)

        while True:
            try:
                user_input = self.prompt_session.prompt(" > ").strip()

                if user_input.lower() in EXIT_KEYWORDS:
                    break

                highlighted = Syntax(user_input, "sql", line_numbers=False)

                console.print(highlighted)

            except KeyboardInterrupt:  # a.k.a Ctrl-C
                continue

            except EOFError:  # a.k.a Ctrl-D
                if exit_prompt() == "y":
                    break

            except Exception as e:
                console.print(f"[bold orange] Error occurred: {e}")
