from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.console.abc import AbstractConsole


def test_console_base_class(capsys):
    class TConsole(AbstractConsole):
        def phase(self, message: str):
            print(message)

        def step(self, message: str):
            print(message)

        def error(self, message: str):
            print(message)

    console = TConsole(cli_context=cli_context)
    assert not console.is_silent

    console.phase("a")
    console.step("b")
    console.error("c")

    out, _ = capsys.readouterr()
    assert out == "a\nb\nc\n"
