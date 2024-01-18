from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.console.abc import AbstractConsole


def test_console_base_class(capsys):
    class TConsole(AbstractConsole):
        def phase(self, message: str):
            print(message)

        def step(self, message: str):
            print(message)

    console = TConsole(print_fn=print, cli_context=cli_context)
    assert not console.is_silent
    assert not console.should_indent_output

    console.phase("a")
    console.step("b")
    out, _ = capsys.readouterr()
    assert out == "a\nb\n"
