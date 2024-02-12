from contextlib import contextmanager

from snowflake.cli.api.console.abc import AbstractConsole


def test_console_base_class(capsys):
    class TConsole(AbstractConsole):
        @contextmanager
        def phase(self, enter_message: str, exit_message: str):
            print(enter_message)
            yield self.step
            print(exit_message)

        def step(self, message: str):
            print(message)

        def warning(self, message: str):
            print(message)

        def message(self, message: str):
            print(message)

    console = TConsole()
    assert not console.is_silent

    with console.phase("Enter", "Exit"):
        console.step("b")
        console.warning("c")
        console.message("d")

    out, _ = capsys.readouterr()
    assert out == "Enter\nb\nc\nd\nExit\n"
