import typer
from snowflake.cli.plugins.nativeapp.utils import is_tty_interactive


def interactive_callback(val):
    if val is None:
        return is_tty_interactive()
    return val


InteractiveOption = typer.Option(
    None,
    help=f"""When enabled, this option displays prompts even if the standard input and output are not terminal devices. Defaults to True in an interactive shell environment, and False otherwise.""",
    callback=interactive_callback,
    show_default=False,
)

ForceOption = typer.Option(
    False,
    "--force",
    help=f"""When enabled, this option causes the command to implicitly approve any prompts that arise.
    You should enable this option if interactive mode is not specified and if you want perform potentially destructive actions. Defaults to unset.""",
    is_flag=True,
)
