import typer

from .. import __about__

app: typer.Typer = typer.Typer(
    name="coverage", context_settings={"help_option_names": ["-h", "--help"]}
)

from . import clear, report
