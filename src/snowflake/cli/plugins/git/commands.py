import logging

from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
)

app = SnowTyper(
    name="git",
    help="Manages git repositories in Snowflake.",
)
log = logging.getLogger(__name__)


@app.command("draft1")
def draft_command1(**options) -> CommandResult:
    """Slot for actual command."""
    return MessageResult("Mornin'!")


@app.command("draft2")
def draft_command2(**options) -> CommandResult:
    """Slot for actual command."""
    return MessageResult("Nice day for fishin', innit?")
