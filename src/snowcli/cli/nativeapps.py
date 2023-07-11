from __future__ import annotations

import logging
import typer
from pathlib import Path
from typing import List, Optional
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS

from snowcli import config

app = typer.Typer(
    name="nativeapp",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage Native Apps in Snowflake",
)
log = logging.getLogger(__name__)


@app.command("dummy")
def nativeapp_dummy():
    """
    List streamlit apps.
    """
    log.info(f"Reached Dummy Command")
