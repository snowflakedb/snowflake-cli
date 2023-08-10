from __future__ import annotations

import logging

import typer

app = typer.Typer()
log = logging.getLogger(__name__)


@app.command("say-hello")
def say_hello(
    your_name: str = typer.Argument(
        ...,
        help="Your name",
    ),
):
    """Say hello"""
    print(f"Hello {your_name}")
