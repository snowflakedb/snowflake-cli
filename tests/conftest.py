from __future__ import annotations

import functools

import pytest
from snowcli.cli import app
from typer import Typer
from typer.testing import CliRunner


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer):
        super().__init__()
        self.app = app

    @functools.wraps(CliRunner.invoke)
    def invoke(self, *a, **kw):
        return super().invoke(self.app, *a, **kw)


@pytest.fixture
def runner():
    return SnowCLIRunner(app)
