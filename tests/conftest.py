from __future__ import annotations

import functools
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from typer import Typer
from typer.testing import CliRunner

TEST_DIR = Path(__file__).parent


@pytest.fixture(scope="session")
def test_snowcli_config():
    test_config = TEST_DIR / "test.toml"
    with NamedTemporaryFile(suffix=".toml", mode="w+") as fh:
        fh.write(test_config.read_text())
        fh.flush()
        yield Path(fh.name)


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer, test_snowcli_config: str):
        super().__init__()
        self.app = app
        self.test_snowcli_config = test_snowcli_config

    @functools.wraps(CliRunner.invoke)
    def invoke(self, *a, **kw):
        return super().invoke(self.app, *a, **kw)

    def invoke_with_config(self, *args, **kwargs):
        return self.invoke(
            ["--config-file", self.test_snowcli_config, *args[0]], **kwargs
        )


@pytest.fixture(scope="function")
def runner(test_snowcli_config):
    from snowcli.cli import app

    return SnowCLIRunner(app, test_snowcli_config)
