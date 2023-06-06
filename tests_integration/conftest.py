from __future__ import annotations

import functools
import pytest
import toml

from pathlib import Path
from snowcli.cli import app
from tempfile import NamedTemporaryFile
from typer import Typer
from typer.testing import CliRunner

TEST_DIR = Path(__file__).parent


@pytest.fixture(scope="session")
def test_snowcli_config():
    test_config = TEST_DIR / "config/test.toml"
    config = toml.load(test_config)
    config["snowsql_config_path"] = str(TEST_DIR / "config/connection_configs.toml")
    with NamedTemporaryFile(suffix=".toml", mode="w+") as fh:
        toml.dump(config, fh)
        fh.flush()
        yield fh.name


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


@pytest.fixture
def runner(test_snowcli_config):
    return SnowCLIRunner(app, test_snowcli_config)
