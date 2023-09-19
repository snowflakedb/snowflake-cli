from __future__ import annotations

import functools
import json
import tempfile
import shutil
import os

import pytest
from dataclasses import dataclass
from pathlib import Path
from snowcli.app.cli_app import app
from typer import Typer
from typer.testing import CliRunner
from typing import List, Dict, Any, Optional

TEST_DIR = Path(__file__).parent


@dataclass
class CommandResult:
    exit_code: int
    json: Optional[List[Dict[str, Any]] | Dict[str, Any]] = None
    output: Optional[str] = None


@pytest.fixture(scope="session")
def test_snowcli_config():
    test_config_name = "connection_configs.toml"
    test_config = TEST_DIR / "config" / test_config_name
    with tempfile.TemporaryDirectory() as td:
        test_config_path = os.path.join(td, test_config_name)
        shutil.copyfile(test_config, test_config_path)
        yield test_config_path


@pytest.fixture(scope="session")
def test_root_path():
    return TEST_DIR


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer, test_snowcli_config: str):
        super().__init__()
        self.app = app
        self.test_snowcli_config = test_snowcli_config

    @functools.wraps(CliRunner.invoke)
    def _invoke(self, *a, **kw):
        kw.update(catch_exceptions=False)
        return super().invoke(self.app, *a, **kw)

    def invoke_with_config(self, *args, **kwargs) -> CommandResult:
        result = self._invoke(
            ["--config-file", self.test_snowcli_config, *args[0]],
            **kwargs,
        )
        return CommandResult(result.exit_code, output=result.output)

    def invoke_integration(self, *args, **kwargs) -> CommandResult:
        result = self._invoke(
            [
                "--config-file",
                self.test_snowcli_config,
                *args[0],
                "--format",
                "JSON",
                "-c",
                "integration",
            ],
            **kwargs,
        )
        if result.output == "" or result.output.strip() == "Done":
            return CommandResult(result.exit_code, json=[])
        return CommandResult(result.exit_code, json.loads(result.output))

    def invoke_integration_without_format(self, *args, **kwargs) -> CommandResult:
        result = self._invoke(
            [
                "--config-file",
                self.test_snowcli_config,
                *args[0],
                "-c",
                "integration",
            ],
            **kwargs,
        )
        return CommandResult(result.exit_code, output=result.output)


@pytest.fixture
def runner(test_snowcli_config):
    return SnowCLIRunner(app, test_snowcli_config)
