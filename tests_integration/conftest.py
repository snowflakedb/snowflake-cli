# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import functools
import json
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import yaml

from snowflake.cli.api.cli_global_context import cli_context_manager
from snowflake.cli.app.cli_app import app_factory
from typer import Typer
from typer.testing import CliRunner

from tests.conftest import clean_logging_handlers_fixture  # noqa: F401
from tests.testing_utils.fixtures import (
    alter_snowflake_yml,  # noqa: F401
    snowflake_home,
)
from tests.testing_utils.files_and_dirs import merge_left

pytest_plugins = [
    "tests_common",
    "tests_integration.testing_utils",
    "tests_integration.snowflake_connector",
]

TEST_DIR = Path(__file__).parent
DEFAULT_TEST_CONFIG = "connection_configs.toml"


@dataclass
class CommandResult:
    exit_code: int
    json: Optional[List[Dict[str, Any]] | Dict[str, Any]] = None
    output: Optional[str] = None
    stderr: Optional[str] = None


class TestConfigProvider:
    def __init__(self, temp_dir_with_configs: Path):
        self._temp_dir_with_configs = temp_dir_with_configs

    def get_config_path(self, file_name: str) -> Path:
        return self._temp_dir_with_configs / file_name


@pytest.fixture(scope="session")
def test_snowcli_config_provider():
    with tempfile.TemporaryDirectory() as td:
        temp_dst = Path(td) / "config"
        shutil.copytree(TEST_DIR / "config", temp_dst)
        yield TestConfigProvider(temp_dst)


@pytest.fixture(scope="session")
def test_root_path():
    return TEST_DIR


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer, test_config_provider: TestConfigProvider):
        super().__init__()
        self.app = app
        self._test_config_provider = test_config_provider
        self._test_config_path = self._test_config_provider.get_config_path(
            DEFAULT_TEST_CONFIG
        )

    def use_config(self, config_file_name: str) -> None:
        self._test_config_path = self._test_config_provider.get_config_path(
            config_file_name
        )

    @functools.wraps(CliRunner.invoke)
    def invoke(self, *a, **kw):
        if "catch_exceptions" not in kw:
            kw.update(catch_exceptions=False)
        return super().invoke(self.app, *a, **kw)

    def invoke_with_config(self, args, **kwargs) -> CommandResult:
        result = self.invoke(
            [
                "--config-file",
                self._test_config_path,
                *args,
            ],
            **kwargs,
        )

        if result.output == "" or result.output.strip() == "Done":
            return CommandResult(result.exit_code, json=[])
        try:
            return CommandResult(
                result.exit_code, json.loads(result.output), output=result.output
            )
        except JSONDecodeError:
            return CommandResult(result.exit_code, output=result.output)

    def invoke_json(self, args, **kwargs) -> CommandResult:
        return self.invoke_with_config([*args, "--format", "JSON"], **kwargs)

    def invoke_with_connection_json(
        self, args, connection: str = "integration", **kwargs
    ) -> CommandResult:
        return self.invoke_json([*args, "-c", connection], **kwargs)

    def invoke_with_connection(
        self, args, connection: str = "integration", **kwargs
    ) -> CommandResult:
        return self.invoke_with_config([*args, "-c", connection], **kwargs)


@pytest.fixture
def runner(test_snowcli_config_provider):
    app = app_factory()
    yield SnowCLIRunner(app, test_snowcli_config_provider)


class QueryResultJsonEncoderError(RuntimeError):
    def __init__(self, output: str):
        super().__init__(f"Can not parse query result:\n{output}")


@pytest.fixture
def project_directory(temporary_working_directory, test_root_path):
    @contextmanager
    def _temporary_project_directory(
        project_name,
        merge_project_definition: Optional[dict] = None,
        subpath: Optional[Path] = None,
    ):
        test_data_file = test_root_path / "test_data" / "projects" / project_name
        project_dir = temporary_working_directory
        if subpath:
            project_dir = temporary_working_directory / subpath
            project_dir.mkdir(parents=True)
        shutil.copytree(test_data_file, project_dir, dirs_exist_ok=True)
        if merge_project_definition:
            with Path("snowflake.yml").open("r") as fh:
                project_definition = yaml.safe_load(fh)
            merge_left(project_definition, merge_project_definition)
            with open(Path(project_dir) / "snowflake.yml", "w") as file:
                yaml.dump(project_definition, file)

        yield project_dir

    return _temporary_project_directory


@pytest.fixture(autouse=True)
def reset_global_context_after_each_test(request):
    cli_context_manager.reset()
    yield


# This automatically used fixture isolates default location
# of config files from user's system.
@pytest.fixture(autouse=True)
def isolate_snowflake_home(snowflake_home):
    yield snowflake_home
