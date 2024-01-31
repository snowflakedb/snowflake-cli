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
import strictyaml
from snowflake.cli.api.cli_global_context import cli_context_manager
from snowflake.cli.api.project.definition import merge_left
from snowflake.cli.app.cli_app import app
from strictyaml import as_document
from typer import Typer
from typer.testing import CliRunner

pytest_plugins = [
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
            return CommandResult(result.exit_code, json.loads(result.output))
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
    return SnowCLIRunner(app, test_snowcli_config_provider)


@pytest.fixture
def alter_snowflake_yml():
    def _update(snowflake_yml_path: Path, parameter_path: str, value):
        import yaml

        with open(snowflake_yml_path) as fh:
            yml = yaml.safe_load(fh)

        parts = parameter_path.split(".")
        current_object = yml
        while parts:
            part = parts.pop(0)
            evaluated_part = int(part) if part.isdigit() else part

            if parts:
                current_object = current_object[evaluated_part]
            else:
                current_object[evaluated_part] = value

        with open(snowflake_yml_path, "w+") as fh:
            yaml.safe_dump(yml, fh)

    return _update


class QueryResultJsonEncoderError(RuntimeError):
    def __init__(self, output: str):
        super().__init__(f"Can not parse query result:\n{output}")


@pytest.fixture
def project_directory(temporary_working_directory, test_root_path):
    @contextmanager
    def _temporary_project_directory(
        project_name, merge_project_definition: Optional[dict] = None
    ):
        test_data_file = test_root_path / "test_data" / "projects" / project_name
        shutil.copytree(test_data_file, temporary_working_directory, dirs_exist_ok=True)
        if merge_project_definition:
            project_definition = strictyaml.load(Path("snowflake.yml").read_text()).data
            merge_left(project_definition, merge_project_definition)
            with open(Path(temporary_working_directory) / "snowflake.yml", "w") as file:
                file.write(as_document(project_definition).as_yaml())

        yield temporary_working_directory

    return _temporary_project_directory


@pytest.fixture(autouse=True)
def reset_global_context_after_each_test(request):
    cli_context_manager.reset()
    yield
