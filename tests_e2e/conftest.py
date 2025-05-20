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

import shutil
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

import pytest
from snowflake.cli import __about__
from snowflake.cli.api.constants import PYTHON_3_12
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS

TEST_DIR = Path(__file__).parent

pytest_plugins = [
    "tests_common",
]


def _clean_output(text: str):
    """
    Replacing util to clean up console output. Typer is using rich.Panel to show the --help content.
    Unfortunately Typer implementation hardcodes box style for Panel.
    The typer.rich_utils.STYLE_OPTIONS_TABLE_BOX works only for content within the Panel.
    """
    if text is None:
        return None
    text = "\n".join(line.rstrip() for line in text.splitlines())
    return (
        text.replace("│", "|")
        .replace("─", "-")
        .replace("╭", "+")
        .replace("╰", "+")
        .replace("╯", "+")
        .replace("╮", "+")
        .replace(__about__.VERSION, "0.0.1-test_patched")
    )


def subprocess_check_output(cmd, stdin: Optional[str] = None):
    try:
        output = subprocess.check_output(
            cmd, input=stdin, shell=IS_WINDOWS, stderr=sys.stdout, encoding="utf-8"
        )
        return _clean_output(output)
    except subprocess.CalledProcessError as err:
        print(err.output)
        raise


def subprocess_run(cmd, stdin: Optional[str] = None):
    p = subprocess.run(
        cmd,
        input=stdin,
        shell=IS_WINDOWS,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    p.stdout = _clean_output(p.stdout)
    p.stderr = _clean_output(p.stderr)
    return p


@pytest.fixture(scope="session")
def test_root_path():
    return TEST_DIR


@pytest.fixture(autouse=True)
def disable_colors_and_styles_in_output(monkeypatch):
    """
    Colors and styles in output cause mismatches in asserts,
    this environment variable turn off styling
    """
    monkeypatch.setenv("TERM", "unknown")


@pytest.fixture(scope="session")
def snowcli(test_root_path):
    with TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        _create_venv(tmp_dir_path)
        _build_snowcli(tmp_dir_path, test_root_path)
        _install_snowcli_with_external_plugin(tmp_dir_path, test_root_path)
        if IS_WINDOWS:
            yield tmp_dir_path / "Scripts" / "snow.exe"
        else:
            yield tmp_dir_path / "bin" / "snow"


@pytest.fixture(autouse=True)
def isolate_default_config_location(monkeypatch, temporary_directory):
    monkeypatch.setenv("SNOWFLAKE_HOME", temporary_directory)


def _create_venv(tmp_dir: Path) -> None:
    subprocess_check_output(["python", "-m", "venv", tmp_dir])


def _build_snowcli(venv_path: Path, test_root_path: Path) -> None:
    subprocess_check_output(
        [_python_path(venv_path), "-m", "pip", "install", "--upgrade", "build"],
    )
    subprocess_check_output(
        [_python_path(venv_path), "-m", "build", test_root_path / ".."]
    )


def _pip_install(python, *args):
    return subprocess_check_output([python, "-m", "pip", "install", *args])


def _install_snowcli_with_external_plugin(
    venv_path: Path, test_root_path: Path
) -> None:
    version = __about__.VERSION
    python = _python_path(venv_path)
    _pip_install(
        python,
        test_root_path / f"../dist/snowflake_cli-{version}-py3-none-any.whl",
    )
    _pip_install(
        python,
        test_root_path.parent
        / "test_external_plugins"
        / "multilingual_hello_command_group",
    )

    if sys.version_info < PYTHON_3_12:
        _pip_install(python, "snowflake-snowpark-python[pandas]==1.25.0")


def _python_path(venv_path: Path) -> Path:
    if IS_WINDOWS:
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


# Inspired by project_directory fixture in tests_integration/conftest.py
# This is a simpler implementation of that fixture, i.e. does not include supporting local PDFs.
@pytest.fixture
def project_directory(temporary_directory, test_root_path):
    @contextmanager
    def _temporary_project_directory(project_name):
        test_data_file = test_root_path / "test_data" / project_name
        shutil.copytree(test_data_file, temporary_directory, dirs_exist_ok=True)
        yield Path(temporary_directory)

    return _temporary_project_directory


@pytest.fixture()
def prepare_test_config_file(temporary_directory):
    def f(config_file_path: SecurePath):
        target_file_path = Path(temporary_directory) / "config.toml"
        config_file_path.copy(target_file_path)
        return target_file_path

    return f


@pytest.fixture()
def config_file(test_root_path, prepare_test_config_file):
    yield prepare_test_config_file(
        SecurePath(test_root_path) / "config" / "config.toml"
    )


@pytest.fixture()
def empty_config_file(test_root_path, prepare_test_config_file):
    yield prepare_test_config_file(SecurePath(test_root_path) / "config" / "empty.toml")


@pytest.fixture()
def example_connection_config_file(test_root_path, prepare_test_config_file):
    yield prepare_test_config_file(
        SecurePath(test_root_path) / "config" / "example_connection.toml"
    )
