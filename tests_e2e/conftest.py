import os
import subprocess
import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import pytest
from snowcli import __about__

TEST_DIR = Path(__file__).parent


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


@pytest.fixture
def temp_dir():
    initial_dir = os.getcwd()
    tmp = tempfile.TemporaryDirectory()
    os.chdir(tmp.name)
    yield tmp.name
    os.chdir(initial_dir)
    tmp.cleanup()


@pytest.fixture(scope="session")
def snowcli(test_root_path):
    with TemporaryDirectory() as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        _create_venv(tmp_dir_path)
        _build_snowcli(tmp_dir_path, test_root_path)
        _install_snowcli_with_external_plugin(tmp_dir_path, test_root_path)
        yield tmp_dir_path / "bin" / "snow"


def _create_venv(tmp_dir: Path) -> None:
    subprocess.check_call(["python", "-m", "venv", tmp_dir])


def _build_snowcli(venv_path: Path, test_root_path: Path) -> None:
    subprocess.check_call(
        [_python_path(venv_path), "-m", "pip", "install", "--upgrade", "build"]
    )
    subprocess.check_call(
        [_python_path(venv_path), "-m", "build", test_root_path / ".."]
    )


def _install_snowcli_with_external_plugin(
    venv_path: Path, test_root_path: Path
) -> None:
    whl_file = _find_whl_file(os.listdir(test_root_path / f"../dist/"))
    subprocess.check_call(
        [
            _python_path(venv_path),
            "-m",
            "pip",
            "install",
            test_root_path / f"../dist/{whl_file}",
        ]
    )
    subprocess.check_call(
        [
            _python_path(venv_path),
            "-m",
            "pip",
            "install",
            test_root_path
            / ".."
            / "test_external_plugins"
            / "multilingual_hello_command_group",
        ]
    )


def _python_path(venv_path: Path) -> Path:
    return venv_path / "bin" / "python"


def _find_whl_file(files: List[str]) -> str:
    for file in files:
        if file.endswith(".whl"):
            return file
    raise RuntimeError(f"Not found whl file in: {files}")
