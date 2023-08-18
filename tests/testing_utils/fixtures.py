import os
from pathlib import Path
from typing import Generator

import pytest
import tempfile

from tests.test_data import test_data
from tests.testing_utils.files_and_dirs import create_named_file, create_temp_file


REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"


@pytest.fixture
def app_zip(temp_dir) -> Generator:
    yield create_temp_file(".zip", temp_dir, [])


@pytest.fixture
def correct_metadata_file(temp_dir) -> Generator:
    yield create_temp_file(".yaml", temp_dir, test_data.correct_package_metadata)


@pytest.fixture
def correct_requirements_txt(temp_dir) -> Generator:
    req_txt = create_named_file(
        REQUIREMENTS_SNOWFLAKE, temp_dir, test_data.requirements
    )
    yield req_txt
    os.remove(req_txt)


@pytest.fixture
def dot_packages_directory(temp_dir):
    dir_path = ".packages/totally-awesome-package"
    os.makedirs(dir_path)
    create_named_file("totally-awesome-module.py", dir_path, [])


@pytest.fixture
def include_paths_env_variable(other_directory: str) -> Generator:
    os.environ["SNOWCLI_INCLUDE_PATHS"] = other_directory
    yield os.environ["SNOWCLI_INCLUDE_PATHS"]
    os.environ.pop("SNOWCLI_INCLUDE_PATHS")


@pytest.fixture
def other_directory() -> Generator:
    tmp_dir = tempfile.TemporaryDirectory()
    yield tmp_dir.name
    tmp_dir.cleanup()


@pytest.fixture
def other_directory_with_chdir(other_directory: str) -> Generator:
    initial_dir = os.getcwd()
    os.chdir(other_directory)
    yield other_directory
    os.chdir(initial_dir)


@pytest.fixture
def package_file():
    with tempfile.TemporaryDirectory() as tmp:
        yield create_named_file("app.zip", tmp, [])


@pytest.fixture
def temp_dir():
    initial_dir = os.getcwd()
    tmp = tempfile.TemporaryDirectory()
    os.chdir(tmp.name)
    yield tmp.name
    os.chdir(initial_dir)
    tmp.cleanup()


@pytest.fixture
def temp_directory_for_app_zip(temp_dir) -> Generator:
    temp_dir = tempfile.TemporaryDirectory(dir=temp_dir)
    yield temp_dir.name


@pytest.fixture
def temp_file_in_other_directory(other_directory: str) -> Generator:
    yield create_temp_file(".txt", other_directory, [])


@pytest.fixture
def txt_file_in_a_subdir(temp_dir: str) -> Generator:
    subdir = tempfile.TemporaryDirectory(dir=temp_dir)
    yield create_temp_file(".txt", subdir.name, [])
