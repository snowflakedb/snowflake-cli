import os
import pytest
from pathlib import Path
from shutil import rmtree

from snowcli.cli.snowpark import procedure


class TestProcedure:
    TEMP_DIRECTORY_NAME = "test_procedure_tmp"
    DIR_INITIAL_CONTENTS = [
        "requirements.txt",
        "__pycache__",
        "local_connection.py",
        ".gitignore",
        "app.py",
        "config.toml",
    ]

    def test_procedure_init(self):
        current_path = Path(os.getcwd())
        temp_directory = os.path.join(current_path, self.TEMP_DIRECTORY_NAME)
        os.mkdir(temp_directory)
        os.chdir(temp_directory)
        procedure.procedure_init()
        assert os.listdir() == self.DIR_INITIAL_CONTENTS
        os.chdir("..")

    # @pytest.fixture(scope="module")
    # def temp_directory(self):
    #     current_path = Path(os.getcwd())
    #     path = os.path.join(current_path, self.TEMP_DIRECTORY_NAME)
    #     os.mkdir(path)
    #     yield path
    #     rmtree(path)
