import os
import tempfile

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

    def test_procedure_init(self, tmp_dir_for_procedure_tests):
        os.chdir(tmp_dir_for_procedure_tests)
        procedure.procedure_init()
        assert os.listdir() == self.DIR_INITIAL_CONTENTS

    def test_procedure_create(self):
        result =

    @pytest.fixture(scope="class")
    def tmp_dir_for_procedure_tests(self):
        temp_dir = tempfile.TemporaryDirectory()
        yield temp_dir.name
        temp_dir.cleanup()
