import os
import tempfile
from unittest import mock
from unittest.mock import MagicMock
from zipfile import ZipFile

import pytest

import snowcli.utils
from snowcli.cli.snowpark import procedure
from snowcli.utils import SplitRequirements


class TestProcedure:
    TEMP_DIRECTORY_NAME = "test_procedure_tmp"
    DIR_INITIAL_CONTENTS = {
        "requirements.txt",
        "local_connection.py",
        ".gitignore",
        "app.py",
        "config.toml",
    }

    def test_procedure_init(self, tmp_dir_for_procedure_tests):
        procedure.procedure_init()
        assert set(os.listdir()) == self.DIR_INITIAL_CONTENTS

    @mock.patch("snowcli.utils.parse_anaconda_packages")
    def test_procedure_package(self,  tmp_dir_for_procedure_tests):
        mock_parse = MagicMock(return_value=SplitRequirements([], []))
        procedure.procedure_init()
        procedure.procedure_package()

        zip_file = ZipFile("app.zip")

        assert os.path.isfile("app.zip")
        assert "requirements.txt" in zip_file.namelist()
        assert "requirements.snowflake.txt" in zip_file.namelist()
        assert"local_connection.py" in zip_file.namelist()
        assert"app.py" in zip_file.namelist()
        assert"config.toml" in zip_file.namelist()




    @pytest.fixture(scope="class")
    def tmp_dir_for_procedure_tests(self):
        initial_dir = os.getcwd()
        tmp_dir = tempfile.TemporaryDirectory()
        os.chdir(tmp_dir.name)
        yield tmp_dir
        tmp_dir.cleanup()
        os.chdir(initial_dir)
