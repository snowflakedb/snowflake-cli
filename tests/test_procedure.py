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

    def test_procedure_init(self, execute_in_tmp_dir, runner):
        runner.invoke(["snowpark", "procedure", "init"])
        assert self.DIR_INITIAL_CONTENTS.issubset(os.listdir(os.getcwd()))

    @mock.patch("snowcli.utils.parse_anaconda_packages")
    def test_procedure_package(self, mock_anaconda, execute_in_tmp_dir, runner):
        mock_anaconda = MagicMock(return_value=SplitRequirements([], []))

        runner.invoke(["snowpark", "procedure", "init"])
        runner.invoke(["snowpark", "procedure", "package"])

        zip_file = ZipFile("app.zip")

        assert os.path.isfile("app.zip")
        assert "requirements.txt" in zip_file.namelist()
        assert "requirements.snowflake.txt" in zip_file.namelist()
        assert "local_connection.py" in zip_file.namelist()
        assert "app.py" in zip_file.namelist()
        assert "config.toml" in zip_file.namelist()
