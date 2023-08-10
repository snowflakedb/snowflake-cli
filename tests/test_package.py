import io
import logging
import os
from zipfile import ZipFile

import pytest
import tempfile
from requirements.requirement import Requirement
from unittest.mock import ANY, MagicMock, patch

from snowcli.cli.snowpark import package
from snowcli.utils import SplitRequirements
from tests.test_data import test_data
from tests.testing_utils.files_and_dirs import create_named_file


class TestPackage:
    @pytest.mark.parametrize(
        "argument",
        [
            (
                "snowflake-connector-python",
                "Package snowflake-connector-python is available on the Snowflake Anaconda channel.",
                "snowcli.cli.snowpark.package",
            ),
            (
                "some-weird-package-we-dont-know",
                "not found in Snowflake anaconda channel...",
                "snowcli.utils",
            ),
        ],
    )
    @patch("tests.test_package.package.manager.utils.requests")
    def test_package_lookup(
        self, mock_requests, caplog, argument, monkeypatch, runner
    ) -> None:
        mock_requests.get.return_value = self.mocked_anaconda_response(
            test_data.anaconda_response
        )

        monkeypatch.setattr("sys.stdin", io.StringIO("N"))

        with caplog.at_level(logging.DEBUG, logger=argument[2]):
            result = runner.invoke(
                ["snowpark", "package", "lookup", argument[0], "--yes"]
            )

        assert result.exit_code == 0
        assert caplog.text
        assert argument[1] in caplog.text

    @patch("tests.test_package.package.manager.utils.install_packages")
    @patch("tests.test_package.package.manager.utils.parse_anaconda_packages")
    def test_package_lookup_with_install_packages(
        self, mock_package, mock_install, caplog, runner
    ) -> None:
        mock_package.return_value = SplitRequirements(
            [], [Requirement("some-other-package")]
        )
        mock_install.return_value = (
            True,
            SplitRequirements(
                [Requirement("snowflake-snowpark-python")],
                [Requirement("some-other-package")],
            ),
        )

        with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark.package"):
            result = runner.invoke(
                ["snowpark", "package", "lookup", "some-other-package", "--yes"]
            )

        assert result.exit_code == 0
        assert (
            'include the following in your packages: [<Requirement: "snowflake-snowpark-python">]'
            in caplog.text
        )

    @patch("tests.test_package.package.manager.utils.requests")
    def test_package_create(
        self, mock_requests, caplog, temp_dir, dot_packages_directory, runner
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = test_data.anaconda_response
        mock_requests.get.return_value = mock_response

        with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark.package"):
            result = runner.invoke(
                ["snowpark", "package", "create", "totally-awesome-package", "--yes"]
            )
        zip_file = ZipFile("totally-awesome-package.zip", "r")

        assert result.exit_code == 0
        assert os.path.isfile("totally-awesome-package.zip")
        assert (
            ".packages/totally-awesome-package/totally-awesome-module.py"
            in zip_file.namelist()
        )
        os.remove("totally-awesome-package.zip")

    @patch("snowcli.cli.sql.snow_cli_global_context_manager.get_connection")
    def test_package_upload(self, mock_conn, package_file: str, runner) -> None:
        result = runner.invoke(
            ["snowpark", "package", "upload", "-f", package_file, "-s", "stageName"]
        )

        assert result.exit_code == 0
        mock_conn.return_value.upload_file_to_stage.assert_called_with(
            file_path=ANY,
            destination_stage="stageName",
            path="/",
            database=ANY,
            schema=ANY,
            overwrite=False,
            role=ANY,
            warehouse=ANY,
        )

    @pytest.fixture
    def temp_dir(self):
        initial_dir = os.getcwd()
        tmp = tempfile.TemporaryDirectory()
        os.chdir(tmp.name)
        yield tmp
        os.chdir(initial_dir)
        tmp.cleanup()

    @pytest.fixture
    def package_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            yield create_named_file("app.zip", tmp, [])

    @pytest.fixture
    def dot_packages_directory(self, temp_dir):
        os.mkdir(".packages")
        os.chdir(".packages")
        os.mkdir("totally-awesome-package")
        os.chdir("totally-awesome-package")
        create_named_file("totally-awesome-module.py", os.getcwd(), [])
        os.chdir(temp_dir.name)

    @staticmethod
    def mocked_anaconda_response(response: dict):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response

        return mock_response
