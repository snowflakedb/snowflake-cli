import io
import logging
import os
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
                "Package snowflake-connector-python is available on the Snowflake anaconda channel.",
                "snowcli.cli.snowpark.package",
            ),
            (
                "some-weird-package-we-dont-know",
                "not found in Snowflake anaconda channel...",
                "snowcli.utils",
            ),
        ],
    )
    @patch("tests.test_package.package.utils.requests")
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

    @patch("tests.test_package.package.utils.install_packages")
    @patch("tests.test_package.package.utils.parse_anaconda_packages")
    def test_package_lookup_with_install_packages(
        self, mock_package, mock_install, caplog, runner
    ) -> None:
        mock_package = MagicMock(return_value=SplitRequirements([], []))
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
            "The package some-other-package is supported, but does depend on the following Snowflake supported native "
            'libraries. You should include the following in your packages: [<Requirement: "snowflake-snowpark-python">]'
            in caplog.messages
        )

    @patch("tests.test_package.package.utils.requests")
    def test_package_create(
        self, mock_requests, caplog, packages_directory, runner
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = test_data.anaconda_response
        mock_requests.get.return_value = mock_response

        with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark.package"):
            result = runner.invoke(
                ["snowpark", "package", "create", "totally-awesome-package"]
            )

        assert result.exit_code == 0
        assert (
            f"Package totally-awesome-package.zip created. You can now upload it to a stage (`snow package upload -f totally-awesome-package.zip -s packages`) and reference it in your procedure or function."
            in caplog.text
        )
        assert os.path.isfile("totally-awesome-package.zip")
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
    def packages_directory(self):
        path = os.path.join(os.getcwd(), ".packages")
        os.mkdir(path)
        yield path

    @pytest.fixture
    def package_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            yield create_named_file("app.zip", tmp, [])

    @staticmethod
    def mocked_anaconda_response(response: dict):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response

        return mock_response
