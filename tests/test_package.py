import logging
import pytest

from pathlib import Path
from requirements.requirement import Requirement
from unittest.mock import ANY, MagicMock, patch
from zipfile import ZipFile

from snowcli.cli.snowpark import package
from snowcli.cli.snowpark.package.utils import NotInAnaconda
from snowcli.utils import SplitRequirements
from tests.testing_utils.fixtures import *


class TestPackage:
    @pytest.mark.parametrize(
        "argument",
        [
            (
                "snowflake-connector-python",
                "Package snowflake-connector-python is available on the Snowflake anaconda channel.",
                "snowcli.cli.snowpark.package.commands",
            ),
            (
                "some-weird-package-we-dont-know",
                "Lookup for package some-weird-package-we-dont-know resulted in some error. Please check the package name or try again with -y option",
                "snowcli.cli.snowpark.package.commands",
            ),
        ],
    )
    @patch("tests.test_package.package.manager.utils.requests")
    def test_package_lookup(
        self, mock_requests, argument, monkeypatch, runner, snapshot
    ) -> None:
        mock_requests.get.return_value = self.mocked_anaconda_response(
            test_data.anaconda_response
        )

        result = runner.invoke(["snowpark", "package", "lookup", argument[0], "--yes"])

        assert result.exit_code == 0
        assert result.output == snapshot

    @patch("tests.test_package.package.manager.utils.install_packages")
    @patch("tests.test_package.package.manager.utils.parse_anaconda_packages")
    def test_package_lookup_with_install_packages(
        self, mock_package, mock_install, runner, capfd, snapshot
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

        result = runner.invoke(
            ["snowpark", "package", "lookup", "some-other-package", "--yes"]
        )
        assert result.exit_code == 0
        assert result.output == snapshot

    @patch("tests.test_package.package.commands.lookup")
    def test_package_create(
        self, mock_lookup, caplog, temp_dir, dot_packages_directory, runner
    ) -> None:

        mock_lookup.return_value = NotInAnaconda(
            SplitRequirements([], ["some-other-package"]), "totally-awesome-package"
        )

        with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark.package"):
            result = runner.invoke(
                ["snowpark", "package", "create", "totally-awesome-package", "--yes"]
            )

        assert result.exit_code == 0
        assert os.path.isfile("totally-awesome-package.zip")

        zip_file = ZipFile("totally-awesome-package.zip", "r")

        assert (
            ".packages/totally-awesome-package/totally-awesome-module.py"
            in zip_file.namelist()
        )
        os.remove("totally-awesome-package.zip")

    @mock.patch("snowcli.cli.snowpark.package.manager.StageManager")
    @mock.patch("snowflake.connector.connect")
    def test_package_upload(
        self,
        mock_connector,
        mock_stage_manager,
        package_file: str,
        runner,
        mock_ctx,
        mock_cursor,
    ) -> None:
        ctx = mock_ctx()
        mock_connector.return_value = ctx
        mock_stage_manager().put.return_value = mock_cursor(
            rows=[("", "", "", "", "", "", "UPLOADED")], columns=[]
        )

        result = runner.invoke(
            ["snowpark", "package", "upload", "-f", package_file, "-s", "stageName"]
        )

        assert result.exit_code == 0
        assert ctx.get_query() == ""

    @staticmethod
    def mocked_anaconda_response(response: dict):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response

        return mock_response
