import logging
import os
from unittest import mock
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest
from requirements.requirement import Requirement
from snowflake.cli.plugins.snowpark.models import SplitRequirements
from snowflake.cli.plugins.snowpark.package.utils import NotInAnaconda

from tests.test_data import test_data


class TestPackage:
    @pytest.mark.parametrize(
        "argument",
        ["snowflake-connector-python", "some-weird-package-we-dont-know"],
    )
    @patch("snowflake.cli.plugins.snowpark.package_utils.requests")
    def test_package_lookup(
        self, mock_requests, argument, monkeypatch, runner, snapshot
    ) -> None:
        mock_requests.get.return_value = self.mocked_anaconda_response(
            test_data.anaconda_response
        )

        result = runner.invoke(["snowpark", "package", "lookup", argument, "--yes"])

        assert result.exit_code == 0
        assert result.output == snapshot

    @patch("snowflake.cli.plugins.snowpark.package_utils.install_packages")
    @patch("snowflake.cli.plugins.snowpark.package_utils.parse_anaconda_packages")
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

    @patch("snowflake.cli.plugins.snowpark.package.commands.lookup")
    def test_package_create(
        self, mock_lookup, caplog, temp_dir, dot_packages_directory, runner
    ) -> None:

        mock_lookup.return_value = NotInAnaconda(
            SplitRequirements([], ["some-other-package"]), "totally-awesome-package"
        )

        with caplog.at_level(
            logging.DEBUG, logger="snowflake.cli.plugins.snowpark.package"
        ):
            result = runner.invoke(
                ["snowpark", "package", "create", "totally-awesome-package", "--yes"]
            )

        assert result.exit_code == 0
        assert os.path.isfile("totally-awesome-package.zip")

        zip_file = ZipFile("totally-awesome-package.zip", "r")

        assert (
            "totally-awesome-package/totally-awesome-module.py" in zip_file.namelist()
        )
        os.remove("totally-awesome-package.zip")

    @mock.patch("snowflake.cli.plugins.snowpark.package.manager.StageManager")
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
