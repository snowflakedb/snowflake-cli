import io
import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from requirements.requirement import Requirement

from snowcli.cli.snowpark import package
from snowcli.utils import SplitRequirements
from tests.test_data import test_data


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
    def test_package_lookup(self, mock_requests, caplog, argument, monkeypatch):
        mock_requests.get.return_value = self.mocked_anaconda_response(
            test_data.anaconda_response
        )

        monkeypatch.setattr("sys.stdin", io.StringIO("N"))

        with caplog.at_level(logging.DEBUG, logger=argument[2]):
            result = package.package_lookup(argument[0], install_packages=True)

        assert caplog.text
        assert argument[1] in caplog.text

    @patch("tests.test_package.package.utils.install_packages")
    @patch("tests.test_package.package.utils.parse_anaconda_packages")
    def test_package_lookup_with_install_packages(
        self, mock_package, mock_install, caplog
    ):
        mock_package = MagicMock(return_value=SplitRequirements([], []))
        mock_install.return_value = (
            True,
            SplitRequirements(
                [Requirement("snowflake-snowpark-python")],
                [Requirement("some-other-package")],
            ),
        )

        with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark.package"):
            result = package.package_lookup("some-other-package", install_packages=True)

        assert (
            "The package some-other-package is supported, but does depend on the following Snowflake supported native "
            'libraries. You should include the following in your packages: [<Requirement: "snowflake-snowpark-python">]'
            in caplog.messages
        )

    @patch("tests.test_package.package.utils.requests")
    def test_package_create(self, mock_requests, caplog, packages_directory):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = test_data.anaconda_response
        mock_requests.get.return_value = mock_response

        with caplog.at_level(logging.DEBUG, logger="snowcli.cli.snowpark.package"):
            result = package.package_create("totally-awesome-package")

        assert (
            f"Package totally-awesome-package.zip created. You can now upload it to a stage (`snow package upload -f totally-awesome-package.zip -s packages`) and reference it in your procedure or function."
            in caplog.text
        )
        assert os.path.isfile("totally-awesome-package.zip")
        os.remove("totally-awesome-package.zip")

    @pytest.fixture
    def packages_directory(self):
        path = os.path.join(os.getcwd(), ".packages")
        os.mkdir(path)
        yield path

    @staticmethod
    def mocked_anaconda_response(response: dict):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response

        return mock_response
