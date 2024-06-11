# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from snowflake.cli.plugins.snowpark.models import Requirement
from snowflake.cli.plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AnacondaPackagesManager,
    AvailablePackage,
)
from snowflake.connector import Error as ConnectorError

from tests.snowpark.mocks import mock_available_packages_sql_result  # noqa: F401
from tests.test_data import test_data
from tests.testing_utils.fixtures import TEST_DIR

ANACONDA_PACKAGES = AnacondaPackages(
    packages={
        "shrubbery": AvailablePackage(
            snowflake_name="shrubbery", versions={"1.2.1", "1.2.2"}
        ),
        "dummy_pkg": AvailablePackage(
            snowflake_name="dummy-pkg", versions={"0.1.1", "1.0.0", "1.1.0"}
        ),
        "jpeg": AvailablePackage(snowflake_name="jpeg", versions={"9e", "9d", "9b"}),
    }
)


def test_latest_version():
    assert (
        ANACONDA_PACKAGES.package_latest_version(Requirement.parse("shrubbery"))
        == "1.2.2"
    )
    assert (
        ANACONDA_PACKAGES.package_latest_version(Requirement.parse("dummy_pkg"))
        == "1.1.0"
    )
    assert (
        ANACONDA_PACKAGES.package_latest_version(Requirement.parse("dummy-pkg"))
        == "1.1.0"
    )
    assert ANACONDA_PACKAGES.package_latest_version(Requirement.parse("jpeg")) is None
    assert (
        ANACONDA_PACKAGES.package_latest_version(Requirement.parse("weird-pkg")) is None
    )


def test_versions():
    assert ANACONDA_PACKAGES.package_versions(Requirement.parse("shrubbery")) == [
        "1.2.2",
        "1.2.1",
    ]
    assert ANACONDA_PACKAGES.package_versions(Requirement.parse("dummy_pkg")) == [
        "1.1.0",
        "1.0.0",
        "0.1.1",
    ]
    assert ANACONDA_PACKAGES.package_versions(Requirement.parse("dummy-pkg")) == [
        "1.1.0",
        "1.0.0",
        "0.1.1",
    ]
    assert ANACONDA_PACKAGES.package_versions(Requirement.parse("jpeg")) == [
        "9e",
        "9d",
        "9b",
    ]
    assert ANACONDA_PACKAGES.package_versions(Requirement.parse("weird-pkg")) == []


@pytest.mark.parametrize(
    "argument, expected",
    [
        ("shrubbery", True),
        ("DUMMY_pkg", True),
        ("dummy-PKG", True),
        ("jpeg", True),
        ("non-existing-pkg", False),
        ("shrubbery>=2", False),
        ("shrubbery>=1.2", True),
        ("shrubbery<=1.2", False),
        ("Shrubbery<=1.3", True),
        ("shrubbery==1.2.*", True),
        ("shrubbery!=1.2.*", False),
        ("dummy-pkg!=1.0.*", True),
        ("shrubbery>1,!=1.2.*", False),
        ("shrubbery>1,<4", True),
        # safe-fail for non-pep508 version formats
        ("jpeg==9", False),
    ],
)
def test_check_if_package_is_avaiable_in_conda(argument, expected):
    assert (
        ANACONDA_PACKAGES.is_package_available(Requirement.parse(argument)) == expected
    )


def test_anaconda_packages_from_sql_query(mock_available_packages_sql_result):
    anaconda_packages_manager = AnacondaPackagesManager()
    anaconda_packages = (
        anaconda_packages_manager.find_packages_available_in_snowflake_anaconda()
    )

    packages = [
        Requirement.parse("pandas==1.4.4"),
        Requirement.parse("FuelSDK>=0.9.3"),
        Requirement.parse("Pamela==1.0.1"),
    ]
    split_requirements = anaconda_packages.filter_available_packages(packages=packages)
    assert len(split_requirements.in_snowflake) == 1
    assert len(split_requirements.unavailable) == 2
    assert split_requirements.in_snowflake[0].name == "pandas"
    assert split_requirements.in_snowflake[0].specifier is True
    assert split_requirements.in_snowflake[0].specs == [("==", "1.4.4")]
    assert split_requirements.unavailable[0].name == "fuelsdk"
    assert split_requirements.unavailable[0].specifier is True
    assert split_requirements.unavailable[0].specs == [(">=", "0.9.3")]
    assert split_requirements.unavailable[1].name == "pamela"
    assert split_requirements.unavailable[1].specs == [("==", "1.0.1")]

    assert anaconda_packages.is_package_available(Requirement.parse_line("pamela"))
    assert anaconda_packages.is_package_available(
        Requirement.parse_line("pandas==1.4.4")
    )
    assert anaconda_packages.is_package_available(
        Requirement.parse_line("snowflake.core")
    )
    assert anaconda_packages.is_package_available(Requirement.parse_line("snowflake"))


@mock.patch("requests.get")
@mock.patch("snowflake.cli.app.snow_connector.connect_to_snowflake")
def test_filter_anaconda_packages_from_fallback_to_channel_data(mock_connect, mock_get):
    mock_connect.side_effect = ConnectorError("test error")

    mock_response = mock.Mock()
    mock_response.status_code = 200
    # load the contents of the local json file under test_data/anaconda_channel_data.json
    with open(TEST_DIR / "test_data/anaconda_channel_data.json") as fh:
        mock_response.json.return_value = json.load(fh)

    mock_get.return_value = mock_response
    anaconda_packages_manager = AnacondaPackagesManager()
    anaconda_packages = (
        anaconda_packages_manager.find_packages_available_in_snowflake_anaconda()
    )

    packages = [
        Requirement.parse("pandas==1.4.4"),
        Requirement.parse("FuelSDK>=0.9.3"),
        Requirement.parse("Pamela==1.0.1"),
    ]
    split_requirements = anaconda_packages.filter_available_packages(packages=packages)
    assert len(split_requirements.in_snowflake) == 1
    assert len(split_requirements.unavailable) == 2
    assert split_requirements.in_snowflake[0].name == "pandas"
    assert split_requirements.in_snowflake[0].specifier is True
    assert split_requirements.in_snowflake[0].specs == [("==", "1.4.4")]
    assert split_requirements.unavailable[0].name == "fuelsdk"
    assert split_requirements.unavailable[0].specifier is True
    assert split_requirements.unavailable[0].specs == [(">=", "0.9.3")]
    assert split_requirements.unavailable[1].name == "pamela"
    assert split_requirements.unavailable[1].specs == [("==", "1.0.1")]

    assert anaconda_packages.is_package_available(Requirement.parse_line("pamela"))
    assert anaconda_packages.is_package_available(
        Requirement.parse_line("pandas==1.4.4")
    )


@patch("snowflake.cli.plugins.snowpark.package.anaconda_packages.requests")
@mock.patch("snowflake.cli.app.snow_connector.connect_to_snowflake")
def test_anaconda_packages_from_fallback_to_channel_data(mock_connect, mock_requests):
    mock_connect.side_effect = ConnectorError("test error")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = test_data.anaconda_response
    mock_requests.get.return_value = mock_response

    anaconda_packages_manager = AnacondaPackagesManager()
    anaconda_packages = (
        anaconda_packages_manager.find_packages_available_in_snowflake_anaconda()
    )

    assert anaconda_packages.is_package_available(Requirement.parse_line("streamlit"))

    anaconda_packages = anaconda_packages.filter_available_packages(test_data.packages)
    assert (
        Requirement.parse_line("snowflake-connector-python")
        in anaconda_packages.in_snowflake
    )
    assert (
        Requirement.parse_line("my-totally-awesome-package")
        in anaconda_packages.unavailable
    )
