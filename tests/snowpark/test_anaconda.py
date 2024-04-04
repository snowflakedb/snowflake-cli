import json
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from snowflake.cli.plugins.snowpark.models import Requirement
from snowflake.cli.plugins.snowpark.package.anaconda import (
    AnacondaChannel,
    AnacondaPackageData,
)

from tests.test_data import test_data
from tests.testing_utils.fixtures import TEST_DIR

ANACONDA = AnacondaChannel(
    packages={
        "shrubbery": AnacondaPackageData(
            snowflake_name="shrubbery", versions={"1.2.1", "1.2.2"}
        ),
        "dummy_pkg": AnacondaPackageData(
            snowflake_name="dummy-pkg", versions={"0.1.1", "1.0.0", "1.1.0"}
        ),
        "jpeg": AnacondaPackageData(snowflake_name="jpeg", versions={"9e", "9d", "9b"}),
    }
)


def test_latest_version():
    assert ANACONDA.package_latest_version(Requirement.parse("shrubbery")) == "1.2.2"
    assert ANACONDA.package_latest_version(Requirement.parse("dummy_pkg")) == "1.1.0"
    assert ANACONDA.package_latest_version(Requirement.parse("dummy-pkg")) == "1.1.0"
    assert ANACONDA.package_latest_version(Requirement.parse("jpeg")) == "9e"
    assert ANACONDA.package_latest_version(Requirement.parse("weird-pkg")) is None


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
    assert ANACONDA.is_package_available(Requirement.parse(argument)) == expected


@mock.patch("requests.get")
def test_parse_anaconda_packages(mock_get):
    mock_response = mock.Mock()
    mock_response.status_code = 200
    # load the contents of the local json file under test_data/anaconda_channel_data.json
    with open(TEST_DIR / "test_data/anaconda_channel_data.json") as fh:
        mock_response.json.return_value = json.load(fh)

    mock_get.return_value = mock_response
    anaconda = AnacondaChannel.from_snowflake()

    packages = [
        Requirement.parse("pandas==1.4.4"),
        Requirement.parse("FuelSDK>=0.9.3"),
        Requirement.parse("Pamela==1.0.1"),
    ]
    split_requirements = anaconda.parse_anaconda_packages(packages=packages)
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


@patch("snowflake.cli.plugins.snowpark.package.anaconda.requests")
def test_anaconda_packages(mock_requests):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = test_data.anaconda_response
    mock_requests.get.return_value = mock_response

    anaconda = AnacondaChannel.from_snowflake()
    assert anaconda.is_package_available(Requirement.parse_line("streamlit"))

    anaconda_packages = anaconda.parse_anaconda_packages(test_data.packages)
    assert (
        Requirement.parse_line("snowflake-connector-python")
        in anaconda_packages.in_snowflake
    )
    assert (
        Requirement.parse_line("my-totally-awesome-package")
        in anaconda_packages.unavailable
    )
