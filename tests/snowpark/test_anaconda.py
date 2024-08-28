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


import pytest
from snowflake.cli._plugins.snowpark.models import Requirement
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AnacondaPackagesManager,
    AvailablePackage,
)

from tests.snowpark.mocks import mock_available_packages_sql_result  # noqa: F401

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
