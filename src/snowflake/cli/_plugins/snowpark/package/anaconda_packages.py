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

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Set

from packaging.requirements import InvalidRequirement
from packaging.requirements import Requirement as PkgRequirement
from packaging.version import InvalidVersion, parse
from snowflake.cli._plugins.snowpark.models import Requirement
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import DictCursor

log = logging.getLogger(__name__)


@dataclass
class FilterRequirementsResult:
    """A dataclass to hold the results of parsing requirements files and dividing them into
    snowflake-supported vs other packages.
    """

    in_snowflake: List[Requirement]
    unavailable: List[Requirement]


@dataclass
class AvailablePackage:
    snowflake_name: str
    versions: Set[str]

    def iter_versions(self):
        for version in self.versions:
            yield parse(version)

    def is_required_version_available(self, requirement: Requirement) -> bool:
        try:
            package_specifiers = PkgRequirement(requirement.line).specifier
            return any(
                version in package_specifiers for version in self.iter_versions()
            )
        except (InvalidVersion, InvalidRequirement):
            # fail-safe for non-pep508 formats
            return False


class AnacondaPackages:
    def __init__(self, packages: Dict[str, AvailablePackage]):
        """
        [packages] should be a dictionary mapping package name to AnacondaPackageData object.
        All package names should be provided in wheel escape format:
        https://peps.python.org/pep-0491/#escaping-and-unicode
        """
        self._packages = packages

    @classmethod
    def empty(cls):
        return cls({})

    def is_package_available(
        self, package: Requirement, skip_version_check: bool = False
    ) -> bool:
        """
        Checks of a requirement is available in the Snowflake Anaconda Channel.

        As Snowflake currently doesn't support extra syntax (ex. `jinja2[diagrams]`), if such
        extra is present in the dependency, we mark it as unavailable.
        """
        if not package.name or package.extras:
            return False
        if package.name not in self._packages:
            return False
        if skip_version_check or not package.specs:
            return True
        if any(spec[0] == "!=" for spec in package.specs):
            # Snowflake doesn't support '!=' so we need to resolve this requirement externally
            return False
        return self._packages[package.name].is_required_version_available(package)

    def package_latest_version(self, package: Requirement) -> str | None:
        """Returns the latest version of the package or None if the latest version can't be determined."""
        if package.name not in self._packages:
            return None
        try:
            return str(max(self._packages[package.name].iter_versions()))
        except InvalidVersion:
            # fail-safe for non-pep8 versions
            return None

    def package_versions(self, package: Requirement) -> List[str]:
        """Returns list of available versions of the package."""
        if package.name not in self._packages:
            return []
        package_data = self._packages[package.name]
        try:
            return list(
                str(x) for x in sorted(package_data.iter_versions(), reverse=True)
            )
        except InvalidVersion:
            return list(sorted(package_data.versions, reverse=True))

    def filter_available_packages(
        self, packages: List[Requirement], skip_version_check: bool = False
    ) -> FilterRequirementsResult:
        """
        Checks if a list of packages are available in the Snowflake Anaconda channel.
        Returns an object with two attributes: 'snowflake' and 'other'.
        Each key contains a list of Requirement object.

        Parameters:
            packages (List[Requirement]) - list of requirements to be checked
            skip_version_check (bool) - skip comparing versions of packages

        Returns:
            result (FilterRequirementsResult) - object containing two arguments:
              - in_snowflake - packages available in conda
              - unavailable - packages not available in conda
        """
        result = FilterRequirementsResult([], [])
        for package in packages:
            if self.is_package_available(
                package, skip_version_check=skip_version_check
            ):
                result.in_snowflake.append(package)
            else:
                log.info(
                    "'%s' not found in Snowflake Anaconda channel (or ignored)...",
                    package.name,
                )
                result.unavailable.append(package)
        return result

    def write_requirements_file_in_snowflake_format(
        self,
        file_path: SecurePath,
        requirements: List[Requirement],
    ):
        """Saves requirements to a file in format accepted by Snowflake SQL commands."""
        log.info("Writing requirements into file %s", file_path.path)
        formatted_requirements = []
        for requirement in requirements:
            if requirement.name and requirement.name in self._packages:
                snowflake_name = self._packages[requirement.name].snowflake_name
                formatted_requirements.append(
                    snowflake_name + requirement.formatted_specs
                )

        if formatted_requirements:
            file_path.write_text("\n".join(formatted_requirements))


class AnacondaPackagesManager(SqlExecutionMixin):
    _snowflake_channel_url: str = (
        "https://repo.anaconda.com/pkgs/snowflake/channeldata.json"
    )

    def find_packages_available_in_snowflake_anaconda(self) -> AnacondaPackages:
        """
        Finds Python packages available in Snowflake to use in functions and stored procedures.
        It tries to get the list of packages using SQL query
        but if the try fails then the fallback is to parse JSON containing info about Snowflake's Anaconda channel.
        """
        packages = self._query_snowflake_for_available_packages()
        return AnacondaPackages(packages)

    def _query_snowflake_for_available_packages(self) -> dict[str, AvailablePackage]:
        cursor = self.execute_query(
            "select package_name, version from snowflake.information_schema.packages where language = 'python'",
            cursor_class=DictCursor,
        )
        if cursor.rowcount is None or cursor.rowcount == 0:
            raise SnowflakeSQLExecutionError()
        packages: dict[str, AvailablePackage] = {}
        for row in cursor:
            if not (package_name := row["PACKAGE_NAME"]):
                continue
            if not (version := row["VERSION"]):
                continue
            standardized_name = Requirement.standardize_name(package_name)
            if standardized_name in packages:
                packages[standardized_name].versions.add(version)
            else:
                packages[standardized_name] = AvailablePackage(
                    snowflake_name=package_name, versions={version}
                )
        return packages
