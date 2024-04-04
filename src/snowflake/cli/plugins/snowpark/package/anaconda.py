from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Set

import requests
from click import ClickException
from packaging.requirements import InvalidRequirement
from packaging.requirements import Requirement as PkgRequirement
from packaging.version import InvalidVersion, parse
from requests import HTTPError
from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    SplitRequirements,
)

log = logging.getLogger(__name__)


@dataclass
class AnacondaPackageData:
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


class AnacondaChannel:
    snowflake_channel_url: str = (
        "https://repo.anaconda.com/pkgs/snowflake/channeldata.json"
    )

    def __init__(self, packages: Dict[str, AnacondaPackageData]):
        """
        [packages] should be a dictionary mapping package name to set of its available versions.
        All package names should be provided in wheel escape format:
        https://peps.python.org/pep-0491/#escaping-and-unicode
        """
        self._packages = packages

    def is_package_available(
        self, package: Requirement, skip_version_check: bool = False
    ) -> bool:
        """
        Checks of a requirement is available in the Snowflake Anaconda Channel.

        As snowflake currently doesn't support extra syntax (ex. `jinja2[diagrams]`), if such
        extra is present in the dependency, we mark it as unavailable.
        """
        if not package.name or package.extras:
            return False
        if package.name not in self._packages:
            return False
        if skip_version_check or not package.specs:
            return True
        return self._packages[package.name].is_required_version_available(package)

    def package_latest_version(self, package: Requirement) -> str | None:
        if package.name not in self._packages:
            return None
        try:
            return str(max(self._packages[package.name].iter_versions()))
        except InvalidVersion:
            # fail-safe for non-pep8 versions
            return max(self._packages[package.name].versions)

    def to_anaconda_requirement_format(self, requirement: Requirement) -> str | None:
        """
        Returns requirement in format ready to be passed to Snowflake commands.
        If package name cannot be found in anaconda channel, returns None.
        """
        if not requirement.name or requirement.name not in self._packages:
            return None
        snowflake_name = self._packages[requirement.name].snowflake_name
        return f"{snowflake_name}{','.join(spec[0] + spec[1] for spec in requirement.specs)}"

    @classmethod
    def from_snowflake(cls):
        try:
            response = requests.get(AnacondaChannel.snowflake_channel_url)
            response.raise_for_status()
            packages = {}
            for key, package in response.json()["packages"].items():
                if not (version := package.get("version")):
                    continue
                package_name = package.get("name", key)
                standardized_name = Requirement.standardize_name(package_name)
                packages[standardized_name] = AnacondaPackageData(
                    snowflake_name=package_name, versions={version}
                )
            return cls(packages)

        except HTTPError as err:
            raise ClickException(
                f"Accessing Snowflake Anaconda channel failed. Reason {err}"
            )

    def parse_anaconda_packages(
        self, packages: List[Requirement], skip_version_check: bool = False
    ) -> SplitRequirements:
        """
        Checks if a list of packages are available in the Snowflake Anaconda channel.
        Returns an object with two attributes: 'snowflake' and 'other'.
        Each key contains a list of Requirement object.

        Parameters:
            packages (List[Requirement]) - list of requirements to be checked
            skip_version_check (bool) - skip comparing versions of packages

        Returns:
            result (SplitRequirements) - required packages split to those available in conda, and others, that need to be
                                         installed using pip

        """
        result = SplitRequirements([], [])
        for package in packages:
            if self.is_package_available(
                package, skip_version_check=skip_version_check
            ):
                result.in_snowflake.append(package)
            else:
                log.info(
                    "'%s' not found in Snowflake Anaconda channel...", package.name
                )
                result.unavailable.append(package)
        return result
