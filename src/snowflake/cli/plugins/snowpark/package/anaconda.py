from __future__ import annotations

import logging
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


class AnacondaChannel:
    snowflake_channel_url: str = (
        "https://repo.anaconda.com/pkgs/snowflake/channeldata.json"
    )

    def __init__(self, packages: Dict[str, Set[str]]):
        """
        [packages] should be a dictionary mapping package name to set of its available versions.
        All package names should be provided in wheel escape format:
        https://peps.python.org/pep-0491/#escaping-and-unicode
        """
        self._packages = packages

    def is_package_available(
        self, package: Requirement, skip_version_check: bool = False
    ) -> bool:
        if not package.name:
            return False
        if package.name not in self._packages:
            return False
        if skip_version_check or not package.specs:
            return True

        try:
            package_specifiers = PkgRequirement(package.line).specifier
            return any(
                parse(version) in package_specifiers
                for version in self._packages[package.name]
            )
        except (InvalidVersion, InvalidRequirement):
            # fail-safe for non-pep508 formats
            return False

    def package_latest_version(self, package: Requirement) -> str | None:
        if package.name not in self._packages:
            return None
        try:
            versions = {parse(v) for v in self._packages[package.name]}
        except InvalidVersion:
            versions = self._packages[package.name]
        return str(max(versions))

    @classmethod
    def from_snowflake(cls):
        try:
            response = requests.get(AnacondaChannel.snowflake_channel_url)
            response.raise_for_status()
            packages = {}
            for key, package in response.json()["packages"].items():
                packages[package.get("name", key)] = {package["version"]}
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
        Returns a dict with two keys: 'snowflake' and 'other'.
        Each key contains a list of Requirement object.

        As snowflake currently doesn't support extra syntax (ex. `jinja2[diagrams]`), if such
        extra is present in the dependency, we mark it as unavailable.

        Parameters:
            packages (List[Requirement]) - list of requirements to be checked
            skip_version_check (bool) - skip comparing versions of packages

        Returns:
            result (SplitRequirements) - required packages split to those available in conda, and others, that need to be
                                         installed using pip

        """
        result = SplitRequirements([], [])
        for package in packages:
            if package.extras:
                result.other.append(package)
            elif self.is_package_available(
                package, skip_version_check=skip_version_check
            ):
                result.snowflake.append(package)
            else:
                log.info(
                    "'%s' not found in Snowflake Anaconda channel...", package.name
                )
                result.other.append(package)
        return result
