from __future__ import annotations

import logging
from typing import List

import requests
from packaging.version import parse
from snowflake.cli.plugins.snowpark.models import Requirement, SplitRequirements

log = logging.getLogger(__name__)


class AnacondaChannel:
    snowflake_channel_url: str = (
        "https://repo.anaconda.com/pkgs/snowflake/channeldata.json"
    )

    def __init__(self, packages):
        self._packages = packages

    def is_package_available(
        self, package: Requirement, skip_version_check: bool = False
    ) -> bool:
        package_name = package.name.lower()
        if package_name not in self._packages:
            return False
        if package.specs and not skip_version_check:
            latest_ver = parse(self._packages[package_name]["version"])
            return all([parse(spec[1]) <= latest_ver for spec in package.specs])
        return True

    def package_version(self, package: Requirement):
        return self._packages[package.name.lower()].get("version")

    @classmethod
    def from_snowflake(cls):
        response = requests.get(AnacondaChannel.snowflake_channel_url)
        response.raise_for_status()
        return cls(packages=response.json()["packages"])

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
