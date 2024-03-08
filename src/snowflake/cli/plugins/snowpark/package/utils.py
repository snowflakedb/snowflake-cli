from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent
from typing import List

from requirements.requirement import Requirement
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import SplitRequirements


@dataclass
class LookupResult:
    requirements: SplitRequirements
    name: str

    @property
    def message(self):
        return ""


class InAnaconda(LookupResult):
    @property
    def message(self):
        return f"Package {self.name} is available on the Snowflake Anaconda channel."


class NotInAnaconda(LookupResult):
    @property
    def message(self):
        return dedent(
            f"""
        The package {self.name} is not available on Snowflake Anaconda. You can create a zip using:
        snow snowpark package create {self.name} --pypi-download
        """
        )


class NotInAnacondaButRequiresNativePackages(NotInAnaconda):
    @property
    def message(self):
        return dedent(
            f"""
        The package {self.name} is not available on Snowflake Anaconda and requires native libraries.
        You can try to create a zip using:
        snow snowpark package create {self.name} --pypi-download
        """
        )


def prepare_app_zip(file_path: SecurePath, temp_dir: SecurePath) -> SecurePath:
    # get filename from file path (e.g. app.zip from /path/to/app.zip)
    # TODO: think if no file exceptions are handled correctly
    file_name = file_path.path.name
    temp_path = temp_dir / file_name
    file_path.copy(temp_path.path)
    return temp_path


def get_readable_list_of_requirements(reqs: List[Requirement]):
    return "\n".join((req.line for req in reqs))
