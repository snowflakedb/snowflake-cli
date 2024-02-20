from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import List

from requirements import requirement


class PypiOption(Enum):
    YES = "yes"
    NO = "no"
    ASK = "ask"


class RequirementType(Enum):
    FILE = "file"
    PACKAGE = "package"


class Requirement(requirement.Requirement):
    @classmethod
    def parse_line(cls, line: str) -> Requirement:
        if len(extras := line.split(";")) > 1:
            line = extras[0]
        result = super().parse_line(line)

        if len(extras) > 1:
            result.extras = extras[1]
        if result.uri and not result.name:
            result.name = get_package_name(result.uri)

        return result

    @classmethod
    def _look_for_specifier(cls, specifier: str, line: str):
        return re.search(cls.specifier_pattern.format(specifier), line)


@dataclass
class SplitRequirements:
    """A dataclass to hold the results of parsing requirements files and dividing them into
    snowflake-supported vs other packages.
    """

    snowflake: List[Requirement]
    other: List[Requirement]


@dataclass
class RequirementWithFiles:
    """A dataclass to hold a requirement and the path to the
    downloaded files/folders that belong to it"""

    requirement: Requirement
    files: List[str]


@dataclass
class RequirementWithFilesAndDeps(RequirementWithFiles):
    dependencies: List[str]

    def to_requirement_with_files(self):
        return RequirementWithFiles(self.requirement, self.files)


pip_failed_msg = """pip failed with return code {}.
            If pip is installed correctly, this may mean you`re trying to install a package
            that isn't compatible with the host architecture -
            and generally means it has native libraries."""
second_chance_msg = """Good news! The following package dependencies can be
                imported directly from Anaconda, and will be excluded from
                the zip: {}"""


def get_package_name(name: str) -> str:
    if name.startswith("git+"):
        pattern = r"github\.com\/([^\/]+)\/([^\/.]+)(\.git)?"
        if match := re.search(pattern, name):
            return match.group(2)
        else:
            return name

    elif name.endswith(".zip"):
        return name.replace(".zip", "")
    else:
        return name
