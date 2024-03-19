from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List

from requirements import requirement


class PypiOption(Enum):
    YES = "yes"
    NO = "no"
    ASK = "ask"


class Requirement(requirement.Requirement):
    extra_pattern = re.compile("'([^']*)'")

    @classmethod
    def parse_line(cls, line: str) -> Requirement:
        if len(line_elements := line.split(";")) > 1:
            line = line_elements[0]
        result = super().parse_line(line)

        if len(line_elements) > 1:
            for element in line_elements[1:]:
                if "extra" in element and (extras := cls.extra_pattern.search(element)):
                    result.extras.extend(extras.groups())

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


@dataclass
class RequirementWithWheelAndDeps:
    """A dataclass to hold a requirement and corresponding .whl file."""

    requirement: Requirement
    wheel_path: Path | None
    dependencies: List[str]

    def extract_files(self, destination: Path) -> None:
        if self.wheel_path is not None:
            with zipfile.ZipFile(self.wheel_path, "r") as whl:
                whl.extractall(destination)

    def namelist(self) -> List[str]:
        if self.wheel_path is None:
            return []
        with zipfile.ZipFile(self.wheel_path, "r") as whl:
            return whl.namelist()


@dataclass
class WheelMetadata:
    """A dataclass to hold metadata from .whl file"""

    name: str
    wheel_path: Path
    dependencies: List[str]

    @classmethod
    def from_wheel(cls, wheel_path: Path):
        with zipfile.ZipFile(wheel_path, "r") as whl:
            metadata_path = [
                path for path in whl.namelist() if path.endswith(".dist-info/METADATA")
            ]
            if len(metadata_path) != 1:
                # malformatted wheel package
                return None

            root = zipfile.Path(whl)
            metadata = (root / metadata_path[0]).read_text()

            dep_keyword = "Requires-Dist:"
            name_keyword = "Name:"
            dependencies = []
            name = None
            for line in metadata.splitlines():
                if line.startswith(dep_keyword):
                    dependencies.append(line[len(dep_keyword) :].strip())
                if line.startswith(name_keyword):
                    if name is not None:
                        return None
                    name = line[len(name_keyword) :].strip()

            if name is None:
                return None
            return cls(name=name, wheel_path=wheel_path, dependencies=dependencies)


def get_package_name(name: str) -> str:
    if name.lower().startswith(("git+", "http")):
        pattern = re.compile(r"github\.com\/[^\/]+\/([^\/][^.@$/]+)")
        if match := pattern.search(name):
            return match.group(1)
        else:
            return name

    elif name.endswith(".zip"):
        return name.replace(".zip", "")
    else:
        return name


pip_failed_msg = (
    "pip failed with return code {}."
    "If pip is installed correctly, this may mean you`re trying to install a package"
    "that isn't compatible with the host architecture -"
    "and generally means it has native libraries."
)
second_chance_msg = (
    "Good news! The following package dependencies can be"
    "imported directly from Anaconda, and will be excluded from"
    "the zip: {}"
)
