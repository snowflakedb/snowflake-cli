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

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import List

from requirements import requirement


class Requirement(requirement.Requirement):
    extra_pattern = re.compile("'([^']*)'")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.package_name = None

    @classmethod
    def parse_line(cls, line: str) -> Requirement:
        if len(line_elements := line.split(";")) > 1:
            line = line_elements[0]
        result = super().parse_line(line)

        if len(line_elements) > 1:
            for element in line_elements[1:]:
                if "extra" in element and (extras := cls.extra_pattern.search(element)):
                    result.extras.extend(extras.groups())

        result.package_name = result.name

        if result.uri and not result.name:
            result.name = get_package_name(result.uri)
        result.name = cls.standardize_name(result.name)

        return result

    @staticmethod
    def standardize_name(name: str) -> str:
        return WheelMetadata.to_wheel_name_format(name.lower())

    @property
    def formatted_specs(self):
        return ",".join(sorted(spec[0] + spec[1] for spec in self.specs))

    @property
    def name_and_version(self):
        return self.name + self.formatted_specs


@dataclass
class RequirementWithFiles:
    """A dataclass to hold a requirement and the path to the
    downloaded files/folders that belong to it"""

    requirement: Requirement
    files: List[str]


@dataclass
class RequirementWithWheel:
    """A dataclass to hold a requirement and corresponding .whl file."""

    requirement: Requirement
    wheel_path: Path | None

    def extract_files(self, destination: Path) -> None:
        if self.wheel_path is not None:
            zipfile.ZipFile(self.wheel_path).extractall(destination)

    def namelist(self) -> List[str]:
        if self.wheel_path is None:
            return []
        return zipfile.ZipFile(self.wheel_path).namelist()


@dataclass
class WheelMetadata:
    """A dataclass to hold metadata from .whl file.
    [name] is the name of the package standardized according to
    https://peps.python.org/pep-0491/#escaping-and-unicode
    """

    name: str
    wheel_path: Path
    dependencies: List[str]

    @classmethod
    def from_wheel(cls, wheel_path: Path):
        """Parses wheel metadata according to
        https://peps.python.org/pep-0491/#file-contents"""
        with zipfile.ZipFile(wheel_path, "r") as whl:
            metadata_path = [
                path for path in whl.namelist() if path.endswith(".dist-info/METADATA")
            ]
            if len(metadata_path) != 1:
                # malformatted wheel package
                return None

            root = zipfile.Path(whl)
            metadata = (root / metadata_path[0]).read_text(encoding="utf-8")

            dep_keyword = "Requires-Dist:"
            dependencies = [
                line[len(dep_keyword) :].strip()
                for line in metadata.splitlines()
                if line.startswith(dep_keyword)
            ]
            name = cls._get_name_from_wheel_filename(wheel_path.name)
            return cls(name=name, wheel_path=wheel_path, dependencies=dependencies)

    @staticmethod
    def _get_name_from_wheel_filename(wheel_filename: str) -> str:
        # wheel filename is in format {name}-{version}[-{extra info}]
        # https://peps.python.org/pep-0491/#file-name-convention
        return wheel_filename.split("-")[0]

    @staticmethod
    def to_wheel_name_format(package_name: str) -> str:
        # https://peps.python.org/pep-0491/#escaping-and-unicode
        return re.sub(r"[^\w\d.]+", "_", package_name, re.UNICODE)


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
