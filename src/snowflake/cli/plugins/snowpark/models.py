from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List

from requirements.requirement import Requirement


class PypiOption(Enum):
    YES = "yes"
    NO = "no"
    ASK = "ask"


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
