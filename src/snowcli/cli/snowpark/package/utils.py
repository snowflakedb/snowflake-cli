from dataclasses import dataclass
from pathlib import Path

from snowcli.utils import SplitRequirements


@dataclass
class LookupResult:
    requirements: SplitRequirements


class InAnaconda(LookupResult):
    pass


class RequiresPackages(LookupResult):
    pass


class NotInAnaconda(LookupResult):
    pass


class NothingFound(LookupResult):
    pass


@dataclass
class CreateResult:
    file_name: Path = Path()


class CreatedSuccessfully(CreateResult):
    pass


class CreationError(CreateResult):
    pass
