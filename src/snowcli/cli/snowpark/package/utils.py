from dataclasses import dataclass

from snowcli.utils import SplitRequirements


class LookupResult:
    pass


@dataclass
class InAnaconda(LookupResult):
    requirements: SplitRequirements


@dataclass
class RequiresPackages(LookupResult):
    requirements: SplitRequirements


@dataclass
class Unsupported(LookupResult):
    requirements: SplitRequirements


class NothingFound(LookupResult):
    pass
