from dataclasses import dataclass

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
