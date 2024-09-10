from dataclasses import dataclass
from typing import List


@dataclass
class HelpText:
    value: str


class ParameterDeclarations:
    decls: List[str]

    def __init__(self, *decls: str):
        self.decls = list(decls)
