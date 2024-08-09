from dataclasses import dataclass
from enum import Enum


class OnErrorType(Enum):
    BREAK = "break"
    CONTINUE = "continue"


@dataclass
class Variable:
    key: str
    value: str

    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value
