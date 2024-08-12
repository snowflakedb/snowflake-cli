from dataclasses import dataclass
from enum import Enum


class OnErrorType(Enum):
    """
    Command option values for what to do when an error occurs.
    """

    BREAK = "break"
    CONTINUE = "continue"


@dataclass
class Variable:
    """
    Key-value pair dataclass, returned after parsing "key=value" command options.
    """

    key: str
    value: str

    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value
