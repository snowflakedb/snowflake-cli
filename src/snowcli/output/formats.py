from enum import Enum


class OutputFormat(Enum):
    TABLE = "TABLE"
    JSON = "JSON"

    @classmethod
    def from_str(cls, value: str):
        return cls(value.upper())
