from __future__ import annotations

import syrupy
from PyRTF.Elements import StyleSheet
from snowflake.snowpark import Session


def hello_function(name: str) -> str:
    return f"{StyleSheet.__str__} {name}"


def hello_procedure(session: Session) -> str:
    return f"StyleSheet.__str__"

