from __future__ import annotations

from PyRTF.Elements import StyleSheet
from snowflake.snowpark import Session


def hello_function(name: str) -> str:
    return StyleSheet.__str__ + name


def hello_procedure(session: Session) -> str:
    return StyleSheet.__str__
