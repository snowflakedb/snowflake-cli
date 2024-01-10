from __future__ import annotations

from PyRTF.Elements import StyleSheet


def hello_function(name: str) -> str:
    return f"{StyleSheet.__str__} {name}"
