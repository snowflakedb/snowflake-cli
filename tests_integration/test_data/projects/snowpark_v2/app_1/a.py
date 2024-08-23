from __future__ import annotations
from snowflake.snowpark import Session


# test import
import syrupy


def hello_procedure(session: Session, name: str) -> str:
    return f"Hello {name}"
