from __future__ import annotations
from snowflake.snowpark import Session
from b import test_procedure


# test import
import syrupy


def hello_procedure(session: Session, name: str) -> str:

    return f"Hello {name}" + test_procedure(session)
