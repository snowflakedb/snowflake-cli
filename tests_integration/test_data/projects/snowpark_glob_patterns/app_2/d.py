from __future__ import annotations
from snowflake.snowpark import Session


# test import
import syrupy


def test_procedure(session: Session) -> str:
    return "Test procedure"
