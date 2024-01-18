from __future__ import annotations

import _snowflake
from common import print_hello
from snowflake.snowpark import Session


def hello_procedure(session: Session, name: str) -> str:
    return print_hello(name)


def test_procedure(session: Session) -> str:
    credentials = _snowflake.get_generic_secret_string("cred")
    return "Test procedure"
