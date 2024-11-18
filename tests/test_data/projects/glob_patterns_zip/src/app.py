from __future__ import annotations

from dir.dir_app import print_hello
from snowflake.snowpark import Session


def hello_procedure(session: Session, name: str) -> str:
    return print_hello(name)


def hello_function(name: str) -> str:
    return print_hello(name)
