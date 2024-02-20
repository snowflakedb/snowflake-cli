from __future__ import annotations

import sys

from common import print_hello
from snowflake.snowpark import Session


def hello_procedure(session: Session, name: str) -> str:
    return print_hello(name)


def test_procedure(session: Session) -> str:
    """Example showing how to access credentials for external access"""
    import _snowflake

    credentials = _snowflake.get_generic_secret_string("cred")
    return "Test procedure"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.config("local_testing", True).getOrCreate() as session:
        print(hello_procedure(session, *sys.argv[1:]))  # type: ignore
