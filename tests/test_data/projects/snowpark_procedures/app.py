from __future__ import annotations

import sys

from snowflake.snowpark import Session


def hello(session: Session, name: str) -> str:
    return f"Hello {name}"


def test(session: Session) -> str:
    return "Test procedure"


# For local debugging. Be aware you may need to type-convert arguments if
# you add input parameters
if __name__ == "__main__":
    from snowflake.cli.api.config import cli_config

    session = Session.builder.configs(cli_config.get_connection("dev")).create()
    if len(sys.argv) > 1:
        print(hello(session, *sys.argv[1:]))  # type: ignore
    else:
        print(hello(session))  # type: ignore
    session.close()
