from __future__ import annotations

import sys

from snowflake.snowpark import Session


def hello(session: Session) -> str:
    return "Hello World!"


# For local debugging. Be aware you may need to type-convert arguments if
# you add input parameters
if __name__ == "__main__":
    from local_connection import get_dev_config

    session = Session.builder.configs(get_dev_config("dev")).create()
    if len(sys.argv) > 1:
        print(hello(session, *sys.argv[1:]))  # type: ignore
    else:
        print(hello(session))  # type: ignore
    session.close()
