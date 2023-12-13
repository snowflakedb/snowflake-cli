from snowflake.snowpark import Session


def hello(session: Session, name: str) -> str:
    return f"Hello {name}"
