from snowflake.snowpark import Session
def hello_procedure(session: Session, name: str) -> str:
    return f"Hello {name}"


def test_procedure(session: Session) -> str:
    return "Test procedure"
