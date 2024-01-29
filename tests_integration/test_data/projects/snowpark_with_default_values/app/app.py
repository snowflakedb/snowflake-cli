from snowflake.snowpark import Session


def whole_new_word(base: str, mult: int, suffix: str) -> str:
    return base * mult + suffix


def whole_new_word_procedure(
    session: Session, base: str, mult: int, suffix: str
) -> str:
    return base * mult + suffix
