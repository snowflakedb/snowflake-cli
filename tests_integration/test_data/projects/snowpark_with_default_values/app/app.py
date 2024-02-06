from typing import List

from snowflake.snowpark import Session


def whole_new_word(base: str, mult: int, suffix: str) -> str:
    return base * mult + suffix


def whole_new_word_procedure(
    session: Session, base: str, mult: int, suffix: str
) -> str:
    return base * mult + suffix


def check_all_types(s: str, i: int, b1: bool, b2: bool, f: float, l: List[int]) -> str:
    return f"s:{s}, i:{i}, b1:{b1}, b2:{b2}, f:{f}, l:{l}"
