from __future__ import annotations

from dummy_pkg_for_tests import shrubbery
from snowflake.snowpark import Session


def test_procedure(session: Session) -> str:
    return shrubbery.knights_of_nii_says()


def test_function() -> str:
    return shrubbery.knights_of_nii_says()
