from __future__ import annotations

from dummy_pkg_for_tests_with_deps import shrubbery


def hello_function(name: str) -> str:
    return shrubbery.monty_quotes()
