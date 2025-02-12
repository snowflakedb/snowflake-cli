import mypy


def check_mypy_version() -> str:
    from importlib.metadata import version

    return version("mypy")
