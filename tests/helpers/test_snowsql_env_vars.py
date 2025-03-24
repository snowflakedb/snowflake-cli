import os

import pytest

COMMAND = "check-snowsql-env-vars"


@pytest.fixture(name="pristine_env", scope="function", autouse=True)
def kepp_env_clean():
    old_env = os.environ.copy()
    for e in old_env:
        if e.startswith("SNOWSQL_"):
            os.environ.pop(e, None)
    yield
    os.environ = old_env


def test_replecaple_env_found(runner):
    os.environ["SNOWSQL_ACCOUNT"] = "test"
    os.environ["SNOWSQL_WAREHOUSE"] = "whwhw"
    result = runner.invoke(("helpers", COMMAND))
    assert result.exit_code == 0
    assert (
        "Found 2 SnowSQL environment variables, 2 with replacements, 0 unused."
        in result.output
    )


def test_non_replacable_env_found(runner):
    os.environ["SNOWSQL_ABC"] = "test"
    os.environ["SNOWSQL_CBD"] = "whwhw"
    result = runner.invoke(("helpers", COMMAND))
    assert result.exit_code == 0
    assert (
        "Found 2 SnowSQL environment variables, 0 with replacements, 2 unused."
        in result.output
    )


def test_no_env_found(runner):
    result = runner.invoke(("helpers", COMMAND))
    assert result.exit_code == 0
    assert (
        "Found 0 SnowSQL environment variables, 0 with replacements, 0 unused."
        in result.output
    )
