import os
from unittest import mock

COMMAND = "check-snowsql-env-vars"

VALID_VARS = {
    "SNOWSQL_ACCOUNT": "test",
    "SNOWSQL_WAREHOUSE": "whwhw",
}
INVALID_VARS = {
    "SNOWSQL_ABC": "test",
    "SNOWSQL_CBD": "whwhw",
}


@mock.patch.dict(os.environ, VALID_VARS, clear=True)
def test_replecaple_env_found(runner):
    result = runner.invoke(("helpers", COMMAND))
    assert result.exit_code == 0
    assert (
        "Found 2 SnowSQL environment variables, 2 with replacements, 0 unused."
        in result.output
    )


@mock.patch.dict(os.environ, INVALID_VARS, clear=True)
def test_non_replacable_env_found(runner):
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
