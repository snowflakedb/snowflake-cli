import os

import pytest

COMMAND = "check-snowsql-env-vars"


@pytest.fixture(name="pristine_env", scope="function", autouse=True)
def kepp_env_clean():
    old_env = os.environ.copy()
    yield
    os.environ = old_env


def test_no_evn_present(runner):
    result = runner.invoke(("helpers", COMMAND))
    assert result.exit_code == 0
    assert "Found 0 SnowSQL environment variables." in result.output


def test_env_found(runner):
    os.environ["SNOWSQL_ACCOUNT"] = "test"
    os.environ["SNOWSQL_WAREHOUSE"] = "whwhw"
    result = runner.invoke(("helpers", COMMAND))
    assert result.exit_code == 0
    assert "Found 2 SnowSQL environment variables." in result.output
