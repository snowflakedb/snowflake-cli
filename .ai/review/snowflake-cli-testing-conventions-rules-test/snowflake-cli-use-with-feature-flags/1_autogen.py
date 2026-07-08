import os

import pytest


def test_deploy_with_os_environ():
    # directly manipulating os.environ
    os.environ["SNOWFLAKE_CLI_FEATURES_ENABLE_NEW_FEATURE"] = "true"
    try:
        result = some_feature_function()
        assert result is not None
    finally:
        del os.environ["SNOWFLAKE_CLI_FEATURES_ENABLE_NEW_FEATURE"]


def test_deploy_with_monkeypatch(monkeypatch):
    # using monkeypatch.setenv instead of with_feature_flags
    monkeypatch.setenv(
        "SNOWFLAKE_CLI_FEATURES_ENABLE_DBT_PROJECTS_PROFILES_FILE_PRECEDENCE", "true"
    )
    result = deploy()
    assert result.exit_code == 0


@pytest.fixture
def enable_my_feature_bad(monkeypatch):
    # fixture also violates: should use with_feature_flags
    monkeypatch.setenv("SNOWFLAKE_CLI_FEATURES_ENABLE_MY_FEATURE", "true")
    yield
