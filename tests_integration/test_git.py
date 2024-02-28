import time
from unittest import mock

import pytest


@pytest.fixture
def git_repository(runner):
    repo_name = "GITHUB_SNOWCLI_API_INTEGRATION"
    integration_name = "GITHUB_SNOWCLI_API_INTEGRATION"

    if not _integration_exists(runner, integration_name=integration_name):
        result = runner.invoke_with_connection(
            [
                "sql",
                "-q",
                f"""
                CREATE API INTEGRATION {integration_name}
                API_PROVIDER = git_https_api
                API_ALLOWED_PREFIXES = ('https://github.com/Snowflake-Labs')
                ALLOWED_AUTHENTICATION_SECRETS = ()
                ENABLED = true
            """,
            ]
        )
        assert result.exit_code == 0

    result = runner.invoke_with_connection(
        [
            "sql",
            "-q",
            f"""
            CREATE GIT REPOSITORY {repo_name}            
            API_INTEGRATION = {integration_name}
            ORIGIN = 'https://github.com/Snowflake-Labs/snowflake-cli.git'   
            """,
        ]
    )
    assert result.exit_code == 0
    return repo_name


@pytest.mark.integration
def test_flow(runner, git_repository):
    result = runner.invoke_with_connection(["object", "list", "git-repository"])
    print(result.output)
    assert result.exit_code == 0


def _integration_exists(runner, integration_name):
    result = runner.invoke_with_connection_json(["sql", "-q", "SHOW INTEGRATIONS"])
    assert result.exit_code == 0
    return any(integration["name"] == integration_name for integration in result.json)
