import pytest

from click import ClickException


@pytest.fixture
def git_repository(runner, test_database):
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
def test_flow(runner, git_repository, snapshot):
    if False:
        # object list
        result = runner.invoke_with_connection_json(
            ["object", "list", "git-repository"]
        )
        assert result.exit_code == 0
        _assert_object_with_name_in(result.json, name=git_repository)

        # describe
        result = runner.invoke_with_connection_json(
            ["object", "describe", "git-repository", git_repository]
        )
        assert result.exit_code == 0
        assert result.json[0]["name"] == git_repository

        # fetch
        result = runner.invoke_with_connection_json(["git", "fetch", git_repository])
        assert result.exit_code == 0
        assert result.json == [
            {
                "status": f"Git Repository {git_repository} is up to date. No change was fetched."
            }
        ]

        # list branches
        result = runner.invoke_with_connection_json(
            ["git", "list-branches", git_repository]
        )
        assert result.exit_code == 0
        _assert_object_with_name_in(result.json, name="main")

        # list tags
        result = runner.invoke_with_connection_json(
            ["git", "list-tags", git_repository]
        )
        assert result.exit_code == 0
        _assert_object_with_name_in(result.json, name="v2.0.0rc2")

    # list files - error messages
    result = runner.invoke_with_connection(["git", "list-files", git_repository])
    assert (
        result.exit_code == 1
        and result.output == snapshot["list-files-not-stage-error"]
    )
    result = runner.invoke_with_connection(["git", "list-files"])

    repository_path = f"@{git_repository}"
    result = runner.invoke_with_connection(["git", "list-files", repository_path])
    print(result.__dict__)

    repository_path = f"@{git_repository}/branches/beach_which_does_not_exist"
    result = runner.invoke_with_connection(["git", "list-files", repository_path])
    print(result.__dict__)

    repository_path = f"@{git_repository}/tags/tag_which_does_not_exist"
    result = runner.invoke_with_connection(["git", "list-files", repository_path])
    print(result.__dict__)

    repository_path = f"@{git_repository}/branches/main"
    result = runner.invoke_with_connection_json(["git", "list-files", repository_path])
    # _assert_object_with_name_in()

    # for later
    # copy - error messages
    result = runner.invoke_with_connection(
        ["git", "copy", git_repository, "@some_stage"]
    )
    assert result.exit_code == 1 and result.output == snapshot["copy-not-stage-error"]


def _assert_object_with_name_in(objects, *, name):
    assert any(object["name"] == name for object in objects)


def _integration_exists(runner, integration_name):
    result = runner.invoke_with_connection_json(["sql", "-q", "SHOW INTEGRATIONS"])
    assert result.exit_code == 0
    return any(
        integration["name"].upper() == integration_name.upper()
        for integration in result.json
    )
