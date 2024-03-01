import pytest
from snowflake.connector.errors import ProgrammingError
from pathlib import Path
import tempfile


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
    # object list
    result = runner.invoke_with_connection_json(["object", "list", "git-repository"])
    assert result.exit_code == 0
    _assert_object_with_key_value(result.json, key="name", value=git_repository)

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
    _assert_object_with_key_value(result.json, key="name", value="main")

    # list tags
    result = runner.invoke_with_connection_json(["git", "list-tags", git_repository])
    assert result.exit_code == 0
    _assert_object_with_key_value(result.json, key="name", value="v2.0.0rc2")

    # list files - error messages
    result = runner.invoke_with_connection(["git", "list-files", git_repository])
    assert result.output == snapshot(name="list-files-not-stage-error")

    try:
        repository_path = f"@{git_repository}"
        runner.invoke_with_connection(["git", "list-files", repository_path])
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "Files paths in git repositories must specify a scope. For example, a branch name, "
            "a tag name, or a valid commit hash. Commit hashes are between 6 and 40 characters long."
        )

    try:
        repository_path = f"@{git_repository}/branches/branch_which_does_not_exist"
        runner.invoke_with_connection(["git", "list-files", repository_path])
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "The specified branch 'branch_which_does_not_exist' cannot be found in the Git Repository."
        )

    try:
        repository_path = f"@{git_repository}/tags/tag_which_does_not_exist"
        runner.invoke_with_connection(["git", "list-files", repository_path])
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "The specified tag 'tag_which_does_not_exist' cannot be found in the Git Repository."
        )

    # list-files - success
    repository_path = f"@{git_repository}/branches/main"
    result = runner.invoke_with_connection_json(["git", "list-files", repository_path])
    _assert_object_with_key_value(
        result.json,
        key="name",
        value="github_snowcli_api_integration/branches/main/RELEASE-NOTES.md",
    )

    # create stage for testing copy
    stage_name = "a_perfect_stage_for_testing"
    result = runner.invoke_with_connection(["sql", "-q", f"create stage {stage_name}"])

    # copy - test error messages
    result = runner.invoke_with_connection(
        ["git", "copy", git_repository, f"@{stage_name}"]
    )
    assert result.output == snapshot(name="copy-not-stage-error")
    try:
        repository_path = f"@{git_repository}"
        runner.invoke_with_connection(
            ["git", "copy", repository_path, f"@{stage_name}"]
        )
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "Files paths in git repositories must specify a scope. For example, a branch name, "
            "a tag name, or a valid commit hash. Commit hashes are between 6 and 40 characters long."
        )

    try:
        repository_path = f"@{git_repository}/branches/branch_which_does_not_exist"
        runner.invoke_with_connection(
            ["git", "copy", repository_path, f"@{stage_name}"]
        )
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "The specified branch 'branch_which_does_not_exist' cannot be found in the Git Repository."
        )

    try:
        repository_path = f"@{git_repository}/tags/tag_which_does_not_exist"
        runner.invoke_with_connection(
            ["git", "copy", repository_path, f"@{stage_name}"]
        )
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "The specified tag 'tag_which_does_not_exist' cannot be found in the Git Repository."
        )

    # copy to stage - success
    repository_path = f"@{git_repository}/branches/main"
    result = runner.invoke_with_connection_json(
        ["git", "copy", repository_path, f"@{stage_name}"]
    )
    assert result.exit_code == 0
    _assert_object_with_key_value(result.json, key="file", value="RELEASE-NOTES.md")
    result = runner.invoke_with_connection_json(["object", "stage", "list", stage_name])
    _assert_object_with_key_value(
        result.json, key="name", value=f"{stage_name}/RELEASE-NOTES.md"
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        # copy to local file system - success
        repository_path = f"@{git_repository}/branches/main/"
        result = runner.invoke_with_connection_json(
            ["git", "copy", repository_path, tmp_dir]
        )
        assert result.exit_code == 0
        assert (Path(tmp_dir) / "RELEASE-NOTES.md").exists()
        import json

        print(json.dumps(result.json, indent=2))


def _assert_object_with_key_value(objects, *, key, value):
    assert any(object[key] == value for object in objects)


def _integration_exists(runner, integration_name):
    result = runner.invoke_with_connection_json(["sql", "-q", "SHOW INTEGRATIONS"])
    assert result.exit_code == 0
    return any(
        integration["name"].upper() == integration_name.upper()
        for integration in result.json
    )
