import pytest
from snowflake.connector.errors import ProgrammingError
from pathlib import Path
import tempfile

TAG_NAME = "a-particular-tag"
BRANCH_NAME = "a-branch-which-is-different-than-main"


@pytest.fixture
def git_repository(runner, test_database):
    repo_name = "SNOWCLI_DUMMY_REPO"
    integration_name = "SNOWCLI_DUMMY_REPO_API_INTEGRATION"

    if not _integration_exists(runner, integration_name=integration_name):
        result = runner.invoke_with_connection(
            [
                "sql",
                "-q",
                f"""
                CREATE API INTEGRATION {integration_name}
                API_PROVIDER = git_https_api
                API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-pczajka')
                ALLOWED_AUTHENTICATION_SECRETS = ()
                ENABLED = true
            """,
            ]
        )
        assert result.exit_code == 0

    result = runner.invoke_with_connection(
        [
            "git",
            "setup",
            repo_name,
            "--api-integration",
            integration_name,
            "--url",
            "https://github.com/sfc-gh-pczajka/dummy-repo-for-snowcli-testing.git",
        ]
    )
    assert result.exit_code == 0
    return repo_name


@pytest.mark.integration
def test_object_commands(runner, git_repository):
    # object list
    result = runner.invoke_with_connection_json(["object", "list", "git-repository"])
    assert result.exit_code == 0
    assert _filter_key(result.json, key="name") == [git_repository]

    # describe
    result = runner.invoke_with_connection_json(
        ["object", "describe", "git-repository", git_repository]
    )
    assert result.exit_code == 0
    assert result.json[0]["name"] == git_repository

    # drop
    result = runner.invoke_with_connection_json(
        ["object", "drop", "git-repository", git_repository]
    )
    assert result.exit_code == 0
    assert result.json == [{"status": "SNOWCLI_DUMMY_REPO successfully dropped."}]


@pytest.mark.integration
def test_fetch(runner, git_repository):
    result = runner.invoke_with_connection_json(["git", "fetch", git_repository])
    assert result.exit_code == 0
    assert result.json == [
        {
            "status": f"Git Repository {git_repository} is up to date. No change was fetched."
        }
    ]


@pytest.mark.integration
def test_list_branches_and_tags(runner, git_repository):
    # list branches
    result = runner.invoke_with_connection_json(
        ["git", "list-branches", git_repository]
    )
    assert result.exit_code == 0
    assert result.json == [
        {
            "checkouts": "",
            "commit_hash": "f1b8cf60445d9d4c9bee32501738df55d0b4312e",
            "name": "a-branch-which-is-different-than-main",
            "path": "/branches/a-branch-which-is-different-than-main",
        },
        {
            "checkouts": "",
            "commit_hash": "599e77bdbf59e29451f1a909baa2734f96b6c801",
            "name": "main",
            "path": "/branches/main",
        },
    ]

    # list tags
    result = runner.invoke_with_connection_json(["git", "list-tags", git_repository])
    assert result.exit_code == 0
    assert result.json == [
        {
            "author": None,
            "commit_hash": "8fb57b3f1cf69c84274e760e86b16eb9933b45d5",
            "message": None,
            "name": "a-particular-tag",
            "path": "/tags/a-particular-tag",
        },
    ]


@pytest.mark.integration
def test_list_files(runner, git_repository):
    # error messages
    result = runner.invoke_with_connection(["git", "list-files", git_repository])
    _assert_error_message_in_output(result.output)

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

    # list-files - branch - no '/' at the end
    repository_path = f"@{git_repository}/branches/{BRANCH_NAME}"
    result = runner.invoke_with_connection_json(["git", "list-files", repository_path])
    assert result.exit_code == 0
    prefix = repository_path[1:].lower()
    assert _filter_key(result.json, key="name") == [
        f"{prefix}/an_existing_directory/file_inside.txt",
        f"{prefix}/file.txt",
        f"{prefix}/file_only_present_on_a_different_branch.txt",
    ]

    # list-files - tags - '/' at the end
    repository_path = f"@{git_repository}/tags/{TAG_NAME}/"
    result = runner.invoke_with_connection_json(["git", "list-files", repository_path])
    assert result.exit_code == 0
    prefix = repository_path[1:-1].lower()
    assert _filter_key(result.json, key="name") == [
        f"{prefix}/an_existing_directory/file_inside.txt",
        f"{prefix}/file.txt",
        f"{prefix}/file_only_present_on_a_particular_tag.txt",
    ]


@pytest.mark.integration
def test_copy(runner, git_repository):
    # create stage for testing copy
    STAGE_NAME = "a_perfect_stage_for_testing"
    result = runner.invoke_with_connection(["sql", "-q", f"create stage {STAGE_NAME}"])
    assert result.exit_code == 0

    # copy - test error messages
    result = runner.invoke_with_connection(
        ["git", "copy", git_repository, f"@{STAGE_NAME}"]
    )
    _assert_error_message_in_output(result.output)
    try:
        repository_path = f"@{git_repository}"
        runner.invoke_with_connection(
            ["git", "copy", repository_path, f"@{STAGE_NAME}"]
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
            ["git", "copy", repository_path, f"@{STAGE_NAME}"]
        )
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "The specified branch 'branch_which_does_not_exist' cannot be found in the Git Repository."
        )

    try:
        repository_path = f"@{git_repository}/tags/tag_which_does_not_exist"
        runner.invoke_with_connection(
            ["git", "copy", repository_path, f"@{STAGE_NAME}"]
        )
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "The specified tag 'tag_which_does_not_exist' cannot be found in the Git Repository."
        )

    # copy to stage - success
    repository_path = f"@{git_repository}/branches/{BRANCH_NAME}"
    result = runner.invoke_with_connection_json(
        ["git", "copy", repository_path, f"@{STAGE_NAME}"]
    )
    assert result.exit_code == 0
    assert result.json == [
        {"file": "an_existing_directory/file_inside.txt"},
        {"file": "file.txt"},
        {"file": "file_only_present_on_a_different_branch.txt"},
    ]

    result = runner.invoke_with_connection_json(["object", "stage", "list", STAGE_NAME])
    assert result.exit_code == 0
    assert _filter_key(result.json, key="name") == [
        f"{STAGE_NAME}/an_existing_directory/file_inside.txt",
        f"{STAGE_NAME}/file.txt",
        f"{STAGE_NAME}/file_only_present_on_a_different_branch.txt",
    ]

    # copy to local file system - success
    with tempfile.TemporaryDirectory() as tmp_dir:
        repository_path = f"@{git_repository}/tags/{TAG_NAME}"
        result = runner.invoke_with_connection_json(
            ["git", "copy", repository_path, tmp_dir]
        )
        assert result.exit_code == 0
        assert (Path(tmp_dir) / "file_only_present_on_a_particular_tag.txt").exists()
        assert _filter_key(result.json, key="file") == [
            f"an_existing_directory/file_inside.txt",
            f"file.txt",
            f"file_only_present_on_a_particular_tag.txt",
        ]


def _filter_key(objects, *, key):
    return [o[key] for o in objects]


def _assert_error_message_in_output(output):
    assert "Error" in output
    assert (
        "REPOSITORY_PATH should be a path to git repository stage with scope" in output
    )
    assert "provided. For example: @my_repo/branches/main" in output


def _integration_exists(runner, integration_name):
    result = runner.invoke_with_connection_json(["sql", "-q", "SHOW INTEGRATIONS"])
    assert result.exit_code == 0
    return any(
        integration["name"].upper() == integration_name.upper()
        for integration in result.json
    )
