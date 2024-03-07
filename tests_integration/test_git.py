import pytest
from snowflake.connector.errors import ProgrammingError
from pathlib import Path
import tempfile

FILE_IN_REPO = "RELEASE-NOTES.md"


@pytest.fixture
def sf_git_repository(runner, test_database):
    repo_name = "SNOWCLI_TESTING_REPO"
    integration_name = "SNOW_GIT_TESTING_API_INTEGRATION"

    if not _integration_exists(runner, integration_name=integration_name):
        result = runner.invoke_with_connection(
            [
                "sql",
                "-q",
                f"""
                CREATE API INTEGRATION {integration_name}
                API_PROVIDER = git_https_api
                API_ALLOWED_PREFIXES = ('https://github.com/snowflakedb/')
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
            ORIGIN = 'https://github.com/snowflakedb/snowflake-cli.git'   
            """,
        ]
    )
    assert result.exit_code == 0
    return repo_name


@pytest.mark.integration
def test_object_commands(runner, sf_git_repository):
    # object list
    result = runner.invoke_with_connection_json(["object", "list", "git-repository"])
    assert result.exit_code == 0
    assert sf_git_repository in _filter_key(result.json, key="name")

    # describe
    result = runner.invoke_with_connection_json(
        ["object", "describe", "git-repository", sf_git_repository]
    )
    assert result.exit_code == 0
    assert result.json[0]["name"] == sf_git_repository

    # drop
    result = runner.invoke_with_connection_json(
        ["object", "drop", "git-repository", sf_git_repository]
    )
    assert result.exit_code == 0
    assert result.json == [{"status": f"{sf_git_repository} successfully dropped."}]


@pytest.mark.integration
def test_fetch(runner, sf_git_repository):
    result = runner.invoke_with_connection_json(["git", "fetch", sf_git_repository])
    assert result.exit_code == 0
    assert result.json == [
        {
            "status": f"Git Repository {sf_git_repository} is up to date. No change was fetched."
        }
    ]


@pytest.mark.integration
def test_list_branches_and_tags(runner, sf_git_repository):
    # list branches
    result = runner.invoke_with_connection_json(
        ["git", "list-branches", sf_git_repository]
    )
    assert result.exit_code == 0
    assert "main" in _filter_key(result.json, key="name")

    # list tags
    result = runner.invoke_with_connection_json(["git", "list-tags", sf_git_repository])
    assert result.exit_code == 0
    assert "v2.0.0" in _filter_key(result.json, key="name")


@pytest.mark.integration
def test_list_files(runner, sf_git_repository):
    # error messages are passed to the user
    result = runner.invoke_with_connection(["git", "list-files", sf_git_repository])
    _assert_invalid_repo_path_error_message(result.output)

    try:
        repository_path = f"@{sf_git_repository}"
        runner.invoke_with_connection(["git", "list-files", repository_path])
        assert False, "Expected exception"
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "Files paths in git repositories must specify a scope. For example, a branch name, "
            "a tag name, or a valid commit hash. Commit hashes are between 6 and 40 characters long."
        )

    repository_path = f"@{sf_git_repository}/tags/v2.1.0-rc1/"
    result = runner.invoke_with_connection_json(["git", "list-files", repository_path])
    assert result.exit_code == 0
    assert f"{repository_path[1:].lower()}{FILE_IN_REPO}" in _filter_key(
        result.json, key="name"
    )


@pytest.mark.integration
def test_copy_to_stage(runner, sf_git_repository):
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v2.1.0-rc0"
    SUBDIR = "tests_integration/config"
    SUBDIR_ON_STAGE = "config"
    FILE_IN_SUBDIR = "connection_configs.toml"
    STAGE_NAME = "a_perfect_stage_for_testing"

    def _assert_file_on_stage(file_path):
        result = runner.invoke_with_connection_json(
            ["object", "stage", "list", STAGE_NAME]
        )
        assert result.exit_code == 0
        print([f["name"] for f in result.json])
        assert f"{STAGE_NAME.lower()}/{file_path}" in [f["name"] for f in result.json]

    # create stage for testing copy
    result = runner.invoke_with_connection(["object", "stage", "create", STAGE_NAME])
    assert result.exit_code == 0

    # copy directory - whole directory
    repository_path = f"{REPO_PATH_PREFIX}/{SUBDIR}"
    result = runner.invoke_with_connection_json(
        ["git", "copy", repository_path, f"@{STAGE_NAME}"]
    )
    assert result.exit_code == 0
    _assert_file_on_stage(f"{SUBDIR_ON_STAGE}/{FILE_IN_SUBDIR}")  # whole dir is copied

    # copy directory - copy contents
    repository_path = f"{REPO_PATH_PREFIX}/{SUBDIR}/"
    result = runner.invoke_with_connection_json(
        ["git", "copy", repository_path, f"@{STAGE_NAME}"]
    )
    assert result.exit_code == 0
    _assert_file_on_stage(FILE_IN_SUBDIR)  # contents are copied

    # copy single file
    repository_path = f"{REPO_PATH_PREFIX}/{FILE_IN_REPO}"
    result = runner.invoke_with_connection_json(
        ["git", "copy", repository_path, f"@{STAGE_NAME}"]
    )
    assert result.exit_code == 0
    _assert_file_on_stage(FILE_IN_REPO)

    # copy file into directory
    repository_path = f"{REPO_PATH_PREFIX}/{FILE_IN_REPO}"
    result = runner.invoke_with_connection_json(
        ["git", "copy", repository_path, f"@{STAGE_NAME}/a_dir/"]
    )
    assert result.exit_code == 0
    _assert_file_on_stage(f"a_dir/{FILE_IN_REPO}")
    # error with no '/' at the end should be fixed by snowcli
    repository_path = f"{REPO_PATH_PREFIX}/{FILE_IN_REPO}"
    result = runner.invoke_with_connection_json(
        ["git", "copy", repository_path, f"@{STAGE_NAME}/another_dir"]
    )
    assert result.exit_code == 0
    _assert_file_on_stage(f"another_dir/{FILE_IN_REPO}")


@pytest.mark.integration
def test_copy_to_local_file_system(runner, sf_git_repository):
    # TODO: change subdir to dedicated one after merging this to main
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v2.1.0-rc0"
    SUBDIR = "tests_integration/config"
    FILE_IN_SUBDIR = "connection_configs.toml"
    with tempfile.TemporaryDirectory() as tmp_dir:
        LOCAL_DIR = Path(tmp_dir) / "a_dir"
        assert not LOCAL_DIR.exists()

        # copy directory - GET only copy contents
        repository_path = f"{REPO_PATH_PREFIX}/{SUBDIR}"
        result = runner.invoke_with_connection_json(
            ["git", "copy", repository_path, str(LOCAL_DIR)]
        )
        assert result.exit_code == 0
        assert LOCAL_DIR.exists()  # create directory if not exists
        assert (LOCAL_DIR / FILE_IN_SUBDIR).exists()  # contents are copied

        # copy single file
        repository_path = f"{REPO_PATH_PREFIX}/{FILE_IN_REPO}"
        result = runner.invoke_with_connection_json(
            ["git", "copy", repository_path, str(LOCAL_DIR)]
        )
        assert result.exit_code == 0
        assert (LOCAL_DIR / FILE_IN_REPO).exists()

        # error messages are passed to the user
        try:
            repository_path = f"@{sf_git_repository}/tags/no-such-tag/"
            runner.invoke_with_connection(
                ["git", "copy", repository_path, str(LOCAL_DIR)]
            )
            assert False, "Expected exception"
        except ProgrammingError as err:
            assert (
                err.raw_msg
                == "The specified tag 'no-such-tag' cannot be found in the Git Repository."
            )


def _filter_key(objects, *, key):
    return [o[key] for o in objects]


def _assert_invalid_repo_path_error_message(output):
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
