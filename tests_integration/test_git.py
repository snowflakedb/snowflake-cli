# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from snowflake.connector.errors import ProgrammingError
from pathlib import Path
import tempfile

FILE_IN_REPO = "RELEASE-NOTES.md"


@pytest.fixture
def sf_git_repository(runner, test_database):
    repo_name = "SNOWCLI_TESTING_REPO"
    integration_name = "SNOWCLI_TESTING_REPO_API_INTEGRATION"
    communication = "\n".join(
        ["https://github.com/snowflakedb/snowflake-cli.git", "n", integration_name, ""]
    )
    result = runner.invoke_with_connection(
        ["git", "setup", repo_name], input=communication
    )
    assert result.exit_code == 0
    assert f"Git Repository {repo_name} was successfully created." in result.output
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
    result = runner.invoke_with_connection(["git", "fetch", sf_git_repository])
    # we check only command's exit code, as checking its output would be flaky
    # (the repository state changes often enough)
    assert result.exit_code == 0, result.output


@pytest.mark.integration
def test_list_branches_and_tags(runner, sf_git_repository):
    # list branches
    result = runner.invoke_with_connection_json(
        ["git", "list-branches", sf_git_repository]
    )
    assert result.exit_code == 0
    assert "main" in _filter_key(result.json, key="name")

    # list tags
    result = runner.invoke_with_connection_json(
        ["git", "list-tags", sf_git_repository, "--like", "v2.1.0%"]
    )
    assert result.exit_code == 0
    assert result.json == [
        {
            "author": None,
            "commit_hash": "f0f7d4bd706b92e1c4556d25bf4015cff30588ed",
            "message": None,
            "name": "v2.1.0",
            "path": "/tags/v2.1.0",
        },
        {
            "author": None,
            "commit_hash": "829887b758b43b86959611dd6127638da75cf871",
            "message": None,
            "name": "v2.1.0-rc0",
            "path": "/tags/v2.1.0-rc0",
        },
        {
            "author": None,
            "commit_hash": "b7efe1fe9c0925b95ba214e233b18924fa0404b3",
            "message": None,
            "name": "v2.1.0-rc1",
            "path": "/tags/v2.1.0-rc1",
        },
        {
            "author": None,
            "commit_hash": "36919a3ec01eea0541a1e17a064f6880e612193a",
            "message": None,
            "name": "v2.1.0-rc2",
            "path": "/tags/v2.1.0-rc2",
        },
    ]


@pytest.mark.integration
def test_list_files(runner, sf_git_repository):
    # error messages are passed to the user
    result = runner.invoke_with_connection(["git", "list-files", sf_git_repository])
    _assert_invalid_repo_path_error_message(result.output)

    repository_path = f"@{sf_git_repository}/branches/missing_slash"
    result = runner.invoke_with_connection(["git", "list-files", repository_path])
    _assert_invalid_repo_path_error_message(result.output)

    try:
        repository_path = f"@{sf_git_repository}/tags/no-such-tag/"
        runner.invoke_with_connection(["git", "list-files", repository_path])
        assert False, "Expected exception"
    except ProgrammingError as err:
        assert (
            err.raw_msg
            == "The specified tag 'no-such-tag' cannot be found in the Git Repository."
        )

    # list files with pattern
    repository_path = f"@{sf_git_repository}/tags/v2.1.0-rc1/"
    prefix = repository_path[1:-1].lower()
    result = runner.invoke_with_connection_json(
        ["git", "list-files", repository_path, "--pattern", "R.*\.md"]
    )
    assert result.exit_code == 0
    assert _filter_key(result.json, key="name") == [
        f"{prefix}/README.md",
        f"{prefix}/RELEASE-NOTES.md",
    ]


@pytest.mark.integration
def test_copy_to_stage(runner, sf_git_repository):
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v2.1.0-rc0"
    SUBDIR = "tests_integration/config"
    SUBDIR_ON_STAGE = "config"
    FILE_IN_SUBDIR = "connection_configs.toml"
    STAGE_NAME = "a_perfect_stage_for_testing"

    def _assert_file_on_stage(file_path):
        result = runner.invoke_with_connection_json(["stage", "list-files", STAGE_NAME])
        assert result.exit_code == 0
        assert f"{STAGE_NAME.lower()}/{file_path}" in [f["name"] for f in result.json]

    # create stage for testing copy
    result = runner.invoke_with_connection(["stage", "create", STAGE_NAME])
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
def test_copy_directory_to_local_file_system(runner, sf_git_repository, test_root_path):
    # Project with files in root and subdirectory
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v2.7.0"
    SUBDIR = "tests_integration/test_data/projects/snowpark_with_import"
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

        def _relative_content_set(root: Path):
            return set(x.relative_to(root) for x in root.rglob("*"))

        downloaded_files = _relative_content_set(LOCAL_DIR)
        expected_files = _relative_content_set(test_root_path.parent / SUBDIR)
        assert downloaded_files == expected_files  # contents are copied


@pytest.mark.integration
def test_copy_single_file_to_local_file_system(runner, sf_git_repository):
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v2.1.0-rc0"
    with tempfile.TemporaryDirectory() as tmp_dir:
        LOCAL_DIR = Path(tmp_dir) / "a_dir"
        assert not LOCAL_DIR.exists()

        # copy single file
        repository_path = f"{REPO_PATH_PREFIX}/{FILE_IN_REPO}"
        result = runner.invoke_with_connection_json(
            ["git", "copy", repository_path, str(LOCAL_DIR)]
        )
        assert result.exit_code == 0
        assert (LOCAL_DIR / FILE_IN_REPO).exists()


@pytest.mark.integration
def test_copy_error(runner, sf_git_repository):
    with tempfile.TemporaryDirectory() as tmp_dir:
        LOCAL_DIR = Path(tmp_dir) / "a_dir"
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


@pytest.mark.integration
def test_execute_with_name_in_pascal_case(
    runner, test_database, sf_git_repository, snapshot
):
    result = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{sf_git_repository}/branches/main/tests_integration/test_data/projects/stage_execute/ScriptInPascalCase.sql",
        ]
    )

    assert result.exit_code == 0
    assert result.json == snapshot


@pytest.mark.integration
def test_execute(runner, test_database, sf_git_repository, snapshot):
    result = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{sf_git_repository}/branches/main/tests_integration/test_data/projects/stage_execute/script_template.sql",
            "-D",
            "text='string'",
            "-D",
            "value=1",
            "-D",
            "boolean=TRUE",
            "-D",
            "null_value=NULL",
        ]
    )

    assert result.exit_code == 0
    assert result.json == snapshot


@pytest.mark.integration
def test_execute_fqn_repo(runner, test_database, sf_git_repository):
    result_fqn = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{test_database}.public.{sf_git_repository}/branches/main/tests_integration/test_data/projects/stage_execute/script_template.sql",
            "-D",
            "text='string'",
            "-D",
            "value=1",
            "-D",
            "boolean=TRUE",
            "-D",
            "null_value=NULL",
        ]
    )

    assert result_fqn.exit_code == 0
    assert result_fqn.json == [
        {
            "File": f"@{test_database}.public.{sf_git_repository}/branches/main/tests_integration/test_data/projects/stage_execute/script_template.sql",
            "Status": "SUCCESS",
            "Error": None,
        }
    ]


def _filter_key(objects, *, key):
    return [o[key] for o in objects]


def _assert_invalid_repo_path_error_message(output):
    assert "Error" in output
    assert (
        "REPOSITORY_PATH should be a path to git repository stage with scope" in output
    )
    assert (
        "provided. Path to the repository root must end with '/'. For example:"
        in output
    )
    assert "@my_repo/branches/main/" in output


def _integration_exists(runner, integration_name):
    result = runner.invoke_with_connection_json(["sql", "-q", "SHOW INTEGRATIONS"])
    assert result.exit_code == 0
    return any(
        integration["name"].upper() == integration_name.upper()
        for integration in result.json
    )
