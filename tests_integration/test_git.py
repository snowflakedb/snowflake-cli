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
import time

import pytest
from snowflake.connector.errors import ProgrammingError
from pathlib import Path
import tempfile

from tests_common import skip_snowpark_on_newest_python
from tests_integration.test_utils import contains_row_with

FILE_IN_REPO = "README.md"


def _sf_git_repository(runner, origin_url: str) -> str:
    repo_name = "SNOWCLI_TESTING_REPO"
    integration_name = "SNOWCLI_TESTING_REPO_API_INTEGRATION"
    communication = "\n".join([origin_url, "n", integration_name, ""])
    result = runner.invoke_with_connection(
        ["git", "setup", repo_name], input=communication
    )
    assert result.exit_code == 0
    assert f"Git Repository {repo_name} was successfully created." in result.output
    return repo_name


@pytest.fixture
def sf_git_repository(runner, test_database):
    # small repository, used for most tests
    yield _sf_git_repository(
        runner, "https://github.com/snowflakedb/homebrew-snowflake-cli.git"
    )


@pytest.fixture
def sf_git_this_repository(runner, test_database):
    # this repository, used for execute tests
    yield _sf_git_repository(runner, "https://github.com/snowflakedb/snowflake-cli.git")


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
    assert result.exit_code == 0, result.output
    assert result.json == [
        {
            "status": "Git Repository SNOWCLI_TESTING_REPO is up to date. No change was fetched."
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
    result = runner.invoke_with_connection_json(
        ["git", "list-tags", sf_git_repository, "--like", "v3.6.0%"]
    )
    assert result.exit_code == 0
    assert result.json == [
        {
            "author": None,
            "commit_hash": "164f1f50b08faa74fd4e042f2766f0545adff5f0",
            "message": None,
            "name": "v3.6.0",
            "path": "/tags/v3.6.0",
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

    repository_path = f"@{sf_git_repository}/tags/no-such-tag/"
    result = runner.invoke_with_connection(["git", "list-files", repository_path])
    assert result.exit_code == 1
    assert "'no-such-tag' cannot be found" in result.output

    # list files with pattern
    repository_path = f"@{sf_git_repository.upper()}/tags/v3.6.0/"
    prefix = repository_path[1:-1].lower()
    result = runner.invoke_with_connection_json(
        ["git", "list-files", repository_path, "--pattern", "update.*"]
    )
    assert result.exit_code == 0
    assert _filter_key(result.json, key="name") == [
        f"{prefix}/update.py",
        f"{prefix}/update.sh",
    ]


@pytest.mark.integration
def test_copy_to_stage(runner, sf_git_repository):
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v3.6.0"
    SUBDIR = "Casks"
    FILE_IN_SUBDIR = "snowflake-cli.rb"
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
    assert result.exit_code == 0, result.output
    _assert_file_on_stage(f"{SUBDIR}/{FILE_IN_SUBDIR}")  # whole dir is copied

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
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v3.6.0"
    SUBDIR = "Casks"
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
            return set(str(x.relative_to(root)) for x in root.rglob("*"))

        downloaded_files = _relative_content_set(LOCAL_DIR)
        # contents are copied
        assert downloaded_files == {
            "snowflake-cli.rb",
            "snowflake-cli.tmpl.rb",
            "snowcli.rb",
            "snowcli.tmpl.rb",
        }


@pytest.mark.integration
def test_copy_single_file_to_local_file_system(runner, sf_git_repository):
    REPO_PATH_PREFIX = f"@{sf_git_repository}/tags/v3.6.0"
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

        repository_path = f"@{sf_git_repository}/tags/no-such-tag/"
        result = runner.invoke_with_connection(
            ["git", "copy", repository_path, str(LOCAL_DIR)]
        )
        assert result.exit_code == 1
        assert "'no-such-tag' cannot be found" in result.output


@pytest.mark.integration
def test_execute_with_name_in_pascal_case(
    runner, test_database, sf_git_this_repository, snapshot
):
    result = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{sf_git_this_repository}/branches/main/tests_integration/test_data/projects/stage_execute/ScriptInPascalCase.sql",
        ]
    )

    assert result.exit_code == 0
    assert result.json == snapshot


@pytest.mark.integration
def test_execute(runner, test_database, sf_git_this_repository, snapshot):
    result = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{sf_git_this_repository}/branches/main/tests_integration/test_data/projects/stage_execute/script_template.sql",
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
@skip_snowpark_on_newest_python
def test_execute_python(runner, test_database, sf_git_this_repository, snapshot):
    result = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{sf_git_this_repository.lower()}/branches/main/tests_integration/test_data/projects/stage_execute/script1.py",
        ]
    )

    assert result.exit_code == 0
    assert result.json == snapshot


@pytest.mark.integration
@skip_snowpark_on_newest_python
def test_git_execute_python_without_requirements(
    snowflake_session,
    runner,
    test_database,
    test_root_path,
    snapshot,
    sf_git_this_repository,
):
    test_id = f"FOO{time.time_ns()}"
    result = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{sf_git_this_repository.lower()}/branches/main/tests_integration/test_data/projects/stage_execute_without_requirements",
            "-D",
            f"test_database_name={test_database}",
            "-D",
            f"TEST_ID={test_id}",
        ]
    )
    assert result.exit_code == 0
    assert result.json == snapshot

    # Assert side effect created by executed script
    *_, schemas = snowflake_session.execute_string(
        f"show schemas like '{test_id}' in database {test_database};"
    )
    assert len(list(schemas)) == 1


@pytest.mark.integration
def test_execute_fqn_repo(runner, test_database, sf_git_this_repository):
    result_fqn = runner.invoke_with_connection_json(
        [
            "git",
            "execute",
            f"@{test_database}.public.{sf_git_this_repository}/branches/main/tests_integration/test_data/projects/stage_execute/script_template.sql",
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
            "File": f"@{test_database}.public.{sf_git_this_repository}/branches/main/tests_integration/test_data/projects/stage_execute/script_template.sql",
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
