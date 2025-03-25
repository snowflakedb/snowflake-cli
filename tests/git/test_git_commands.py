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

from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector import DictCursor, ProgrammingError

EXAMPLE_URL = "https://github.com/an-example-repo.git"
STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"


def test_toplevel_help(runner):
    result = runner.invoke(["--help"])
    assert (
        result.exit_code == 0
        and "Manages git repositories in Snowflake." in result.output
    )
    result = runner.invoke(["git", "--help"])
    assert result.exit_code == 0, result.output


@mock.patch("snowflake.connector.connect")
def test_list_branches(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-branches", "repo_name"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show git branches like '%%' in repo_name"


@mock.patch("snowflake.connector.connect")
def test_list_branches_like(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-branches", "repo_name", "--like", "PATTERN"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show git branches like 'PATTERN' in repo_name"


@mock.patch("snowflake.connector.connect")
def test_list_tags(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show git tags like '%%' in repo_name"


@mock.patch("snowflake.connector.connect")
def test_list_tags_like(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name", "--like", "PATTERN"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show git tags like 'PATTERN' in repo_name"


@mock.patch("snowflake.connector.connect")
def test_list_files(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-files", "@repo_name/branches/main/"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "ls @repo_name/branches/main/"


@mock.patch("snowflake.connector.connect")
def test_list_files_pattern(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        ["git", "list-files", "@repo_name/branches/main/", "--pattern", "REGEX"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "ls @repo_name/branches/main/ pattern = 'REGEX'"


def test_list_files_not_a_stage_error(runner):
    result = runner.invoke(["git", "list-files", "repo_name/branches/main"])
    assert result.exit_code == 1
    _assert_invalid_repo_path_error_message(result.output)

    result = runner.invoke(["git", "list-files", "@repo_name/branches/main"])
    assert result.exit_code == 1
    _assert_invalid_repo_path_error_message(result.output)


@mock.patch("snowflake.connector.connect")
def test_fetch(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "fetch", "repo_name"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "alter git repository repo_name fetch"


@mock.patch("snowflake.connector.connect")
@mock.patch.object(StageManager, "iter_stage")
@mock.patch("snowflake.cli._plugins.git.commands.QueryResult")
def test_copy_to_local_file_system(
    mock_result, mock_iter, mock_connector, runner, mock_ctx, temporary_directory
):
    repo_prefix = "@repo_name/branches/main/"
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_iter.return_value = (
        StagePath.from_git_str(x)
        for x in [f"{repo_prefix}file.txt", f"{repo_prefix}dir/file_in_dir.txt"]
    )
    mock_iter.__len__.return_value = 2
    mock_result.result = {"file": "mock"}

    local_path = Path(temporary_directory) / "local_dir"
    assert not local_path.exists()
    result = runner.invoke(["git", "copy", repo_prefix, str(local_path)])

    assert result.exit_code == 0, result.output
    assert local_path.exists()
    assert (
        ctx.get_query()
        == f"""get {repo_prefix}file.txt file://{local_path.resolve()}/ parallel=4
get {repo_prefix}dir/file_in_dir.txt file://{local_path.resolve() / 'dir'}/ parallel=4"""
    )


@mock.patch("snowflake.connector.connect")
def test_copy_to_remote_dir(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        ["git", "copy", "@repo_name/branches/main/", "@stage_path/dir_in_stage"]
    )

    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query()
        == "copy files into @stage_path/dir_in_stage/ from @repo_name/branches/main/"
    )


def test_copy_not_a_stage_error(runner):
    result = runner.invoke(["git", "copy", "repo_name", "@stage_path/dir_in_stage"])
    assert result.exit_code == 1
    _assert_invalid_repo_path_error_message(result.output)

    result = runner.invoke(
        ["git", "copy", "@repo_name/tags/tag", "@stage_path/dir_in_stage"]
    )
    assert result.exit_code == 1
    _assert_invalid_repo_path_error_message(result.output)


def test_copy_to_user_stage_error(runner):
    result = runner.invoke(["git", "copy", "@repo_name/branches/main/", "@~/dir"])
    assert result.exit_code == 1
    assert (
        "Destination path cannot be a user stage. Please provide a named stage."
        in result.output
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
def test_setup_already_exists_error(mock_om_describe, mock_connector, runner, mock_ctx):
    mock_om_describe.return_value = {"object_details": "something"}
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["git", "setup", "repo_name"])
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert "Repository 'repo_name' already exists" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
def test_setup_invalid_url_error(mock_om_describe, mock_connector, runner, mock_ctx):
    mock_om_describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    communication = "http://invalid_url.git\ns"
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert "Url address should start with 'https'" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
def test_setup_no_secret_existing_api(
    mock_om_show, mock_om_describe, mock_connector, runner, mock_ctx, mock_cursor
):
    mock_om_show.return_value = mock_cursor([], [])
    mock_om_describe.side_effect = [
        # repo does not exist
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
        # api integration exists
        None,
    ]
    mock_om_describe.return_value = [None, {"object_details": "something"}]
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "n", "existing_api_integration", ""])
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: n",
                "API integration identifier (will be created if not exists) [repo_name_api_integration]: existing_api_integration",
                "Using existing API integration 'existing_api_integration'.",
            ]
        )
    )
    assert ctx.get_query() == dedent(
        """
        create git repository IDENTIFIER('repo_name')
        api_integration = existing_api_integration
        origin = 'https://github.com/an-example-repo.git'
        """
    )


@pytest.mark.parametrize(
    "repo_name, int_name, secret_name",
    [
        ("db.schema.FooRepo", "FooRepo_api_integration", "db.schema.FooRepo_secret"),
        ("schema.FooRepo", "FooRepo_api_integration", "schema.FooRepo_secret"),
        ("FooRepo", "FooRepo_api_integration", "FooRepo_secret"),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
def test_setup_no_secret_create_api(
    mock_om_show,
    mock_om_describe,
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    repo_name,
    int_name,
    secret_name,
):
    mock_om_show.return_value = mock_cursor([], [])
    mock_om_describe.side_effect = ProgrammingError(
        # nothing exists
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "n", "", ""])
    result = runner.invoke(["git", "setup", repo_name], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: n",
                f"API integration identifier (will be created if not exists) [{int_name}]: ",
                f"API integration '{int_name}' successfully created.",
            ]
        )
    )
    assert ctx.get_query() == dedent(
        f"""
        create api integration IDENTIFIER('{int_name}')
        api_provider = git_https_api
        api_allowed_prefixes = ('https://github.com/an-example-repo.git')
        allowed_authentication_secrets = ()
        enabled = true
        
        
        create git repository IDENTIFIER('{repo_name}')
        api_integration = {int_name}
        origin = 'https://github.com/an-example-repo.git'
        """
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
def test_setup_existing_secret_existing_api(
    mock_om_show, mock_om_describe, mock_connector, runner, mock_ctx, mock_cursor
):
    mock_om_show.return_value = mock_cursor([], [])
    mock_om_describe.side_effect = [
        # repo does not exist
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
        # secret exists
        None,
        # api integration exists
        None,
    ]
    mock_om_describe.return_value = [
        None,
        None,
        "integration_details",
        None,
        "secret_details",
    ]
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join(
        [EXAMPLE_URL, "y", "existing_secret", "existing_api_integration", ""]
    )
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: y",
                "Secret identifier (will be created if not exists) [repo_name_secret]: existing_secret",
                "Using existing secret 'existing_secret'",
                "API integration identifier (will be created if not exists) [repo_name_api_integration]: existing_api_integration",
                "Using existing API integration 'existing_api_integration'.",
            ]
        )
    )
    assert ctx.get_query() == dedent(
        """
        create git repository IDENTIFIER('repo_name')
        api_integration = existing_api_integration
        origin = 'https://github.com/an-example-repo.git'
        git_credentials = existing_secret
        """
    )


@pytest.mark.parametrize(
    "repo_name, int_name, existing_secret_name",
    [
        ("db.schema.FooRepo", "FooRepo_api_integration", "db.schema.existing_secret"),
        (
            "schema.FooRepo",
            "FooRepo_api_integration",
            "different_schema.existing_secret",
        ),
        ("FooRepo", "FooRepo_api_integration", "existing_secret"),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
def test_setup_existing_secret_create_api(
    mock_om_show,
    mock_om_describe,
    mock_connector,
    runner,
    mock_ctx,
    mock_cursor,
    repo_name,
    int_name,
    existing_secret_name,
):
    mock_om_show.return_value = mock_cursor([], [])
    mock_om_describe.side_effect = [
        # repo does not exists
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
        # chosen secret exists
        None,
        # chosen integration does not exist
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ]
    mock_om_describe.return_value = [None, None, "secret_details", None, None, None]
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "y", existing_secret_name, "", ""])
    result = runner.invoke(["git", "setup", repo_name], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: y",
                f"Secret identifier (will be created if not exists) [FooRepo_secret]: {existing_secret_name}",
                f"Using existing secret '{existing_secret_name}'",
                f"API integration identifier (will be created if not exists) [{int_name}]: ",
                f"API integration '{int_name}' successfully created.",
            ]
        )
    )
    assert ctx.get_query() == dedent(
        f"""
        create api integration IDENTIFIER('{int_name}')
        api_provider = git_https_api
        api_allowed_prefixes = ('https://github.com/an-example-repo.git')
        allowed_authentication_secrets = ({existing_secret_name})
        enabled = true


        create git repository IDENTIFIER('{repo_name}')
        api_integration = {int_name}
        origin = 'https://github.com/an-example-repo.git'
        git_credentials = {existing_secret_name}
        """
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
def test_setup_create_secret_create_api(
    mock_om_show, mock_om_describe, mock_connector, runner, mock_ctx, mock_cursor
):
    mock_om_show.return_value = mock_cursor([], [])
    mock_om_describe.side_effect = ProgrammingError(
        # nothing exists
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join(
        [EXAMPLE_URL, "y", "", "john_doe", "admin123", "new_integration", ""]
    )
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: y",
                "Secret identifier (will be created if not exists) [repo_name_secret]: ",
                "Secret 'repo_name_secret' will be created",
                "username: john_doe",
                "password/token: ",
                "API integration identifier (will be created if not exists) [repo_name_api_integration]: new_integration",
                "Secret 'repo_name_secret' successfully created.",
                "API integration 'new_integration' successfully created.",
            ]
        )
    )
    assert ctx.get_query() == dedent(
        """
        create secret IDENTIFIER('repo_name_secret')
        type = password
        username = 'john_doe'
        password = 'admin123'
        
        
        create api integration IDENTIFIER('new_integration')
        api_provider = git_https_api
        api_allowed_prefixes = ('https://github.com/an-example-repo.git')
        allowed_authentication_secrets = (repo_name_secret)
        enabled = true
        
        
        create git repository IDENTIFIER('repo_name')
        api_integration = new_integration
        origin = 'https://github.com/an-example-repo.git'
        git_credentials = repo_name_secret
        """
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
def test_api_integration_and_secrets_get_unique_names(
    mock_om_show, mock_om_describe, mock_connector, runner, mock_ctx, mock_cursor
):
    mock_om_show.return_value = mock_cursor(
        [{"name": f"repo_name_secret{x}"} for x in range(1, 3)]
        + [{"name": f"repo_name_api_integration{x}"} for x in range(1, 4)]
        + [{"name": "repo_name_secret"}, {"name": "repo_name_api_integration"}],
        [],
    )
    mock_om_describe.side_effect = [
        # repo does not exist
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
        # chosen secret does not exist
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
        # chosen api integration does not exist
        ProgrammingError(errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED),
    ]
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "y", "", "john_doe", "admin123", "", ""])
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: y",
                "Secret identifier (will be created if not exists) [repo_name_secret3]: ",
                "Secret 'repo_name_secret3' will be created",
                "username: john_doe",
                "password/token: ",
                "API integration identifier (will be created if not exists) [repo_name_api_integration4]: ",
                "Secret 'repo_name_secret3' successfully created.",
                "API integration 'repo_name_api_integration4' successfully created.",
            ]
        )
    )


@pytest.mark.parametrize(
    "repository_path, expected_stage, expected_files",
    [
        (
            "@repo/branches/main/",
            "@repo/branches/main",
            ["/s1.sql", "/a/S3.sql"],
        ),
        (
            "@repo/branches/main/a",
            "@repo/branches/main",
            ["/a/S3.sql"],
        ),
        (
            "@db.schema.repo/branches/main/",
            "@db.schema.repo/branches/main",
            [
                "/s1.sql",
                "/a/S3.sql",
            ],
        ),
        (
            "@db.schema.repo/branches/main/s1.sql",
            "@db.schema.repo/branches/main",
            ["/s1.sql"],
        ),
        (
            "@DB.SCHEMA.REPO/branches/main/s1.sql",
            "@DB.SCHEMA.REPO/branches/main",
            ["/s1.sql"],
        ),
        (
            "@DB.schema.REPO/branches/main/a/S3.sql",
            "@DB.schema.REPO/branches/main",
            ["/a/S3.sql"],
        ),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._conn", new=mock.MagicMock(database="FOO", schema="BAR"))
@mock.patch(f"snowflake.cli._plugins.stage.manager.time.time", lambda: 123)
def test_execute(
    mock_execute,
    mock_cursor,
    runner,
    repository_path,
    expected_stage,
    expected_files,
    os_agnostic_snapshot,
    caplog,
):
    mock_execute.return_value = mock_cursor(
        [
            {"name": "/a/S3.sql"},
            {"name": "/s1.sql"},
            {"name": "/s2"},
        ],
        [],
    )

    result = runner.invoke(["git", "execute", repository_path])

    assert result.exit_code == 0, result.output
    create_call, copy_call, ls_call, *execute_calls = mock_execute.mock_calls
    stage = "FOO.BAR.snowflake_cli_tmp_stage_123"
    assert create_call == mock.call(
        f"create temporary stage if not exists IDENTIFIER('{stage}')"
    )
    assert copy_call == mock.call(f"copy files into @{stage}/ from {expected_stage}/")
    assert ls_call == mock.call(f"ls @{stage}", cursor_class=DictCursor)
    assert execute_calls == [
        mock.call(f"execute immediate from @{stage}{p}") for p in expected_files
    ]
    assert result.output == os_agnostic_snapshot
    for expected_file in expected_files:
        assert f"Executing SQL file: @{stage}{expected_file}" in caplog.messages


@pytest.mark.parametrize(
    "repository_path, expected_stage, expected_files",
    [
        (
            "@repo/branches/main/",
            "@repo/branches/main",
            [
                "/S2.sql",
                "/s1.sql",
                "/a/s3.sql",
            ],
        ),
        (
            "@repo/branches/main/s1.sql",
            "@repo/branches/main",
            ["/s1.sql"],
        ),
        (
            "@repo/branches/main/S2.sql",
            "@repo/branches/main",
            ["/S2.sql"],
        ),
        (
            "@repo/branches/main/a/s3.sql",
            "@repo/branches/main",
            ["/a/s3.sql"],
        ),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._conn", new=mock.MagicMock(database="FOO", schema="BAR"))
@mock.patch(f"snowflake.cli._plugins.stage.manager.time.time", lambda: 123)
def test_execute_new_git_repository_list_files(
    mock_execute,
    mock_cursor,
    runner,
    repository_path,
    expected_stage,
    expected_files,
    os_agnostic_snapshot,
    caplog,
):
    mock_execute.return_value = mock_cursor(
        [
            {"name": "/s1.sql"},
            {"name": "/S2.sql"},
            {"name": "/a/s3.sql"},
        ],
        [],
    )

    result = runner.invoke(["git", "execute", repository_path])

    assert result.exit_code == 0, result.output
    create_call, copy_call, ls_call, *execute_calls = mock_execute.mock_calls
    stage = "FOO.BAR.snowflake_cli_tmp_stage_123"
    assert create_call == mock.call(
        f"create temporary stage if not exists IDENTIFIER('{stage}')"
    )
    assert copy_call == mock.call(f"copy files into @{stage}/ from {expected_stage}/")
    assert ls_call == mock.call(f"ls @{stage}", cursor_class=DictCursor)
    assert execute_calls == [
        mock.call(f"execute immediate from @{stage}{p}") for p in expected_files
    ]
    assert result.output == os_agnostic_snapshot
    for expected_file in expected_files:
        assert f"Executing SQL file: @{stage}{expected_file}" in caplog.messages


@pytest.mark.parametrize(
    "repository_path, expected_stage, expected_files",
    [
        (
            '@repo/branches/"feature/commit"/',
            '@repo/branches/"feature/commit"',
            [
                "/s1.sql",
                "/a/S3.sql",
            ],
        ),
        (
            '@repo/branches/"feature/commit"/s1.sql',
            '@repo/branches/"feature/commit"',
            [
                "/s1.sql",
            ],
        ),
        (
            '@repo/branches/"feature/commit"/a/',
            '@repo/branches/"feature/commit"',
            [
                "/a/S3.sql",
            ],
        ),
        (
            '@db.schema.repo/branches/"feature/commit"/',
            '@db.schema.repo/branches/"feature/commit"',
            [
                "/s1.sql",
                "/a/S3.sql",
            ],
        ),
    ],
)
@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._conn", new=mock.MagicMock(database="FOO", schema="BAR"))
@mock.patch(f"snowflake.cli._plugins.stage.manager.time.time", lambda: 123)
def test_execute_slash_in_repository_name(
    mock_execute,
    mock_cursor,
    runner,
    repository_path,
    expected_stage,
    expected_files,
    os_agnostic_snapshot,
):
    mock_execute.return_value = mock_cursor(
        [
            {"name": "/a/S3.sql"},
            {"name": "/s1.sql"},
            {"name": "/s2"},
        ],
        [],
    )

    result = runner.invoke(["git", "execute", repository_path])

    assert result.exit_code == 0, result.output
    create_call, copy_call, ls_call, *execute_calls = mock_execute.mock_calls
    assert create_call == mock.call(
        "create temporary stage if not exists IDENTIFIER('FOO.BAR.snowflake_cli_tmp_stage_123')"
    )
    assert copy_call == mock.call(
        f"copy files into @FOO.BAR.snowflake_cli_tmp_stage_123/ from {expected_stage}/"
    )
    assert ls_call == mock.call(
        f"ls @FOO.BAR.snowflake_cli_tmp_stage_123", cursor_class=DictCursor
    )
    assert execute_calls == [
        mock.call(f"execute immediate from @FOO.BAR.snowflake_cli_tmp_stage_123{p}")
        for p in expected_files
    ]
    assert result.output == os_agnostic_snapshot


@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._conn", new=mock.MagicMock(database="FOO", schema="BAR"))
@mock.patch(f"snowflake.cli._plugins.stage.manager.time.time", lambda: 123)
def test_execute_with_variables(mock_execute, mock_cursor, runner):
    mock_execute.return_value = mock_cursor([{"name": "/s1.sql"}], [])

    result = runner.invoke(
        [
            "git",
            "execute",
            "@repo/branches/main/",
            "-D",
            "key1='string value'",
            "-D",
            "key2=1",
            "-D",
            "key3=TRUE",
            "-D",
            "key4=NULL",
            "-D",
            "key5='var=value'",
        ]
    )

    assert result.exit_code == 0
    create_call, copy_call, ls_call, *execute_calls = mock_execute.mock_calls
    assert create_call == mock.call(
        "create temporary stage if not exists IDENTIFIER('FOO.BAR.snowflake_cli_tmp_stage_123')"
    )
    assert copy_call == mock.call(
        "copy files into @FOO.BAR.snowflake_cli_tmp_stage_123/ from @repo/branches/main/"
    )
    assert ls_call == mock.call(
        f"ls @FOO.BAR.snowflake_cli_tmp_stage_123", cursor_class=DictCursor
    )
    assert execute_calls == [
        mock.call(
            f"execute immediate from @FOO.BAR.snowflake_cli_tmp_stage_123/s1.sql using (key1=>'string value', key2=>1, key3=>TRUE, key4=>NULL, key5=>'var=value')"
        )
    ]


@mock.patch(f"{STAGE_MANAGER}.execute_query")
@mock.patch(f"{STAGE_MANAGER}._conn", new=mock.MagicMock(database="FOO", schema="BAR"))
@mock.patch(f"snowflake.cli._plugins.stage.manager.time.time", lambda: 123)
def test_execute_file_with_space_in_name(mock_execute, mock_cursor, runner):
    mock_execute.return_value = mock_cursor([{"name": "/Script 1.sql"}], [])

    result = runner.invoke(
        [
            "git",
            "execute",
            "@repo/branches/main/",
        ]
    )

    assert result.exit_code == 0
    _, __, *calls = mock_execute.mock_calls
    assert calls == [
        mock.call("ls @FOO.BAR.snowflake_cli_tmp_stage_123", cursor_class=DictCursor),
        mock.call(
            f"execute immediate from '@FOO.BAR.snowflake_cli_tmp_stage_123/Script 1.sql'"
        ),
    ]


def test_raise_error_for_invalid_quotes_number_in_path(runner):
    result = runner.invoke(
        [
            "git",
            "execute",
            '@repo/branches"/"ma"in/',
        ]
    )
    assert result.exit_code == 2, result.output
    assert (
        'Invalid string @repo/branches"/"ma"in/, too much " in path, expected 2.'
        in result.output
    )


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "command, parameters",
    [
        ("list", []),
        ("list", ["--like", "PATTERN"]),
        ("describe", ["NAME"]),
        ("drop", ["NAME"]),
    ],
)
def test_command_aliases(mock_connector, runner, mock_ctx, command, parameters):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", command, "git-repository", *parameters])
    assert result.exit_code == 0, result.output
    result = runner.invoke(["git", command, *parameters], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert queries[0] == queries[1]


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
