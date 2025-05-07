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

import json
import tempfile
from typing import Dict
from unittest import mock
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.spcs.image_repository.manager import ImageRepositoryManager
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import (
    DatabaseNotProvidedError,
    SchemaNotProvidedError,
)
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

from tests.spcs.test_common import SPCS_OBJECT_EXISTS_ERROR
from tests_common import change_directory

MOCK_ROWS = [
    [
        "2023-01-01 00:00:00",
        "IMAGES",
        "DB",
        "SCHEMA",
        "orgname-alias.registry.snowflakecomputing.com/DB/SCHEMA/IMAGES",
        "ROLE",
        "ROLE",
        "",
    ]
]

MOCK_COLUMNS = [
    "created_on",
    "name",
    "database_name",
    "schema_name",
    "repository_url",
    "owner",
    "owner_role_type",
    "comment",
]
MOCK_ROWS_DICT = [
    {col_name: col_val for col_name, col_val in zip(MOCK_COLUMNS, row)}
    for row in MOCK_ROWS
]
EXECUTE_QUERY = "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager._execute_schema_query"


@pytest.mark.parametrize(
    "replace, if_not_exists, expected_query",
    [
        (False, False, "create image repository test_repo"),
        (False, True, "create image repository if not exists test_repo"),
        (True, False, "create or replace image repository test_repo"),
        # (True, True) is an invalid case as OR REPLACE and IF NOT EXISTS are mutually exclusive.
    ],
)
@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager._execute_schema_query"
)
def test_create(mock_execute, replace, if_not_exists, expected_query):
    repo_name = "test_repo"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute.return_value = cursor
    result = ImageRepositoryManager().create(
        name=repo_name, replace=replace, if_not_exists=if_not_exists
    )
    mock_execute.assert_called_once_with(expected_query, name=repo_name)
    assert result == cursor


def test_create_replace_and_if_not_exist():
    with pytest.raises(ValueError) as e:
        ImageRepositoryManager().create(
            name="test_repo", replace=True, if_not_exists=True
        )
    assert "mutually exclusive" in str(e.value)


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager.create"
)
def test_create_cli(mock_create, mock_cursor, runner, os_agnostic_snapshot):
    repo_name = "test_repo"
    cursor = mock_cursor(
        rows=[[f"Image Repository {repo_name.upper()} successfully created."]],
        columns=["status"],
    )
    mock_create.return_value = cursor
    command = ["spcs", "image-repository", "create", repo_name]
    result = runner.invoke(command)
    mock_create.assert_called_once_with(
        name=repo_name, replace=False, if_not_exists=False
    )
    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


def test_create_cli_replace_and_if_not_exists_fails(runner, os_agnostic_snapshot):
    command = [
        "spcs",
        "image-repository",
        "create",
        "test_repo",
        "--replace",
        "--if-not-exists",
    ]
    result = runner.invoke(command)
    assert result.exit_code == 2
    assert result.output == os_agnostic_snapshot


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager._execute_schema_query"
)
@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.handle_object_already_exists"
)
def test_create_repository_already_exists(mock_handle, mock_execute):
    repo_name = "test_object"
    mock_execute.side_effect = SPCS_OBJECT_EXISTS_ERROR
    ImageRepositoryManager().create(repo_name, replace=False, if_not_exists=False)
    mock_handle.assert_called_once_with(
        SPCS_OBJECT_EXISTS_ERROR,
        ObjectType.IMAGE_REPOSITORY,
        repo_name,
        replace_available=True,
    )


def test_deploy_command_requires_pdf(runner):
    with tempfile.TemporaryDirectory() as tmpdir:
        with change_directory(tmpdir):
            result = runner.invoke(["spcs", "image-repository", "deploy"])
            assert result.exit_code == 1
            assert "Cannot find project definition (snowflake.yml)." in result.output


@patch(EXECUTE_QUERY)
def test_deploy_from_project_definition(
    mock_execute_query, runner, project_directory, mock_cursor, os_agnostic_snapshot
):
    mock_execute_query.return_value = mock_cursor(
        rows=[["Image Repository TEST_IMAGE_REPOSITORY successfully created."]],
        columns=["status"],
    )

    with project_directory("spcs_image_repository"):
        result = runner.invoke(["spcs", "image-repository", "deploy"])

        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_called_once_with(
            "create image repository test_image_repository",
            name="test_image_repository",
        )


@patch(EXECUTE_QUERY)
def test_deploy_from_project_definition_replace(
    mock_execute_query, runner, project_directory, mock_cursor, os_agnostic_snapshot
):
    image_repository_name = "test_image_repository"
    mock_execute_query.return_value = mock_cursor(
        rows=[
            [f"Image Repository {image_repository_name.upper()} successfully created."]
        ],
        columns=["status"],
    )

    with project_directory("spcs_image_repository"):
        result = runner.invoke(["spcs", "image-repository", "deploy", "--replace"])

        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_called_once_with(
            "create or replace image repository test_image_repository",
            name="test_image_repository",
        )


@patch(EXECUTE_QUERY)
def test_deploy_from_project_definition_image_repository_already_exists(
    mock_execute_query, runner, project_directory
):
    mock_execute_query.side_effect = ProgrammingError(
        errno=2002, msg="Object 'test_image_repository' already exists."
    )

    with project_directory("spcs_image_repository"):
        result = runner.invoke(["spcs", "image-repository", "deploy"])

        assert result.exit_code == 1, result.output
        assert (
            "Image-repository TEST_IMAGE_REPOSITORY already exists. Use --replace flag"
            in result.output
        )


def test_deploy_from_project_definition_no_image_repository(runner, project_directory):
    with project_directory("empty_project"):
        result = runner.invoke(["spcs", "image-repository", "deploy"])

        assert result.exit_code == 1, result.output
        assert "No image repository project definition found in" in result.output


def test_deploy_from_project_definition_not_existing_entity_id(
    runner, project_directory
):
    with project_directory("spcs_image_repository"):
        result = runner.invoke(
            ["spcs", "image-repository", "deploy", "not_existing_entity_id"]
        )

        assert result.exit_code == 2, result.output
        assert (
            "No 'not_existing_entity_id' entity in project definition file."
            in result.output
        )


@patch(EXECUTE_QUERY)
def test_deploy_from_project_definition_multiple_image_repositories_with_entity_id(
    mock_execute_query, runner, project_directory, mock_cursor, os_agnostic_snapshot
):
    mock_execute_query.return_value = mock_cursor(
        rows=[["Image Repository TEST_IMAGE_REPOSITORY successfully created."]],
        columns=["status"],
    )

    with project_directory("spcs_multiple_image_repositories"):
        result = runner.invoke(
            ["spcs", "image-repository", "deploy", "test_image_repository"]
        )

        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_called_once_with(
            "create image repository test_image_repository",
            name="test_image_repository",
        )


def test_deploy_from_project_definition_multiple_image_repositories(
    runner, project_directory
):
    with project_directory("spcs_multiple_image_repositories"):
        result = runner.invoke(["spcs", "image-repository", "deploy"])

        assert result.exit_code == 2, result.output
        assert (
            "Multiple image repositories found. Please provide entity id"
            in result.output
        )


@patch(
    "snowflake.cli._plugins.spcs.image_repository.commands.ImageRepositoryManager.list_images"
)
def test_list_images_cli(
    mock_list_images,
    runner,
    mock_cursor,
):
    cursor = mock_cursor(
        rows=[
            [
                "2024-10-11 14:23:49-07:00",
                "echo_service",
                "latest",
                "sha256:a8a001fef406fdb3125ce8e8bf9970c35af7084",
                "/db/schema/repo/echo_service",
            ]
        ],
        columns=["created_on", "image_name", "tags", "digest", "image_path"],
    )
    mock_list_images.return_value = cursor

    result = runner.invoke(
        ["spcs", "image-repository", "list-images", "IMAGES", "--format", "JSON"]
    )

    assert result.exit_code == 0, result.output
    assert "/db/schema/repo/echo_service" in result.output


@patch(
    "snowflake.cli._plugins.spcs.image_repository.commands.ImageRepositoryManager.list_images"
)
def test_list_images_cli_with_like(
    mock_list_images,
    runner,
    mock_cursor,
):
    cursor = mock_cursor(
        rows=[
            [
                "2024-10-11 14:23:49-07:00",
                "echo_service",
                "latest",
                "sha256:a8a001fef406fdb3125ce8e8bf9970c35af7084",
                "/db/schema/repo/echo_service",
            ]
        ],
        columns=["created_on", "image_name", "tags", "digest", "image_path"],
    )
    mock_list_images.return_value = cursor

    result = runner.invoke(
        [
            "spcs",
            "image-repository",
            "list-images",
            "IMAGES",
            "--format",
            "JSON",
            "--like",
            "%echo_service%",
        ]
    )

    assert result.exit_code == 0, result.output
    assert "/db/schema/repo/echo_service" in result.output


@patch(
    "snowflake.cli._plugins.spcs.image_repository.commands.ImageRepositoryManager.execute_query"
)
def test_list_images(mock_execute_query):
    repo_name = "test_repo"
    like_option = ""
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ImageRepositoryManager().list_images(repo_name, like_option)
    expected_query = f"show images in image repository test_repo"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch(
    "snowflake.cli._plugins.spcs.image_repository.commands.ImageRepositoryManager.execute_query"
)
def test_list_images_with_like(mock_execute_query):
    repo_name = "test_repo"
    like = "echo_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ImageRepositoryManager().list_images(repo_name, like)
    expected_query = f"show images like 'echo_service' in image repository test_repo"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@mock.patch("snowflake.cli._plugins.spcs.image_repository.commands.requests.get")
@mock.patch(EXECUTE_QUERY)
@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.commands.ImageRepositoryManager._conn"
)
@mock.patch(
    "snowflake.cli._plugins.spcs.image_registry.manager.RegistryManager.login_to_registry"
)
def test_list_tags(
    mock_login,
    mock_conn,
    mock_execute,
    mock_get_tags,
    runner,
    mock_cursor,
):
    mock_conn.database = "DB"
    mock_conn.schema = "SCHEMA"
    mock_conn.role = "MY_ROLE"

    mock_execute.return_value = mock_cursor(rows=MOCK_ROWS_DICT, columns=MOCK_COLUMNS)
    mock_login.return_value = "TOKEN"

    mock_get_tags.return_value.status_code = 200
    mock_get_tags.return_value.text = (
        '{"name":"baserepo/super-cool-repo","tags":["1.2.0"]}'
    )

    result = runner.invoke(
        [
            "spcs",
            "image-repository",
            "list-tags",
            "IMAGES",
            "--image-name",
            "/DB/SCHEMA/IMAGES/super-cool-repo",
            "--format",
            "JSON",
        ]
    )

    assert result.exit_code == 0, result.output
    assert (
        "DeprecationWarning: The command 'list-tags' is deprecated."
        == result.output[0 : result.output.find("\n")]
    )
    assert json.loads(result.output[result.output.find("\n") :]) == [
        {"tag": "/DB/SCHEMA/IMAGES/super-cool-repo:1.2.0"}
    ], str(result.output)


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager.get_repository_url"
)
def test_get_repository_url_cli(mock_url, runner):
    repo_url = "repotest.registry.snowflakecomputing.com/db/schema/IMAGES"
    mock_url.return_value = repo_url
    result = runner.invoke(["spcs", "image-repository", "url", "IMAGES"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == repo_url


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager.show_specific_object"
)
def test_get_repository_url(mock_get_row):
    expected_row = MOCK_ROWS_DICT[0]
    mock_get_row.return_value = expected_row
    result = ImageRepositoryManager().get_repository_url(repo_name="IMAGES")
    mock_get_row.assert_called_once_with(
        "image repositories", "IMAGES", check_schema=True
    )
    assert isinstance(expected_row, Dict)
    assert "repository_url" in expected_row
    assert result == f"https://{expected_row['repository_url']}"


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager.show_specific_object"
)
def test_get_repository_url_no_scheme(mock_get_row):
    expected_row = MOCK_ROWS_DICT[0]
    mock_get_row.return_value = expected_row
    result = ImageRepositoryManager().get_repository_url(
        repo_name="IMAGES", with_scheme=False
    )
    mock_get_row.assert_called_once_with(
        "image repositories", "IMAGES", check_schema=True
    )
    assert isinstance(expected_row, Dict)
    assert "repository_url" in expected_row
    assert result == expected_row["repository_url"]


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager._conn"
)
@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager.show_specific_object"
)
def test_get_repository_url_no_repo_found(mock_get_row, mock_conn):
    mock_get_row.return_value = None
    mock_conn.database = "DB"
    mock_conn.schema = "SCHEMA"
    with pytest.raises(ProgrammingError) as e:
        ImageRepositoryManager().get_repository_url(repo_name="IMAGES")
    assert (
        e.value.msg
        == "Image repository 'DB.SCHEMA.IMAGES' does not exist or not authorized."
    )
    mock_get_row.assert_called_once_with(
        "image repositories", "IMAGES", check_schema=True
    )


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager._conn"
)
def test_get_repository_url_no_database_provided(mock_conn):
    mock_conn.database = None
    with pytest.raises(DatabaseNotProvidedError):
        ImageRepositoryManager().get_repository_url("IMAGES")


@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager._conn"
)
def test_get_repository_url_no_schema_provided(mock_conn):
    mock_conn.database = "DB"
    mock_conn.schema = None
    with pytest.raises(SchemaNotProvidedError):
        ImageRepositoryManager().get_repository_url("IMAGES")


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize(
    "command, parameters",
    [
        ("list", []),
        ("list", ["--like", "PATTERN"]),
        ("drop", ["NAME"]),
    ],
)
def test_command_aliases(mock_connector, runner, mock_ctx, command, parameters):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", command, "image-repository", *parameters])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["spcs", "image-repository", command, *parameters], catch_exceptions=False
    )
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert queries[0] == queries[1]
