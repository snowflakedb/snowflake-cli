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
from typing import Dict
from unittest import mock
from unittest.mock import Mock

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


@mock.patch("snowflake.cli._plugins.spcs.image_repository.commands.requests.get")
@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.commands.ImageRepositoryManager._execute_query"
)
@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.commands.ImageRepositoryManager._conn"
)
@mock.patch(
    "snowflake.cli._plugins.spcs.image_registry.commands.RegistryManager.login_to_registry"
)
def test_list_images(
    mock_login,
    mock_conn,
    mock_execute,
    mock_get_images,
    runner,
    mock_cursor,
):
    mock_conn.database = "DB"
    mock_conn.schema = "SCHEMA"
    mock_conn.role = "MY_ROLE"

    mock_execute.return_value = mock_cursor(
        rows=MOCK_ROWS_DICT,
        columns=MOCK_COLUMNS,
    )
    mock_login.return_value = "TOKEN"

    mock_get_images.return_value.status_code = 200
    mock_get_images.return_value.text = '{"repositories":["baserepo/super-cool-repo"]}'

    result = runner.invoke(
        ["spcs", "image-repository", "list-images", "IMAGES", "--format", "JSON"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [{"image": "/DB/SCHEMA/IMAGES/super-cool-repo"}]


@mock.patch("snowflake.cli._plugins.spcs.image_repository.commands.requests.get")
@mock.patch(
    "snowflake.cli._plugins.spcs.image_repository.manager.ImageRepositoryManager._execute_query"
)
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
    assert json.loads(result.output) == [
        {"tag": "/DB/SCHEMA/IMAGES/super-cool-repo:1.2.0"}
    ]


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
