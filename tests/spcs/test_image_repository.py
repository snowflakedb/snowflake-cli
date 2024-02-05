from tests.testing_utils.fixtures import *
import json


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


@mock.patch("snowflake.cli.plugins.spcs.image_repository.commands.requests.get")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_repository.commands.ImageRepositoryManager._execute_query"
)
@mock.patch(
    "snowflake.cli.plugins.spcs.image_repository.commands.ImageRepositoryManager._conn"
)
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.commands.RegistryManager.login_to_registry"
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


@mock.patch("snowflake.cli.plugins.spcs.image_repository.commands.requests.get")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_repository.manager.ImageRepositoryManager._execute_query"
)
@mock.patch(
    "snowflake.cli.plugins.spcs.image_repository.commands.ImageRepositoryManager._conn"
)
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.login_to_registry"
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
            "--image_name",
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
    "snowflake.cli.plugins.spcs.image_repository.commands.ImageRepositoryManager._execute_query"
)
@mock.patch(
    "snowflake.cli.plugins.spcs.image_repository.commands.ImageRepositoryManager._conn"
)
def test_get_repository_url_cli(mock_conn, mock_execute_query, runner, mock_cursor):
    mock_execute_query.return_value = mock_cursor(
        rows=MOCK_ROWS_DICT,
        columns=MOCK_COLUMNS,
    )

    result = runner.invoke(["spcs", "image-repository", "url", "IMAGES"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == MOCK_ROWS_DICT[0]["repository_url"]
