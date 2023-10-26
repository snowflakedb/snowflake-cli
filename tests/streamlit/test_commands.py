from tempfile import NamedTemporaryFile
from textwrap import dedent

from tests.testing_utils.fixtures import *

STREAMLIT_NAME = "test_streamlit"
TEST_WAREHOUSE = "test_warehouse"


@mock.patch("snowflake.connector.connect")
def test_list_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["streamlit", "list"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show streamlits"


@mock.patch("snowflake.connector.connect")
def test_describe_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["streamlit", "describe", STREAMLIT_NAME])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        f"describe streamlit {STREAMLIT_NAME}",
        f"call SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME('{STREAMLIT_NAME}')",
    ]


def _put_query(source: str, dest: str):
    return dedent(
        f"put file://{source} {dest} auto_compress=false parallel=4 overwrite=True"
    )


@mock.patch("snowcli.cli.connection.util.get_account")
@mock.patch("snowcli.cli.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_single_file(
    mock_connector, mock_typer, mock_get_account, mock_cursor, runner, mock_ctx
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with NamedTemporaryFile(suffix=".py") as file:
        result = runner.invoke(
            [
                "streamlit",
                "deploy",
                STREAMLIT_NAME,
                "--file",
                file.name,
                "--open",
                "--query-warehouse",
                TEST_WAREHOUSE,
            ]
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query(file.name, "@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/test_streamlit"),
        dedent(
            f"""
    CREATE  STREAMLIT {STREAMLIT_NAME}
    ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
    MAIN_FILE = '{Path(file.name).name}'
    QUERY_WAREHOUSE = test_warehouse
    """
        ),
    ]

    mock_typer.launch.assert_called_once_with(
        f"https://app.snowflake.com/test.region.aws/my_account/#/streamlit-apps/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}"
    )


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_all_files_default_stage(
    mock_connector, mock_cursor, runner, mock_ctx, project_file
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_file("example_streamlit") as pdir:
        result = runner.invoke(
            ["streamlit", "deploy", STREAMLIT_NAME, "--query-warehouse", TEST_WAREHOUSE]
        )

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query("app.py", root_path),
        _put_query("environment.yml", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
    CREATE  STREAMLIT {STREAMLIT_NAME}
    ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = test_warehouse
    """
        ),
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_all_files_users_stage(
    mock_connector, mock_cursor, runner, mock_ctx, project_file
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_file("example_streamlit") as pdir:
        result = runner.invoke(
            [
                "streamlit",
                "deploy",
                STREAMLIT_NAME,
                "--file",
                "main.py",
                "--stage",
                "MY_FANCY_STAGE",
                "--query-warehouse",
                TEST_WAREHOUSE,
            ]
        )

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.MY_FANCY_STAGE/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.MY_FANCY_STAGE",
        _put_query("main.py", root_path),
        _put_query("environment.yml", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
    CREATE  STREAMLIT {STREAMLIT_NAME}
    ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.MY_FANCY_STAGE/{STREAMLIT_NAME}'
    MAIN_FILE = 'main.py'
    QUERY_WAREHOUSE = test_warehouse
    """
        ),
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_main_and_environment_files(
    mock_connector, mock_cursor, runner, mock_ctx, project_file
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_file("example_streamlit") as pdir:
        (pdir / "pages" / "my_page.py").unlink()
        (pdir / "pages").rmdir()

        result = runner.invoke(
            [
                "streamlit",
                "deploy",
                STREAMLIT_NAME,
                "--file",
                "main.py",
                "--query-warehouse",
                TEST_WAREHOUSE,
            ]
        )

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query("main.py", root_path),
        _put_query("environment.yml", root_path),
        dedent(
            f"""
    CREATE  STREAMLIT {STREAMLIT_NAME}
    ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
    MAIN_FILE = 'main.py'
    QUERY_WAREHOUSE = test_warehouse
    """
        ),
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_main_and_pages_files(
    mock_connector, mock_cursor, runner, mock_ctx, project_file
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_file("example_streamlit") as pdir:
        (pdir / "environment.yml").unlink()
        result = runner.invoke(
            [
                "streamlit",
                "deploy",
                STREAMLIT_NAME,
                "--file",
                "main.py",
                "--query-warehouse",
                TEST_WAREHOUSE,
            ]
        )

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query("main.py", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
    CREATE  STREAMLIT {STREAMLIT_NAME}
    ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
    MAIN_FILE = 'main.py'
    QUERY_WAREHOUSE = test_warehouse
    """
        ),
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_main_and_pages_files_experimental(
    mock_connector, mock_cursor, runner, mock_ctx, project_file
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_file("example_streamlit"):
        result = runner.invoke(
            [
                "streamlit",
                "deploy",
                STREAMLIT_NAME,
                "--query-warehouse",
                "test_warehouse",
                "--experimental",
            ]
        )

    root_path = (
        f"snow://streamlit/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}/"
        "default_checkout"
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        dedent(
            f"""
    CREATE  STREAMLIT {STREAMLIT_NAME}

    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = test_warehouse
    """
        ),
        "ALTER streamlit test_streamlit CHECKOUT",
        _put_query("app.py", root_path),
        _put_query("environment.yml", f"{root_path}"),
        _put_query("pages/*.py", f"{root_path}/pages"),
    ]


@pytest.mark.parametrize(
    "opts",
    [
        ("--pages-dir", "foo/bar"),
        ("--env-file", "foo.yml"),
        ("--env-file", "environment.yml"),
        ("--env-file", "pages"),
    ],
)
def test_deploy_streamlit_nonexisting_file(runner, opts):
    with NamedTemporaryFile(suffix=".py") as file:
        result = runner.invoke(
            [
                "streamlit",
                "deploy",
                STREAMLIT_NAME,
                "--file",
                file.name,
                "--query-warehouse",
                TEST_WAREHOUSE,
                *opts,
            ]
        )

        assert f"Provided file {opts[1]} does not exist" in result.output


@mock.patch("snowflake.connector.connect")
def test_share_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    role = "other_role"

    result = runner.invoke(["streamlit", "share", STREAMLIT_NAME, role])

    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query() == f"grant usage on streamlit {STREAMLIT_NAME} to role {role}"
    )


@mock.patch("snowflake.connector.connect")
def test_drop_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["streamlit", "drop", STREAMLIT_NAME])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == f"drop streamlit {STREAMLIT_NAME}"
