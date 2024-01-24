from textwrap import dedent

from snowflake.cli.plugins.connection.util import REGIONLESS_QUERY

from tests.testing_utils.fixtures import *

STREAMLIT_NAME = "test_streamlit"
TEST_WAREHOUSE = "test_warehouse"


@mock.patch("snowflake.connector.connect")
def test_list_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", "list", "streamlit"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show streamlits like '%%'"


@mock.patch("snowflake.connector.connect")
def test_describe_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["object", "describe", "streamlit", STREAMLIT_NAME])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        f"describe streamlit {STREAMLIT_NAME}",
    ]


def _put_query(source: str, dest: str):
    return dedent(
        f"put file://{source} {dest} auto_compress=false parallel=4 overwrite=True"
    )


@mock.patch("snowflake.cli.plugins.connection.util.get_account")
@mock.patch("snowflake.cli.plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
def test_deploy_only_streamlit_file(
    mock_connector,
    mock_typer,
    mock_get_account,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with project_directory("example_streamlit") as pdir:
        (pdir / "environment.yml").unlink()
        shutil.rmtree(pdir / "pages")
        result = runner.invoke(["streamlit", "deploy"])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query(
            "streamlit_app.py", "@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/test_streamlit"
        ),
        dedent(
            f"""
            CREATE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        "select system$get_snowsight_host()",
        REGIONLESS_QUERY,
    ]
    mock_typer.launch.assert_not_called()


@mock.patch("snowflake.cli.plugins.connection.util.get_account")
@mock.patch("snowflake.cli.plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
def test_deploy_only_streamlit_file_no_stage(
    mock_connector,
    mock_typer,
    mock_get_account,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with project_directory("example_streamlit_no_stage") as pdir:
        (pdir / "environment.yml").unlink()
        shutil.rmtree(pdir / "pages")
        result = runner.invoke(["streamlit", "deploy"])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query(
            "streamlit_app.py", "@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/test_streamlit"
        ),
        dedent(
            f"""
            CREATE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        "select system$get_snowsight_host()",
        REGIONLESS_QUERY,
    ]
    mock_typer.launch.assert_not_called()


@mock.patch("snowflake.cli.plugins.connection.util.get_account")
@mock.patch("snowflake.cli.plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
def test_deploy_only_streamlit_file_replace(
    mock_connector,
    mock_typer,
    mock_get_account,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with project_directory("example_streamlit") as pdir:
        (pdir / "environment.yml").unlink()
        shutil.rmtree(pdir / "pages")
        result = runner.invoke(["streamlit", "deploy", "--replace"])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query(
            "streamlit_app.py", "@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/test_streamlit"
        ),
        dedent(
            f"""
            CREATE OR REPLACE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        "select system$get_snowsight_host()",
        REGIONLESS_QUERY,
    ]
    mock_typer.launch.assert_not_called()


@mock.patch("snowflake.cli.plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
def test_deploy_launch_browser(
    mock_connector, mock_typer, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit"):
        result = runner.invoke(["streamlit", "deploy", "--open"])

    assert result.exit_code == 0, result.output

    mock_typer.launch.assert_called_once_with(
        f"https://snowsight.domain/test.region.aws/account/#/streamlit-apps/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}"
    )


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_and_environment_files(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit") as pdir:
        shutil.rmtree(pdir / "pages")

        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", root_path),
        dedent(
            f"""
            CREATE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_and_pages_files(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit") as pdir:
        (pdir / "environment.yml").unlink()
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query("streamlit_app.py", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
            CREATE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_all_streamlit_files(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("streamlit_full_definition"):
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        _put_query("utils/utils.py", f"{root_path}/utils"),
        _put_query("extra_file.py", root_path),
        dedent(
            f"""
            CREATE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        "select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_put_files_on_stage(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory(
        "example_streamlit",
        merge_project_definition={"streamlit": {"stage": "streamlit_stage"}},
    ):
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT_STAGE/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT_STAGE",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
            CREATE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT_STAGE/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_all_streamlit_files_not_defaults(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit_no_defaults"):
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MOCKDATABASE.MOCKSCHEMA.STREAMLIT_STAGE/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MOCKDATABASE.MOCKSCHEMA.STREAMLIT_STAGE",
        _put_query("main.py", root_path),
        _put_query("streamlit_environment.yml", root_path),
        _put_query("streamlit_pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
            CREATE STREAMLIT {STREAMLIT_NAME}
            ROOT_LOCATION = '@MOCKDATABASE.MOCKSCHEMA.STREAMLIT_STAGE/{STREAMLIT_NAME}'
            MAIN_FILE = 'main.py'
            QUERY_WAREHOUSE = streamlit_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_main_and_pages_files_experimental(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit"):
        result = runner.invoke(["streamlit", "deploy", "--experimental"])

    root_path = (
        f"snow://streamlit/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}/"
        "default_checkout"
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        dedent(
            f"""
            CREATE STREAMLIT IF NOT EXISTS {STREAMLIT_NAME}
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        "ALTER streamlit test_streamlit CHECKOUT",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", f"{root_path}"),
        _put_query("pages/*.py", f"{root_path}/pages"),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_main_and_pages_files_experimental_double_deploy(
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit"):
        result1 = runner.invoke(["streamlit", "deploy", "--experimental"])

    assert result1.exit_code == 0, result1.output

    # Reset to a fresh cursor, and clear the list of queries,
    # keeping the same connection context
    ctx.cs = mock_cursor(
        rows=[
            {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
            {"REGIONLESS": "false"},
            {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
        ],
        columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
    )
    ctx.queries = []

    with project_directory("example_streamlit"):
        result2 = runner.invoke(["streamlit", "deploy", "--experimental"])

    assert result2.exit_code == 0, result2.output

    root_path = (
        f"snow://streamlit/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}/"
        "default_checkout"
    )

    # Same as normal, except no CHECKOUT query
    assert ctx.get_queries() == [
        dedent(
            f"""
            CREATE STREAMLIT IF NOT EXISTS {STREAMLIT_NAME}
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", f"{root_path}"),
        _put_query("pages/*.py", f"{root_path}/pages"),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_main_and_pages_files_experimental_no_stage(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit_no_stage"):
        result = runner.invoke(["streamlit", "deploy", "--experimental"])

    root_path = (
        f"snow://streamlit/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}/"
        "default_checkout"
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        dedent(
            f"""
            CREATE STREAMLIT IF NOT EXISTS {STREAMLIT_NAME}
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        "ALTER streamlit test_streamlit CHECKOUT",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", f"{root_path}"),
        _put_query("pages/*.py", f"{root_path}/pages"),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_main_and_pages_files_experimental_replace(
    mock_connector, mock_cursor, runner, mock_ctx, project_directory
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit"):
        result = runner.invoke(["streamlit", "deploy", "--experimental", "--replace"])

    root_path = (
        f"snow://streamlit/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}/"
        "default_checkout"
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        dedent(
            f"""
            CREATE OR REPLACE STREAMLIT {STREAMLIT_NAME}
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        "ALTER streamlit test_streamlit CHECKOUT",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", f"{root_path}"),
        _put_query("pages/*.py", f"{root_path}/pages"),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@pytest.mark.parametrize(
    "opts",
    [
        ("pages_dir", "foo/bar"),
        ("env_file", "foo.yml"),
    ],
)
@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_nonexisting_file(
    mock_connector, runner, mock_ctx, project_directory, opts
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    with project_directory(
        "example_streamlit", merge_project_definition={"streamlit": {opts[0]: opts[1]}}
    ):
        result = runner.invoke(["streamlit", "deploy"])

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

    result = runner.invoke(["object", "drop", "streamlit", STREAMLIT_NAME])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == f"drop streamlit {STREAMLIT_NAME}"


@mock.patch(
    "snowflake.cli.api.commands.project_initialisation._create_project_template"
)
def test_init_streamlit(mock_create_project_template, runner, temp_dir):
    runner.invoke(["streamlit", "init", "my_project3"])
    mock_create_project_template.assert_called_once_with(
        "default_streamlit", project_directory="my_project3"
    )


@mock.patch("snowflake.connector.connect")
def test_get_streamlit_url(mock_connector, mock_cursor, runner, mock_ctx):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"REGIONLESS": "false"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    result = runner.invoke(["streamlit", "get-url", STREAMLIT_NAME])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        "select current_account_name()",
    ]
