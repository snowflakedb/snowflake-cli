import os
import pytest
from tempfile import NamedTemporaryFile
from textwrap import dedent
from unittest import mock
from unittest.mock import call

from tests.testing_utils.fixtures import *

STREAMLIT_NAME = "test_streamlit"


@mock.patch("snowflake.connector.connect")
def test_create_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    with NamedTemporaryFile(suffix=".py") as file:
        result = runner.invoke_with_config(
            ["streamlit", "create", STREAMLIT_NAME, "--file", file.name]
        )

        assert result.exit_code == 0, result.output
        assert ctx.get_query() == dedent(
            f"""
                create streamlit {STREAMLIT_NAME}
            
                MAIN_FILE = '{os.path.basename(file.name)}'
                QUERY_WAREHOUSE = MockWarehouse;
                
                alter streamlit {STREAMLIT_NAME} checkout;
            """
        )


@mock.patch("snowflake.connector.connect")
def test_create_streamlit_with_use_packaging_workaround(
    mock_connector, runner, mock_ctx
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    with NamedTemporaryFile(suffix=".py") as file:
        result = runner.invoke_with_config(
            [
                "streamlit",
                "create",
                STREAMLIT_NAME,
                "--file",
                file.name,
                "--use-packaging-workaround",
            ]
        )

        assert result.exit_code == 0, result.output
        assert ctx.get_query() == dedent(
            f"""
                create streamlit {STREAMLIT_NAME}
            
                MAIN_FILE = 'streamlit_app_launcher.py'
                QUERY_WAREHOUSE = MockWarehouse;
                
                alter streamlit {STREAMLIT_NAME} checkout;
            """
        )


@pytest.mark.parametrize(
    "stage, expected",
    [
        ("stage_name", "@stage_name"),
        ("snow://stage_dots", "snow://stage_dots"),
    ],
)
@mock.patch("snowflake.connector.connect")
def test_create_streamlit_with_from_stage(
    mock_connector, runner, mock_ctx, stage, expected
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    with NamedTemporaryFile(suffix=".py") as file:
        result = runner.invoke(
            [
                "streamlit",
                "create",
                STREAMLIT_NAME,
                "--file",
                file.name,
                "--from-stage",
                stage,
            ]
        )

        assert result.exit_code == 0, result.output
        assert ctx.get_query() == dedent(
            f"""
                create streamlit {STREAMLIT_NAME}
                FROM {expected}
                MAIN_FILE = '{os.path.basename(file.name)}'
                QUERY_WAREHOUSE = MockWarehouse;
                
                alter streamlit {STREAMLIT_NAME} checkout;
            """
        )


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


@mock.patch("snowcli.cli.streamlit.manager.typer")
@mock.patch("snowcli.cli.streamlit.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit(
    mock_connector, mock_stage_manager, mock_typer, mock_cursor, runner, mock_ctx
):
    ctx = mock_ctx(
        mock_cursor(
            rows=["snowflake.com"], columns=["SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME"]
        )
    )
    mock_connector.return_value = ctx

    with NamedTemporaryFile(suffix=".py") as file:
        result = runner.invoke(
            ["streamlit", "deploy", STREAMLIT_NAME, "--file", file.name, "--open"]
        )

        assert result.exit_code == 0, result.output
        assert (
            ctx.get_query()
            == f"call SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME('{STREAMLIT_NAME}')"
        )
        mock_stage_manager().put.assert_called_once_with(
            file.name,
            f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout",
            4,
            True,
        )
        mock_typer.launch.assert_called_once_with(
            f"https://account.test.region.aws.snowflakecomputing.com/test.region.aws/account/#/streamlit-apps/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}"
        )


@mock.patch("snowcli.cli.streamlit.manager.snowpark_package")
@mock.patch("snowcli.cli.streamlit.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_deploy_streamlit_with_packaging_workaround(
    mock_connector,
    mock_stage_manager,
    mock_snowpark_package,
    mock_cursor,
    runner,
    mock_ctx,
    temp_dir,
):
    ctx = mock_ctx(
        mock_cursor(
            rows=["snowflake.com"], columns=["SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME"]
        )
    )
    mock_connector.return_value = ctx

    with NamedTemporaryFile(suffix=".py") as file:
        result = runner.invoke_with_config(
            [
                "streamlit",
                "deploy",
                STREAMLIT_NAME,
                "--file",
                file.name,
                "--use-packaging-workaround",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            ctx.get_query()
            == f"call SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME('{STREAMLIT_NAME}')"
        )
        mock_snowpark_package.assert_called_once_with("ask", True, "ask")

        mock_stage_manager().put.assert_has_calls(
            [
                call(
                    "app.zip",
                    f"{STREAMLIT_NAME}_stage",
                    4,
                    True,
                ),
                call(
                    "streamlit_app_launcher.py",
                    f"{STREAMLIT_NAME}_stage",
                    4,
                    True,
                ),
                call(
                    file.name,
                    f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout",
                    4,
                    True,
                ),
            ]
        )


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
