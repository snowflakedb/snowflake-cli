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

import shutil
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli.plugins.connection.util import REGIONLESS_QUERY

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
        f"put file://{Path(source)} {dest} auto_compress=false parallel=4 overwrite=True"
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
        "create stage if not exists MockDatabase.MockSchema.streamlit",
        _put_query(
            "streamlit_app.py", "@MockDatabase.MockSchema.streamlit/test_streamlit"
        ),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
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
        "create stage if not exists MockDatabase.MockSchema.streamlit",
        _put_query(
            "streamlit_app.py", "@MockDatabase.MockSchema.streamlit/test_streamlit"
        ),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
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
        "create stage if not exists MockDatabase.MockSchema.streamlit",
        _put_query(
            "streamlit_app.py", "@MockDatabase.MockSchema.streamlit/test_streamlit"
        ),
        dedent(
            f"""
            CREATE OR REPLACE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
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

    root_path = f"@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MockDatabase.MockSchema.streamlit",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", root_path),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
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

    root_path = f"@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MockDatabase.MockSchema.streamlit",
        _put_query("streamlit_app.py", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
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

    root_path = f"@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MockDatabase.MockSchema.streamlit",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        _put_query("utils/utils.py", f"{root_path}/utils"),
        _put_query("extra_file.py", root_path),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
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

    root_path = f"@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MockDatabase.MockSchema.streamlit_stage",
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", root_path),
        _put_query("pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
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

    root_path = f"@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists MockDatabase.MockSchema.streamlit_stage",
        _put_query("main.py", root_path),
        _put_query("streamlit_environment.yml", root_path),
        _put_query("streamlit_pages/*.py", f"{root_path}/pages"),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}'
            MAIN_FILE = 'main.py'
            QUERY_WAREHOUSE = streamlit_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        f"select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
@pytest.mark.parametrize("enable_streamlit_no_checkouts", [True, False])
def test_deploy_streamlit_main_and_pages_files_experimental(
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
    enable_streamlit_versioned_stage,
    enable_streamlit_no_checkouts,
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

    with mock.patch(
        "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
        return_value=enable_streamlit_versioned_stage,
    ), mock.patch(
        "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled",
        return_value=enable_streamlit_no_checkouts,
    ):
        with project_directory("example_streamlit"):
            result = runner.invoke(["streamlit", "deploy", "--experimental"])

    if enable_streamlit_versioned_stage:
        root_path = (
            f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/versions/live"
        )
        post_create_command = f"ALTER STREAMLIT MockDatabase.MockSchema.{STREAMLIT_NAME} ADD LIVE VERSION FROM LAST"
    else:
        root_path = f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
        if enable_streamlit_no_checkouts:
            post_create_command = None
        else:
            post_create_command = (
                f"ALTER streamlit MockDatabase.MockSchema.{STREAMLIT_NAME} CHECKOUT"
            )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        cmd
        for cmd in [
            dedent(
                f"""
            CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
            """
            ).strip(),
            post_create_command,
            _put_query("streamlit_app.py", root_path),
            _put_query("environment.yml", f"{root_path}"),
            _put_query("pages/*.py", f"{root_path}/pages"),
            "select system$get_snowsight_host()",
            REGIONLESS_QUERY,
            "select current_account_name()",
        ]
        if cmd is not None
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
        f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
    )

    # Same as normal, except no ALTER query
    assert ctx.get_queries() == [
        dedent(
            f"""
            CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
            """
        ).strip(),
        _put_query("streamlit_app.py", root_path),
        _put_query("environment.yml", f"{root_path}"),
        _put_query("pages/*.py", f"{root_path}/pages"),
        "select system$get_snowsight_host()",
        REGIONLESS_QUERY,
        "select current_account_name()",
    ]


@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
def test_deploy_streamlit_main_and_pages_files_experimental_no_stage(
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
    enable_streamlit_versioned_stage,
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

    with mock.patch(
        "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
        return_value=enable_streamlit_versioned_stage,
    ):
        with project_directory("example_streamlit_no_stage"):
            result = runner.invoke(["streamlit", "deploy", "--experimental"])

    if enable_streamlit_versioned_stage:
        root_path = (
            f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/versions/live"
        )
        post_create_command = f"ALTER STREAMLIT MockDatabase.MockSchema.{STREAMLIT_NAME} ADD LIVE VERSION FROM LAST"
    else:
        root_path = f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
        post_create_command = (
            f"ALTER streamlit MockDatabase.MockSchema.{STREAMLIT_NAME} CHECKOUT"
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        dedent(
            f"""
            CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        post_create_command,
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
        f"snow://streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        dedent(
            f"""
            CREATE OR REPLACE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            TITLE = 'My Fancy Streamlit'
            """
        ).strip(),
        f"ALTER streamlit MockDatabase.MockSchema.{STREAMLIT_NAME} CHECKOUT",
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

        assert f"Provided file {opts[1]} does not exist" in result.output.replace(
            "\\", "/"
        )


@mock.patch("snowflake.connector.connect")
def test_share_streamlit(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    role = "other_role"

    result = runner.invoke(["streamlit", "share", STREAMLIT_NAME, role])

    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query()
        == f"grant usage on streamlit IDENTIFIER('{STREAMLIT_NAME}') to role {role}"
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

    result = runner.invoke(["object", command, "streamlit", *parameters])
    assert result.exit_code == 0, result.output
    result = runner.invoke(["streamlit", command, *parameters], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert queries[0] == queries[1]
