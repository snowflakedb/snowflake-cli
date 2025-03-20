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
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli.api.identifiers import FQN

STREAMLIT_NAME = "test_streamlit"
TEST_WAREHOUSE = "test_warehouse"
GET_UI_PARAMETERS = "snowflake.cli._plugins.connection.util.get_ui_parameters"
STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"

mock_streamlit_exists = mock.patch(
    "snowflake.cli._plugins.streamlit.manager.ObjectManager.object_exists",
    lambda _, **kwargs: False,
)


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
        f"describe streamlit IDENTIFIER('{STREAMLIT_NAME}')",
    ]


def _put_query(project_root: Path, source: str, dest: str):
    return dedent(
        f"put file://{project_root.resolve() / 'output' / 'bundle' / 'streamlit' / source} {dest} auto_compress=false parallel=4 overwrite=False"
    )


@mock.patch("snowflake.cli._plugins.connection.util.get_account")
@mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_only_streamlit_file(
    mock_list_files,
    mock_param,
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
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with project_directory("example_streamlit") as tmp_dir:
        (tmp_dir / "environment.yml").unlink()
        shutil.rmtree(tmp_dir / "pages")
        result = runner.invoke(["streamlit", "deploy"])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(
            tmp_dir,
            "streamlit_app.py",
            "@MockDatabase.MockSchema.streamlit/test_streamlit",
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
    ]
    mock_typer.launch.assert_not_called()


@mock.patch("snowflake.cli._plugins.connection.util.get_account")
@mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_only_streamlit_file_no_stage(
    mock_list_files,
    mock_param,
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
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with project_directory("example_streamlit_no_stage") as tmp_dir:
        (tmp_dir / "environment.yml").unlink()
        shutil.rmtree(tmp_dir / "pages")
        result = runner.invoke(["streamlit", "deploy"])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(
            tmp_dir,
            "streamlit_app.py",
            "@MockDatabase.MockSchema.streamlit/test_streamlit",
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
    ]
    mock_typer.launch.assert_not_called()


@mock.patch("snowflake.cli._plugins.connection.util.get_account")
@mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_with_empty_pages(
    mock_list_files,
    mock_param,
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
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with project_directory("streamlit_empty_pages") as tmp_dir:
        (tmp_dir / "pages").mkdir(parents=True, exist_ok=True)
        result = runner.invoke(["streamlit", "deploy"])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(
            tmp_dir,
            "environment.yml",
            "@MockDatabase.MockSchema.streamlit/test_streamlit",
        ),
        _put_query(
            tmp_dir,
            "streamlit_app.py",
            "@MockDatabase.MockSchema.streamlit/test_streamlit",
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
    ]


@mock.patch("snowflake.cli._plugins.connection.util.get_account")
@mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_only_streamlit_file_replace(
    mock_list_files,
    mock_param,
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
                {"CURRENT_ACCOUNT_NAME()": "my_account"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx
    mock_get_account.return_value = "my_account"

    with project_directory("example_streamlit") as tmp_dir:
        (tmp_dir / "environment.yml").unlink()
        shutil.rmtree(tmp_dir / "pages")
        result = runner.invoke(["streamlit", "deploy", "--replace"])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(
            tmp_dir,
            "streamlit_app.py",
            "@MockDatabase.MockSchema.streamlit/test_streamlit",
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
    ]
    mock_typer.launch.assert_not_called()


@pytest.mark.parametrize("project_name", ["example_streamlit_v2", "example_streamlit"])
@mock.patch("snowflake.cli._plugins.streamlit.commands.typer")
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_launch_browser(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_typer,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
    project_name,
):
    ctx = mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory(project_name):
        result = runner.invoke(["streamlit", "deploy", "--open"])

    assert result.exit_code == 0, result.output

    mock_typer.launch.assert_called_once_with(
        f"https://snowsight.domain/test.region.aws/account/#/streamlit-apps/MOCKDATABASE.MOCKSCHEMA.{STREAMLIT_NAME.upper()}"
    )


@pytest.mark.parametrize("project_name", ["example_streamlit_v2", "example_streamlit"])
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_streamlit_and_environment_files(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    alter_snowflake_yml,
    project_directory,
    project_name,
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

    with project_directory(project_name) as tmp_dir:
        shutil.rmtree(tmp_dir / "pages")
        if project_name == "example_streamlit_v2":
            alter_snowflake_yml(
                tmp_dir / "snowflake.yml",
                parameter_path="entities.test_streamlit.artifacts",
                value=["streamlit_app.py", "environment.yml"],
            )

        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(tmp_dir, "environment.yml", root_path),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
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
        f"select current_account_name()",
    ]


@pytest.mark.parametrize("project_name", ["example_streamlit_v2", "example_streamlit"])
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_streamlit_and_pages_files(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    alter_snowflake_yml,
    project_directory,
    project_name,
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

    with project_directory(project_name) as tmp_dir:
        (tmp_dir / "environment.yml").unlink()
        if project_name == "example_streamlit_v2":
            alter_snowflake_yml(
                tmp_dir / "snowflake.yml",
                parameter_path="entities.test_streamlit.artifacts",
                value=["streamlit_app.py", "pages/"],
            )
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
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
        f"select current_account_name()",
    ]


@pytest.mark.parametrize(
    "project_name", ["streamlit_full_definition_v2", "streamlit_full_definition"]
)
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_all_streamlit_files(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
    project_name,
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

    with project_directory(project_name) as tmp_dir:
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(tmp_dir, "environment.yml", root_path),
        _put_query(tmp_dir, "extra_file.py", root_path),
        _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
        _put_query(tmp_dir, "utils/utils.py", f"{root_path}/utils"),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/{STREAMLIT_NAME}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = test_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        "select current_account_name()",
    ]


@pytest.mark.parametrize(
    "project_name, merge_definition",
    [
        (
            "example_streamlit_v2",
            {
                "entities": {
                    "test_streamlit": {
                        "stage": "streamlit_stage",
                        "artifacts": ["streamlit_app.py", "environment.yml", "pages"],
                    }
                }
            },
        ),
        ("example_streamlit", {"streamlit": {"stage": "streamlit_stage"}}),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_put_files_on_stage(
    test_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    alter_snowflake_yml,
    project_directory,
    project_name,
    merge_definition,
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

    with project_directory(
        project_name,
        merge_project_definition=merge_definition,
    ) as tmp_dir:
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit_stage')",
        _put_query(tmp_dir, "environment.yml", root_path),
        _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
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
        f"select current_account_name()",
    ]


@pytest.mark.parametrize(
    "project_name",
    ["example_streamlit_no_defaults", "example_streamlit_no_defaults_v2"],
)
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_all_streamlit_files_not_defaults(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
    project_name,
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

    with project_directory(project_name) as tmp_dir:
        result = runner.invoke(["streamlit", "deploy"])

    root_path = f"@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit_stage')",
        _put_query(tmp_dir, "main.py", root_path),
        _put_query(tmp_dir, "streamlit_environment.yml", root_path),
        _put_query(
            tmp_dir, "streamlit_pages/first_page.py", f"{root_path}/streamlit_pages"
        ),
        dedent(
            f"""
            CREATE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.{STREAMLIT_NAME}')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit_stage/{STREAMLIT_NAME}'
            MAIN_FILE = 'main.py'
            QUERY_WAREHOUSE = streamlit_warehouse
            """
        ).strip(),
        f"select system$get_snowsight_host()",
        f"select current_account_name()",
    ]


@pytest.mark.parametrize("project_name", ["example_streamlit", "example_streamlit_v2"])
@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
@pytest.mark.parametrize("enable_streamlit_no_checkouts", [True, False])
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_streamlit_main_and_pages_files_experimental(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    alter_snowflake_yml,
    project_directory,
    os_agnostic_snapshot,
    enable_streamlit_versioned_stage,
    enable_streamlit_no_checkouts,
    project_name,
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

    with (
        mock.patch(
            "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
            return_value=enable_streamlit_versioned_stage,
        ),
        mock.patch(
            "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled",
            return_value=enable_streamlit_no_checkouts,
        ),
    ):
        with project_directory(project_name) as tmp_dir:
            if project_name == "example_streamlit_v2":
                alter_snowflake_yml(
                    tmp_dir / "snowflake.yml",
                    parameter_path="entities.test_streamlit.artifacts",
                    value=["streamlit_app.py", "environment.yml", "pages"],
                )
            result = runner.invoke(["streamlit", "deploy", "--experimental"])

    if enable_streamlit_versioned_stage:
        root_path = f"@streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/versions/live"
        post_create_command = f"ALTER STREAMLIT MockDatabase.MockSchema.{STREAMLIT_NAME} ADD LIVE VERSION FROM LAST"
    else:
        root_path = (
            f"@streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
        )
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
            "create stage if not exists IDENTIFIER('streamlit')",
            _put_query(tmp_dir, "environment.yml", f"{root_path}"),
            _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
            _put_query(tmp_dir, "streamlit_app.py", root_path),
            "select system$get_snowsight_host()",
            "select current_account_name()",
        ]
        if cmd is not None
    ]


@pytest.mark.parametrize("project_name", ["example_streamlit", "example_streamlit_v2"])
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_streamlit_main_and_pages_files_experimental_double_deploy(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    alter_snowflake_yml,
    project_directory,
    project_name,
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

    with project_directory(project_name) as pdir:
        if project_name == "example_streamlit_v2":
            alter_snowflake_yml(
                pdir / "snowflake.yml",
                parameter_path="entities.test_streamlit.artifacts",
                value=["streamlit_app.py", "environment.yml", "pages"],
            )
        result1 = runner.invoke(["streamlit", "deploy", "--experimental"])

    assert result1.exit_code == 0, result1.output

    # Reset to a fresh cursor, and clear the list of queries,
    # keeping the same connection context
    ctx.cs = mock_cursor(
        rows=[
            {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
            {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
        ],
        columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
    )
    ctx.queries = []

    with project_directory(project_name) as tmp_dir:
        if project_name == "example_streamlit_v2":
            alter_snowflake_yml(
                tmp_dir / "snowflake.yml",
                parameter_path="entities.test_streamlit.artifacts",
                value=["streamlit_app.py", "environment.yml", "pages"],
            )
        result2 = runner.invoke(["streamlit", "deploy", "--experimental"])

    root_path = f"@streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
    assert result2.exit_code == 0, result2.output
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
        "create stage if not exists IDENTIFIER('streamlit')",
        _put_query(tmp_dir, "environment.yml", f"{root_path}"),
        _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
        "select system$get_snowsight_host()",
        "select current_account_name()",
    ]


@pytest.mark.parametrize(
    "project_name", ["example_streamlit_no_stage", "example_streamlit_no_stage_v2"]
)
@mock.patch("snowflake.connector.connect")
@pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_streamlit_main_and_pages_files_experimental_no_stage(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    alter_snowflake_yml,
    project_directory,
    enable_streamlit_versioned_stage,
    project_name,
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

    with mock.patch(
        "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
        return_value=enable_streamlit_versioned_stage,
    ):
        with project_directory(project_name) as tmp_dir:

            result = runner.invoke(["streamlit", "deploy", "--experimental"])

    if enable_streamlit_versioned_stage:
        root_path = f"@streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/versions/live"
        post_create_command = f"ALTER STREAMLIT MockDatabase.MockSchema.{STREAMLIT_NAME} ADD LIVE VERSION FROM LAST"
    else:
        root_path = (
            f"@streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
        )
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
        "create stage if not exists IDENTIFIER('streamlit')",
        _put_query(tmp_dir, "environment.yml", f"{root_path}"),
        _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
        f"select system$get_snowsight_host()",
        f"select current_account_name()",
    ]


@pytest.mark.parametrize("project_name", ["example_streamlit", "example_streamlit_v2"])
@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
@mock_streamlit_exists
def test_deploy_streamlit_main_and_pages_files_experimental_replace(
    mock_list_files,
    mock_param,
    mock_connector,
    mock_cursor,
    runner,
    mock_ctx,
    alter_snowflake_yml,
    project_directory,
    project_name,
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

    with project_directory(project_name) as tmp_dir:
        if project_name == "example_streamlit_v2":
            alter_snowflake_yml(
                tmp_dir / "snowflake.yml",
                parameter_path="entities.test_streamlit.artifacts",
                value=["streamlit_app.py", "environment.yml", "pages/"],
            )
        result = runner.invoke(["streamlit", "deploy", "--experimental", "--replace"])

    root_path = f"@streamlit/MockDatabase.MockSchema.{STREAMLIT_NAME}/default_checkout"
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
        "create stage if not exists IDENTIFIER('streamlit')",
        _put_query(tmp_dir, "environment.yml", f"{root_path}"),
        _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
        f"select system$get_snowsight_host()",
        f"select current_account_name()",
    ]


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
    assert ctx.get_query() == f"drop streamlit IDENTIFIER('{STREAMLIT_NAME}')"


@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
def test_get_streamlit_url(mock_param, mock_connector, mock_cursor, runner, mock_ctx):
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

    result = runner.invoke(["streamlit", "get-url", STREAMLIT_NAME])

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        "select system$get_snowsight_host()",
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


@pytest.mark.parametrize("entity_id", ["app_1", "app_2"])
@mock.patch("snowflake.cli._plugins.streamlit.commands.StreamlitManager")
@mock.patch("snowflake.cli._plugins.streamlit.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_selecting_streamlit_from_pdf(
    _, __, mock_manager, project_directory, runner, entity_id
):

    with project_directory("example_streamlit_multiple_v2"):
        result = runner.invoke(["streamlit", "deploy", entity_id])

    assert result.exit_code == 0, result.output

    calls = mock_manager().deploy
    assert calls.call_count == 1

    # Make sure the streamlit was called with proper app definition
    st = calls.call_args.kwargs
    assert st["streamlit"].entity_id == entity_id


@mock.patch("snowflake.connector.connect")
def test_multiple_streamlit_raise_error_if_multiple_entities(
    _, runner, project_directory, os_agnostic_snapshot
):

    with project_directory("example_streamlit_multiple_v2"):
        result = runner.invoke(["streamlit", "deploy"])

    assert result.exit_code == 2, result.output
    assert result.output == os_agnostic_snapshot


@mock.patch("snowflake.connector.connect")
@mock.patch(
    GET_UI_PARAMETERS,
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_deploy_streamlit_with_comment_v2(
    mock_list_files,
    mock_param,
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
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )
    mock_connector.return_value = ctx

    with project_directory("example_streamlit_with_comment_v2") as tmp_dir:
        result = runner.invoke(["streamlit", "deploy", "--replace"])

    root_path = f"@MockDatabase.MockSchema.streamlit/test_streamlit_deploy_snowcli"
    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == [
        f"describe streamlit IDENTIFIER('MockDatabase.MockSchema.test_streamlit_deploy_snowcli')",
        "create stage if not exists IDENTIFIER('MockDatabase.MockSchema.streamlit')",
        _put_query(tmp_dir, "environment.yml", root_path),
        _put_query(tmp_dir, "pages/my_page.py", f"{root_path}/pages"),
        _put_query(tmp_dir, "streamlit_app.py", root_path),
        dedent(
            f"""
            CREATE OR REPLACE STREAMLIT IDENTIFIER('MockDatabase.MockSchema.test_streamlit_deploy_snowcli')
            ROOT_LOCATION = '@MockDatabase.MockSchema.streamlit/test_streamlit_deploy_snowcli'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = xsmall
            TITLE = 'My Streamlit App with Comment'
            COMMENT = 'This is a test comment'
            """
        ).strip(),
        "select system$get_snowsight_host()",
        "select current_account_name()",
    ]


@mock.patch.object(StreamlitManager, "execute")
def test_execute_streamlit(mock_execute, runner):
    result = runner.invoke(["streamlit", "execute", STREAMLIT_NAME])

    assert result.exit_code == 0, result.output
    assert result.output == f"Streamlit {STREAMLIT_NAME} executed.\n"
    mock_execute.assert_called_once_with(app_name=FQN.from_string(STREAMLIT_NAME))
