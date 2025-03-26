import os
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.connector.compat import IS_WINDOWS

bundle_root = Path("output") / "bundle" / "streamlit"
STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"


@pytest.mark.parametrize(
    "artifacts, paths",
    [
        (
            "src",
            [
                {"local": bundle_root / "src" / "app.py", "stage": "/src"},
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                },
            ],
        ),
        (
            "src/",
            [
                {"local": bundle_root / "src" / "app.py", "stage": "/src"},
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                },
            ],
        ),
        (
            "src/*",
            [
                {"local": bundle_root / "src" / "app.py", "stage": "/src"},
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                },
            ],
        ),
        ("src/*.py", [{"local": bundle_root / "src" / "app.py", "stage": "/src"}]),
        (
            "src/dir/dir_app.py",
            [
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                }
            ],
        ),
        (
            {"src": "src/**/*", "dest": "source/"},
            [
                {"local": bundle_root / "source" / "app.py", "stage": "/source"},
                {"local": bundle_root / "source" / "dir_app.py", "stage": "/source"},
                {
                    "local": bundle_root / "source" / "dir" / "dir_app.py",
                    "stage": "/source/dir",
                },
            ],
        ),
        (
            {"src": "src", "dest": "source/"},
            [
                {
                    "local": bundle_root / "source" / "src" / "app.py",
                    "stage": "/source/src",
                },
                {
                    "local": bundle_root / "source" / "src" / "dir" / "dir_app.py",
                    "stage": "/source/src/dir",
                },
            ],
        ),
        (
            {"src": "src/", "dest": "source/"},
            [
                {
                    "local": bundle_root / "source" / "src" / "app.py",
                    "stage": "/source/src",
                },
                {
                    "local": bundle_root / "source" / "src" / "dir" / "dir_app.py",
                    "stage": "/source/src/dir",
                },
            ],
        ),
        (
            {"src": "src/*", "dest": "source/"},
            [
                {"local": bundle_root / "source" / "app.py", "stage": "/source"},
                {
                    "local": bundle_root / "source" / "dir" / "dir_app.py",
                    "stage": "/source/dir",
                },
            ],
        ),
        (
            {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
            [
                {
                    "local": bundle_root / "source" / "dir" / "apps" / "dir_app.py",
                    "stage": "/source/dir/apps",
                }
            ],
        ),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.StageManager.put")
@mock.patch(
    "snowflake.cli._plugins.connection.util.get_ui_parameters",
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_deploy_with_artifacts(
    mock_list_files,
    mock_param,
    mock_sm_put,
    mock_conn,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    artifacts,
    paths,
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
    mock_conn.return_value = ctx

    streamlit_files = [
        "streamlit_app.py",
        "pages/my_page.py",
        "environment.yml",
    ]

    with project_directory("glob_patterns") as tmp:
        alter_snowflake_yml(
            tmp / "snowflake.yml",
            "entities.my_streamlit.artifacts",
            streamlit_files + [artifacts],
        )

        result = runner.invoke(
            [
                "streamlit",
                "deploy",
                "--replace",
            ]
        )
        assert result.exit_code == 0, result.output

        put_calls = _extract_put_calls(mock_sm_put)
        # Windows needs absolute paths.
        if IS_WINDOWS:
            tmp_path = tmp.absolute()
        else:
            tmp_path = tmp.resolve()
        for path in paths:
            assert {
                "local_path": tmp_path / path["local"],
                "stage_path": "@MockDatabase.MockSchema.streamlit/test_streamlit_deploy_snowcli"
                + path["stage"],
            } in put_calls


@pytest.mark.parametrize(
    "artifacts, paths",
    [
        (
            "src",
            [
                {"local": bundle_root / "src" / "app.py", "stage": "/src"},
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                },
            ],
        ),
        (
            "src/",
            [
                {"local": bundle_root / "src" / "app.py", "stage": "/src"},
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                },
            ],
        ),
        (
            "src/*",
            [
                {"local": bundle_root / "src" / "app.py", "stage": "/src"},
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                },
            ],
        ),
        ("src/*.py", [{"local": bundle_root / "src" / "app.py", "stage": "/src"}]),
        (
            "src/dir/dir_app.py",
            [
                {
                    "local": bundle_root / "src" / "dir" / "dir_app.py",
                    "stage": "/src/dir",
                }
            ],
        ),
        (
            {"src": "src/**/*", "dest": "source/"},
            [
                {"local": bundle_root / "source" / "app.py", "stage": "/source"},
                {"local": bundle_root / "source" / "dir_app.py", "stage": "/source"},
                {
                    "local": bundle_root / "source" / "dir" / "dir_app.py",
                    "stage": "/source/dir",
                },
            ],
        ),
        (
            {"src": "src", "dest": "source/"},
            [
                {
                    "local": bundle_root / "source" / "src" / "app.py",
                    "stage": "/source/src",
                },
                {
                    "local": bundle_root / "source" / "src" / "dir" / "dir_app.py",
                    "stage": "/source/src/dir",
                },
            ],
        ),
        (
            {"src": "src/", "dest": "source/"},
            [
                {
                    "local": bundle_root / "source" / "src" / "app.py",
                    "stage": "/source/src",
                },
                {
                    "local": bundle_root / "source" / "src" / "dir" / "dir_app.py",
                    "stage": "/source/src/dir",
                },
            ],
        ),
        (
            {"src": "src/*", "dest": "source/"},
            [
                {"local": bundle_root / "source" / "app.py", "stage": "/source"},
                {
                    "local": bundle_root / "source" / "dir" / "dir_app.py",
                    "stage": "/source/dir",
                },
            ],
        ),
        (
            {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
            [
                {
                    "local": bundle_root / "source" / "dir" / "apps" / "dir_app.py",
                    "stage": "/source/dir/apps",
                }
            ],
        ),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.StageManager.put")
@mock.patch(
    "snowflake.cli._plugins.connection.util.get_ui_parameters",
    return_value={UIParameter.NA_ENABLE_REGIONLESS_REDIRECT: False},
)
@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_deploy_with_artifacts_from_other_directory(
    mock_list_files,
    mock_param,
    mock_sm_put,
    mock_conn,
    mock_cursor,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    artifacts,
    paths,
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
    mock_conn.return_value = ctx

    streamlit_files = [
        "streamlit_app.py",
        "pages/my_page.py",
        "environment.yml",
    ]

    with project_directory("glob_patterns") as tmp:
        os.chdir(Path(os.getcwd()).parent)
        alter_snowflake_yml(
            tmp / "snowflake.yml",
            "entities.my_streamlit.artifacts",
            streamlit_files + [artifacts],
        )

        result = runner.invoke(["streamlit", "deploy", "-p", tmp, "--replace"])
        assert result.exit_code == 0, result.output

        put_calls = _extract_put_calls(mock_sm_put)
        for path in paths:
            assert {
                "local_path": tmp / path["local"],
                "stage_path": "@MockDatabase.MockSchema.streamlit/test_streamlit_deploy_snowcli"
                + path["stage"],
            } in put_calls


def _extract_put_calls(mock_sm_put):
    # Extract the put calls from the mock for better visibility in test logs
    return [
        {
            "local_path": call.kwargs.get("local_path"),
            "stage_path": call.kwargs.get("stage_path"),
        }
        for call in mock_sm_put.mock_calls
        if call.kwargs.get("local_path")
    ]
