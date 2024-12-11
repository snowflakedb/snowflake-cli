import os
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.connector import ProgrammingError
from snowflake.connector.compat import IS_WINDOWS

mock_session_has_warehouse = mock.patch(
    "snowflake.cli.api.sql_execution.SqlExecutionMixin.session_has_warehouse",
    lambda _: True,
)

bundle_root = Path("output") / "bundle" / "snowpark"


@pytest.mark.parametrize(
    "artifacts, local_path, stage_path",
    [
        ("src", bundle_root / "src.zip", "/"),
        ("src/", bundle_root / "src.zip", "/"),
        ("src/*", bundle_root / "src.zip", "/"),
        ("src/*.py", bundle_root / "src.zip", "/"),
        (
            "src/dir/dir_app.py",
            bundle_root / "src" / "dir" / "dir_app.py",
            "/src/dir/",
        ),
        (
            {"src": "src/**/*", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src/", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src/*", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
            bundle_root / "source" / "dir" / "apps" / "dir_app.py",
            "/source/dir/apps/",
        ),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch("snowflake.cli._plugins.snowpark.commands.StageManager.put")
@mock_session_has_warehouse
def test_build_and_deploy_with_artifacts(
    mock_sm_put,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    artifacts,
    local_path,
    stage_path,
    enable_snowpark_glob_support_feature_flag,
):
    mock_om_describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("glob_patterns") as tmp:
        alter_snowflake_yml(
            tmp / "snowflake.yml", "entities.hello_procedure.artifacts", [artifacts]
        )

        result = runner.invoke(
            [
                "snowpark",
                "build",
            ]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke(
            [
                "snowpark",
                "deploy",
            ]
        )
        assert result.exit_code == 0, result.output
        # Windows needs absolute paths.
        if IS_WINDOWS:
            tmp_path = tmp.absolute()
        else:
            tmp_path = tmp.resolve()
        assert {
            "local_path": tmp_path / local_path,
            "stage_path": "@MockDatabase.MockSchema.dev_deployment" + stage_path,
        } in _extract_put_calls(mock_sm_put)


@pytest.mark.parametrize(
    "artifact, local_path, stage_path",
    [
        ("src", bundle_root / "src.zip", "/"),
        ("src/", bundle_root / "src.zip", "/"),
        ("src/*", bundle_root / "src.zip", "/"),
        ("src/*.py", bundle_root / "src.zip", "/"),
        (
            "src/dir/dir_app.py",
            bundle_root / "src" / "dir" / "dir_app.py",
            "/src/dir/",
        ),
        (
            {"src": "src/**/*", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src/", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src/*", "dest": "source/"},
            bundle_root / "source" / "src.zip",
            "/source/",
        ),
        (
            {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
            bundle_root / "source" / "dir" / "apps" / "dir_app.py",
            "/source/dir/apps/",
        ),
    ],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.describe")
@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager.show")
@mock.patch("snowflake.cli._plugins.snowpark.commands.StageManager.put")
@mock_session_has_warehouse
def test_build_and_deploy_with_artifacts_run_from_other_directory(
    mock_sm_put,
    mock_om_show,
    mock_om_describe,
    mock_conn,
    runner,
    mock_ctx,
    project_directory,
    alter_snowflake_yml,
    artifact,
    local_path,
    stage_path,
    enable_snowpark_glob_support_feature_flag,
):
    mock_om_describe.side_effect = ProgrammingError(
        errno=DOES_NOT_EXIST_OR_NOT_AUTHORIZED
    )
    ctx = mock_ctx()
    mock_conn.return_value = ctx

    with project_directory("glob_patterns") as tmp:
        os.chdir(Path(os.getcwd()).parent)
        alter_snowflake_yml(
            tmp / "snowflake.yml", "entities.hello_procedure.artifacts", [artifact]
        )

        result = runner.invoke(
            [
                "snowpark",
                "build",
                "-p",
                tmp,
            ]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke(
            [
                "snowpark",
                "deploy",
                "-p",
                tmp,
            ]
        )
        assert result.exit_code == 0, result.output
        assert {
            "local_path": tmp / local_path,
            "stage_path": "@MockDatabase.MockSchema.dev_deployment" + stage_path,
        } in _extract_put_calls(mock_sm_put)


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
