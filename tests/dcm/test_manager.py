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
import os
from pathlib import PureWindowsPath
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dcm.manager import (
    SOURCES_FOLDER,
    DCMProjectManager,
    UploadPlan,
    resolve_asset_paths,
)
from snowflake.cli._plugins.dcm.models import (
    MANIFEST_FILE_NAME,
    DCMAsset,
    DCMManifest,
)
from snowflake.cli._plugins.dcm.multistep_progress import (
    MultiStepProgress,
    StepDefinition,
    StepState,
)
from snowflake.cli._plugins.dcm.progress import DETAIL_BULLET, FileUploadProgress
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath

from tests.dcm.multi_step_progress_capture import capture_rendered

execute_queries = "snowflake.cli._plugins.dcm.manager.DCMProjectManager.execute_query"
execute_query_with_params = (
    "snowflake.cli._plugins.dcm.manager.DCMProjectManager.execute_query_with_params"
)
TEST_PROJECT = FQN.from_string("my_project")
TEST_SFQID = "af72f4cc-107c-4f1b-b8a9-7a9811203bc5"


@pytest.fixture
def mock_conn_cursor():
    cursor = mock.MagicMock()
    cursor.sfqid = TEST_SFQID
    with mock.patch.object(
        DCMProjectManager, "_conn", new_callable=mock.PropertyMock
    ) as mock_conn:
        mock_conn.return_value.cursor.return_value = cursor
        yield cursor


@pytest.fixture
def mock_from_resource():
    with mock.patch(
        "snowflake.cli._plugins.dcm.manager.FQN.from_resource",
        return_value=FQN(
            database="MockDatabase",
            schema="MockSchema",
            name="DCM_TEST_PIPELINE_1757333281_OUTPUT_TMP_STAGE",
        ),
    ) as _fixture:
        yield _fixture


@mock.patch(execute_queries)
def test_create(mock_execute_query):
    project_identifier = FQN.from_string("project_mock_fqn")
    mgr = DCMProjectManager()
    mgr.create(project_identifier=project_identifier)

    mock_execute_query.assert_called_once_with(
        "CREATE DCM PROJECT IDENTIFIER('project_mock_fqn')"
    )


@mock.patch(execute_queries)
def test_analyze_project_basic(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_analyze_project_with_configuration(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE USING CONFIGURATION some_configuration FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_analyze_project_with_variables(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        variables=["key=value", "aaa=bbb"],
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE USING"
        " (key=>value, aaa=>bbb) FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_analyze_project_with_configuration_and_variables(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        variables=["key=value", "aaa=bbb"],
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_analyze_project_with_output_path(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        output_path="@output_stage/outputs",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE USING CONFIGURATION"
        " some_configuration FROM @test_stage OUTPUT_PATH @output_stage/outputs"
    )


@mock.patch(execute_queries)
@mock.patch(execute_query_with_params)
def test_raw_analyze_project_with_env_vars(
    mock_execute_with_params, mock_execute_query
):
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        env_vars=env_vars,
    )

    mock_execute_with_params.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE ENVIRONMENT (?)"
        " FROM @my_stage",
        params=[json.dumps(env_vars)],
    )
    mock_execute_query.assert_not_called()


@mock.patch(execute_queries)
@mock.patch(execute_query_with_params)
def test_raw_analyze_project_with_output_path_and_env_vars(
    mock_execute_with_params,
    mock_execute_query,
):
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        output_path="@output_stage/outputs",
        env_vars=env_vars,
    )

    mock_execute_with_params.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE ENVIRONMENT (?)"
        " FROM @test_stage OUTPUT_PATH @output_stage/outputs",
        params=[json.dumps(env_vars)],
    )
    mock_execute_query.assert_not_called()


def test_deploy_async_project(mock_conn_cursor):
    mgr = DCMProjectManager()
    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @test_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_deploy_async_project_with_skip_plan(mock_conn_cursor):
    mgr = DCMProjectManager()
    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
        skip_plan=True,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @test_stage SKIP PLAN",
        None,
        _force_qmark_paramstyle=True,
    )


def test_deploy_async_project_with_from_stage(mock_conn_cursor):
    mgr = DCMProjectManager()
    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @my_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_deploy_async_project_with_from_stage_without_prefix(mock_conn_cursor):
    mgr = DCMProjectManager()
    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT,
        from_stage="my_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @my_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_deploy_async_project_with_default_deployment(
    mock_conn_cursor, project_directory
):
    mgr = DCMProjectManager()

    sfqid = mgr.deploy_async(project_identifier=TEST_PROJECT, from_stage="@test_stage")

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY FROM @test_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_deploy_async_project_with_env_vars(mock_conn_cursor):
    mgr = DCMProjectManager()
    env_vars = {"DB_HOST": "prod.analytics.internal"}

    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        env_vars=env_vars,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION"
        " some_configuration ENVIRONMENT (?) FROM @test_stage",
        [json.dumps(env_vars)],
        _force_qmark_paramstyle=True,
    )


def test_deploy_async_project_without_env_vars_passes_no_params(mock_conn_cursor):
    mgr = DCMProjectManager()

    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT, from_stage="@test_stage", env_vars=None
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY FROM @test_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_deploy_async_project_with_empty_env_vars_passes_no_params(mock_conn_cursor):
    mgr = DCMProjectManager()

    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT, from_stage="@test_stage", env_vars={}
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY FROM @test_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_plan_async_project_with_output_path(mock_conn_cursor):
    mgr = DCMProjectManager()

    sfqid = mgr.plan_async(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        output_path="@output_stage/outputs",
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION"
        " some_configuration FROM @test_stage OUTPUT_PATH @output_stage/outputs",
        None,
        _force_qmark_paramstyle=True,
    )


def test_plan_async_project_with_from_stage(mock_conn_cursor):
    mgr = DCMProjectManager()

    sfqid = mgr.plan_async(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        configuration="some_configuration",
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION"
        " some_configuration FROM @my_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_plan_async_project_with_delta(mock_conn_cursor):
    mgr = DCMProjectManager()

    sfqid = mgr.plan_async(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        delta=True,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN DELTA FROM @my_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_plan_async_project_with_env_vars(mock_conn_cursor):
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    sfqid = mgr.plan_async(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        env_vars=env_vars,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN ENVIRONMENT (?)"
        " FROM @my_stage",
        [json.dumps(env_vars)],
        _force_qmark_paramstyle=True,
    )


def test_plan_async_project_with_configuration_variables_and_env_vars(
    mock_conn_cursor,
):
    # ENVIRONMENT is stacked after USING CONFIGURATION/variables and before FROM --
    # verifies the two clause-building code paths (templating vars vs. env vars)
    # compose correctly instead of one clobbering the other.
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    sfqid = mgr.plan_async(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        configuration="some_configuration",
        variables=["key=value"],
        env_vars=env_vars,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION"
        " some_configuration (key=>value) ENVIRONMENT (?) FROM @my_stage",
        [json.dumps(env_vars)],
        _force_qmark_paramstyle=True,
    )


def test_plan_async_project_with_output_path_and_env_vars(mock_conn_cursor):
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    sfqid = mgr.plan_async(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        output_path="@output_stage/outputs",
        env_vars=env_vars,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN ENVIRONMENT (?)"
        " FROM @test_stage OUTPUT_PATH @output_stage/outputs",
        [json.dumps(env_vars)],
        _force_qmark_paramstyle=True,
    )


@mock.patch(execute_queries)
def test_list_deployments(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.list_deployments(project_identifier=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="SHOW DEPLOYMENTS IN DCM PROJECT IDENTIFIER('my_project')"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize("if_exists", [True, False])
def test_drop_deployment(mock_execute_query, if_exists):
    mgr = DCMProjectManager()
    mgr.drop_deployment(
        project_identifier=TEST_PROJECT, deployment_name="v1", if_exists=if_exists
    )

    expected_query = "ALTER DCM PROJECT IDENTIFIER('my_project') DROP DEPLOYMENT"
    if if_exists:
        expected_query += " IF EXISTS"
    expected_query += ' "v1"'

    mock_execute_query.assert_called_once_with(query=expected_query)


@mock.patch(execute_queries)
def test_preview_project_basic(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.preview(
        project_identifier=TEST_PROJECT,
        object_identifier=FQN.from_string("my_table"),
        from_stage="@test_stage",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PREVIEW IDENTIFIER('my_table') FROM @test_stage"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize(
    "configuration,variables,limit,expected_suffix",
    [
        (
            "dev",
            ["key=value"],
            10,
            " USING CONFIGURATION dev (key=>value) FROM @test_stage LIMIT 10",
        ),
        (
            "prod",
            None,
            None,
            " USING CONFIGURATION prod FROM @test_stage",
        ),
        (
            None,
            ["var1=val1", "var2=val2"],
            5,
            " USING (var1=>val1, var2=>val2) FROM @test_stage LIMIT 5",
        ),
        (
            None,
            None,
            100,
            " FROM @test_stage LIMIT 100",
        ),
    ],
)
def test_preview_project_with_various_options(
    mock_execute_query, configuration, variables, limit, expected_suffix
):
    mgr = DCMProjectManager()
    mgr.preview(
        project_identifier=TEST_PROJECT,
        object_identifier=FQN.from_string("my_view"),
        from_stage="@test_stage",
        configuration=configuration,
        variables=variables,
        limit=limit,
    )

    expected_query = (
        f"EXECUTE DCM PROJECT IDENTIFIER('my_project') PREVIEW IDENTIFIER('my_view')"
        + expected_suffix
    )
    mock_execute_query.assert_called_once_with(query=expected_query)


@mock.patch(execute_queries)
@mock.patch(execute_query_with_params)
def test_preview_project_with_env_vars(mock_execute_with_params, mock_execute_query):
    mgr = DCMProjectManager()
    env_vars = {"DB_HOST": "prod.analytics.internal"}

    mgr.preview(
        project_identifier=TEST_PROJECT,
        object_identifier=FQN.from_string("my_view"),
        from_stage="@test_stage",
        env_vars=env_vars,
    )

    mock_execute_with_params.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PREVIEW"
        " IDENTIFIER('my_view') ENVIRONMENT (?) FROM @test_stage",
        params=[json.dumps(env_vars)],
    )
    mock_execute_query.assert_not_called()


@mock.patch(execute_queries)
def test_refresh_project(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.refresh(project_identifier=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') REFRESH ALL"
    )


@mock.patch(execute_queries)
def test_test_project(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.test(project_identifier=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') TEST ALL"
    )


@pytest.mark.parametrize(
    "alias,expected_alias",
    [
        ("test-1", '"test-1"'),
        ("my alias", '"my alias"'),
        ("v1.0", '"v1.0"'),
        ("test_alias", '"test_alias"'),
        ("v1", '"v1"'),
    ],
)
def test_deploy_async_project_with_alias_special_characters(
    mock_conn_cursor, alias, expected_alias
):
    mgr = DCMProjectManager()
    sfqid = mgr.deploy_async(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        alias=alias,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        f"EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY AS {expected_alias} FROM @test_stage",
        None,
        _force_qmark_paramstyle=True,
    )


def test_purge_async_project(mock_conn_cursor):
    mgr = DCMProjectManager()
    sfqid = mgr.purge_async(
        project_identifier=TEST_PROJECT,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PURGE",
        None,
        _force_qmark_paramstyle=True,
    )


def test_purge_async_project_with_skip_plan(mock_conn_cursor):
    mgr = DCMProjectManager()
    sfqid = mgr.purge_async(
        project_identifier=TEST_PROJECT,
        skip_plan=True,
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PURGE SKIP PLAN",
        None,
        _force_qmark_paramstyle=True,
    )


def test_purge_async_project_with_alias(mock_conn_cursor):
    mgr = DCMProjectManager()
    sfqid = mgr.purge_async(
        project_identifier=TEST_PROJECT,
        alias="my_alias",
    )

    assert sfqid == TEST_SFQID
    mock_conn_cursor.execute_async.assert_called_once_with(
        "EXECUTE DCM PROJECT IDENTIFIER('my_project') PURGE AS \"my_alias\"",
        None,
        _force_qmark_paramstyle=True,
    )


class TestSyncLocalFiles:
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put")
    @mock.patch(
        "snowflake.cli._plugins.dcm.manager.DCMProjectManager._bundle_definition_files"
    )
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_uploads_to_temporary_stage(
        self,
        mock_create_stage,
        mock_bundle,
        mock_put,
        mock_put_recursive,
        project_directory,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        mock_put_recursive.return_value = iter([])

        with project_directory("dcm_project"):
            result = DCMProjectManager.sync_local_files(
                project_identifier=TEST_PROJECT, progress=mock.MagicMock()
            )

            mock_create_stage.assert_called_once()
            assert mock_create_stage.call_args.kwargs["temporary"] is True

            mock_bundle.assert_called_once()

            mock_put_recursive.assert_called_once()
            assert mock_put_recursive.call_args.kwargs["stage_path"] == str(
                mock_from_resource()
            )

            mock_put.assert_not_called()

            assert result == str(mock_from_resource())

    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put")
    @mock.patch(
        "snowflake.cli._plugins.dcm.manager.DCMProjectManager._bundle_definition_files"
    )
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_with_source_directory(
        self,
        _mock_create_stage,
        mock_bundle,
        mock_put,
        mock_put_recursive,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        mock_put_recursive.return_value = iter([])
        source_dir = tmp_path / "custom_source"
        source_dir.mkdir()

        manifest_content = {
            "manifest_version": 2,
            "type": "dcm_project",
        }
        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()
        (sources_dir / "custom_query.sql").touch()

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT,
            source_directory=str(source_dir),
            progress=mock.MagicMock(),
        )

        mock_bundle.assert_called_once()
        actual_project_root = mock_bundle.call_args.kwargs["project_root"]
        assert actual_project_root.resolve() == source_dir.resolve()

    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put")
    @mock.patch(
        "snowflake.cli._plugins.dcm.manager.DCMProjectManager._bundle_definition_files"
    )
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_with_relative_source_directory(
        self,
        _mock_create_stage,
        mock_bundle,
        mock_put,
        mock_put_recursive,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        mock_put_recursive.return_value = iter([])
        source_dir = tmp_path / "relative_source"
        source_dir.mkdir()

        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump({"manifest_version": 2, "type": "dcm_project"}, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()
        (sources_dir / "file.sql").touch()

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            DCMProjectManager.sync_local_files(
                project_identifier=TEST_PROJECT,
                source_directory="relative_source",
                progress=mock.MagicMock(),
            )

            mock_bundle.assert_called_once()
            actual_project_root = mock_bundle.call_args.kwargs["project_root"]
            assert actual_project_root.is_absolute()
            assert actual_project_root.resolve() == source_dir.resolve()
        finally:
            os.chdir(original_cwd)

    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put")
    @mock.patch(
        "snowflake.cli._plugins.dcm.manager.DCMProjectManager._bundle_definition_files"
    )
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_collects_manifest_and_sources(
        self,
        _mock_create_stage,
        mock_bundle,
        mock_put,
        mock_put_recursive,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        mock_put_recursive.return_value = iter([])
        source_dir = tmp_path / "project_with_sources"
        source_dir.mkdir()

        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump({"manifest_version": 2, "type": "dcm_project"}, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()

        definitions_dir = sources_dir / "definitions"
        definitions_dir.mkdir()
        (definitions_dir / "table.sql").touch()

        macros_dir = sources_dir / "macros"
        macros_dir.mkdir()
        (macros_dir / "helpers.sql").touch()
        (macros_dir / "utils.jinja").touch()

        (sources_dir / "dbt_project.yml").touch()

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT,
            source_directory=str(source_dir),
            progress=mock.MagicMock(),
        )

        mock_bundle.assert_called_once()
        artifacts = mock_bundle.call_args.kwargs["artifacts"]
        artifact_srcs = [a.src for a in artifacts]

        assert MANIFEST_FILE_NAME in artifact_srcs
        assert SOURCES_FOLDER in artifact_srcs

    @mock.patch("snowflake.cli._plugins.stage.manager.StageManager.execute_query")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_uploads_hidden_files(
        self,
        _mock_create_stage,
        mock_execute_query,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        mock_execute_query.return_value = mock_cursor(rows=[], columns=[])

        source_dir = tmp_path / "project_with_dotfiles"
        source_dir.mkdir()
        with open(source_dir / MANIFEST_FILE_NAME, "w") as f:
            yaml.dump({"manifest_version": 2, "type": "dcm_project"}, f)

        dbt = source_dir / SOURCES_FOLDER / "dbt"
        (dbt / "models").mkdir(parents=True)
        (dbt / ".gitignore").touch()
        (dbt / "models" / "model.sql").touch()

        hidden_dir = source_dir / SOURCES_FOLDER / ".hidden_dir"
        (hidden_dir / "sub").mkdir(parents=True)
        (hidden_dir / "visible.sql").touch()
        (hidden_dir / "sub" / "deep.sql").touch()

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT,
            source_directory=str(source_dir),
            progress=mock.MagicMock(),
        )

        put_queries = [
            call.args[0]
            for call in mock_execute_query.call_args_list
            if call.args and call.args[0].lstrip().lower().startswith("put ")
        ]
        for q in put_queries:
            assert (
                "/dbt/*" not in q
            ), f"PUT for dotfile-only dbt/ dir would crash the connector: {q}"
            assert (
                "/.hidden_dir/*" not in q
            ), f"hidden dir must not be uploaded via dir/* glob: {q}"
        for filename, stage_dest in (
            (".gitignore", f"/{SOURCES_FOLDER}/dbt"),
            ("visible.sql", f"/{SOURCES_FOLDER}/.hidden_dir"),
            ("deep.sql", f"/{SOURCES_FOLDER}/.hidden_dir/sub"),
        ):
            assert any(
                filename in q and stage_dest in q for q in put_queries
            ), f"expected a PUT for {filename} to {stage_dest}; got: {put_queries}"


def test_connection_returns_underlying_connection():
    # given
    sentinel = object()

    with mock.patch.object(
        DCMProjectManager, "_conn", new_callable=mock.PropertyMock
    ) as mock_conn:
        mock_conn.return_value = sentinel

        # when
        connection = DCMProjectManager().connection

        # then
        assert connection is sentinel


def test_add_sources_without_sources_folder_is_noop(tmp_path):
    # given
    plan = UploadPlan()

    # when
    DCMProjectManager._add_sources(plan, tmp_path, "@stage")  # noqa: SLF001

    # then
    assert plan.artifacts == []
    assert plan.relative_paths_to_upload == []
    assert plan.individual_files == []


def test_add_sources_records_paths_for_folder_grouping(tmp_path):
    # given: a nested source file, so the relative path has a separator in it
    plan = UploadPlan()
    nested = tmp_path / SOURCES_FOLDER / "definitions" / "deeper"
    nested.mkdir(parents=True)
    (nested / "a.sql").touch()

    # when
    DCMProjectManager._add_sources(plan, tmp_path, "@stage")  # noqa: SLF001

    # then: recorded as parts, so _upload_folders groups the same way on every
    # platform without splitting on a separator the host may not use
    assert [rel.parts for rel in plan.relative_paths_to_upload] == [
        (SOURCES_FOLDER, "definitions", "deeper", "a.sql")
    ]


def test_windows_relative_path_is_recorded_as_parts():
    # given: a relative path shaped the way Windows produces one
    relative = PureWindowsPath("definitions") / "deeper" / "a.sql"

    # when
    recorded = DCMProjectManager._sources_relative_path(relative)  # noqa: SLF001

    # then: the windows separators become parts, whatever the host flavour is
    assert recorded.parts == (SOURCES_FOLDER, "definitions", "deeper", "a.sql")


class TestSyncLocalFilesProgress:
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put")
    @mock.patch(
        "snowflake.cli._plugins.dcm.manager.DCMProjectManager._bundle_definition_files"
    )
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_advances_progress_per_uploaded_file(
        self,
        _mock_create_stage,
        _mock_bundle,
        _mock_put,
        mock_put_recursive,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        # given
        mock_put_recursive.return_value = iter(
            [
                {"source": "a.sql", "target": "stage/a.sql"},
                {"source": "b.sql", "target": "stage/b.sql"},
            ]
        )
        source_dir = tmp_path / "project_with_progress"
        source_dir.mkdir()
        with open(source_dir / MANIFEST_FILE_NAME, "w") as f:
            yaml.dump({"manifest_version": 2, "type": "dcm_project"}, f)
        hidden_dir = source_dir / SOURCES_FOLDER / ".hidden"
        hidden_dir.mkdir(parents=True)
        (hidden_dir / "x.sql").touch()

        progress = MultiStepProgress([StepDefinition("upload", "Uploading")])
        updater = progress.step_progress_updater("upload")

        # when
        with mock.patch.object(FileUploadProgress, "advance") as mock_advance:
            DCMProjectManager.sync_local_files(
                project_identifier=TEST_PROJECT,
                source_directory=str(source_dir),
                progress=updater,
            )

        # then
        assert mock_advance.call_count == 3
        assert progress.step_state("upload") == StepState.RUNNING


# Project tree from the spec's glob cookbook (reserved paths omitted so the
# glob assertions are unambiguous; reserved-path exclusion is server-side).
_SPEC_TREE = [
    "README.md",
    "config.yaml",
    "scripts/build.py",
    "apps/index.md",
    "apps/sales/main.py",
    "apps/sales/logo.png",
    "apps/sales/util/helpers.py",
    "data[1].csv",  # literal brackets in a filename
]


def _make_tree(root, files):
    for rel in files:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")


class TestResolveAssetPaths:
    """E2E: a manifest's asset patterns -> the exact set of files selected.

    Mirrors the spec's glob cookbook so the matching engine is airtight.
    """

    @pytest.fixture
    def project(self, tmp_path):
        _make_tree(tmp_path, _SPEC_TREE)
        return tmp_path

    @pytest.mark.parametrize(
        "pattern, expected",
        [
            ("config.yaml", {"config.yaml"}),
            # a literal directory -> its whole subtree
            (
                "apps/sales",
                {
                    "apps/sales/main.py",
                    "apps/sales/logo.png",
                    "apps/sales/util/helpers.py",
                },
            ),
            # '*' -> top-level files only, does not descend into scripts/ or apps/
            ("*", {"README.md", "config.yaml", "data[1].csv"}),
            # '**/*' -> every file, any depth
            (
                "**/*",
                {
                    "README.md",
                    "config.yaml",
                    "data[1].csv",
                    "scripts/build.py",
                    "apps/index.md",
                    "apps/sales/main.py",
                    "apps/sales/logo.png",
                    "apps/sales/util/helpers.py",
                },
            ),
            (
                "**/*.py",
                {
                    "scripts/build.py",
                    "apps/sales/main.py",
                    "apps/sales/util/helpers.py",
                },
            ),
            (
                "apps/**/*",
                {
                    "apps/index.md",
                    "apps/sales/main.py",
                    "apps/sales/logo.png",
                    "apps/sales/util/helpers.py",
                },
            ),
            ("apps/**/*.py", {"apps/sales/main.py", "apps/sales/util/helpers.py"}),
            # 'apps/*' -> direct children only, does not descend into apps/sales/
            ("apps/*", {"apps/index.md"}),
            ("apps/sales/*.png", {"apps/sales/logo.png"}),
            # '[' and ']' are literal, not a character class
            ("data[1].csv", {"data[1].csv"}),
        ],
    )
    def test_glob_cookbook(self, project, pattern, expected):
        resolved = resolve_asset_paths(project, [DCMAsset(name="a", paths=[pattern])])
        assert set(resolved) == expected

    def test_bracket_pattern_is_literal_not_char_class(self, tmp_path):
        # A char-class interpretation of data[1].csv would match data1.csv.
        _make_tree(tmp_path, ["data[1].csv", "data1.csv"])
        resolved = resolve_asset_paths(
            tmp_path, [DCMAsset(name="a", paths=["data[1].csv"])]
        )
        assert resolved == ["data[1].csv"]

    def test_merged_paths_union_deduped(self, project):
        resolved = resolve_asset_paths(
            project,
            [DCMAsset(name="a", paths=["apps/*", "apps/index.md", "config.yaml"])],
        )
        # apps/index.md matches both of the first two entries but appears once;
        # output is sorted and de-duplicated.
        assert resolved == ["apps/index.md", "config.yaml"]

    def test_multiple_assets_union(self, project):
        resolved = resolve_asset_paths(
            project,
            [
                DCMAsset(name="cfg", paths=["config.yaml"]),
                DCMAsset(name="py", paths=["**/*.py"]),
            ],
        )
        assert set(resolved) == {
            "config.yaml",
            "scripts/build.py",
            "apps/sales/main.py",
            "apps/sales/util/helpers.py",
        }

    def test_dotfiles_excluded(self, tmp_path):
        _make_tree(
            tmp_path, ["visible.sql", ".hidden.sql", "dir/.secret", "dir/ok.sql"]
        )
        resolved = resolve_asset_paths(tmp_path, [DCMAsset(name="a", paths=["**/*"])])
        assert set(resolved) == {"visible.sql", "dir/ok.sql"}

    # A tree with dotfiles at the root, inside a normal subfolder, and inside
    # dot-directories at both the root and a subfolder. `Path.glob`/`rglob`
    # *do* yield dot-prefixed entries (unlike shell globbing), so the resolver's
    # `_is_hidden` filter is what actually keeps them out -- verify it holds for
    # every pattern shape and at every depth.
    _DOTFILE_TREE = [
        "visible.txt",
        ".roothidden",  # dotfile at the project root
        "pub/visible.py",
        "pub/.hidden.py",  # dotfile in a subfolder
        "pub/.hiddendir/inside.txt",  # file inside a subfolder dot-directory
        ".hiddentop/inside.txt",  # file inside a root dot-directory
    ]

    @pytest.mark.parametrize(
        "pattern, expected",
        [
            # root glob: root dotfile excluded, does not descend
            ("*", {"visible.txt"}),
            # recursive: every dotfile at every depth excluded (incl. dot-dirs)
            ("**/*", {"visible.txt", "pub/visible.py"}),
            # subfolder glob: the subfolder's dotfile is excluded
            ("pub/*", {"pub/visible.py"}),
            # subfolder recursive: subfolder dotfile + dot-dir contents excluded
            ("pub/**/*", {"pub/visible.py"}),
            # literal directory -> whole subtree, still skipping dotfiles/dot-dirs
            ("pub", {"pub/visible.py"}),
        ],
    )
    def test_dotfiles_excluded_at_root_and_every_subfolder(
        self, tmp_path, pattern, expected
    ):
        _make_tree(tmp_path, self._DOTFILE_TREE)
        resolved = resolve_asset_paths(tmp_path, [DCMAsset(name="a", paths=[pattern])])
        assert set(resolved) == expected
        # belt-and-suspenders: no resolved path has a dot-prefixed component anywhere
        assert all(not part.startswith(".") for p in resolved for part in p.split("/"))

    @pytest.mark.parametrize(
        "pattern",
        [".roothidden", "pub/.hidden.py", ".hiddentop/inside.txt", ".hiddentop"],
    )
    def test_explicitly_named_dotfile_or_dotdir_is_skipped(self, tmp_path, pattern):
        # Even when a pattern names a dotfile/dot-dir literally (so `Path.glob`
        # *does* yield it), `_is_hidden` drops it -> nothing resolves, warn+skip.
        _make_tree(tmp_path, self._DOTFILE_TREE)
        with mock.patch(
            "snowflake.cli._plugins.dcm.manager.cli_console.warning"
        ) as warn:
            resolved = resolve_asset_paths(
                tmp_path, [DCMAsset(name="a", paths=[pattern])]
            )
        assert resolved == []
        warn.assert_called_once()

    def test_no_match_is_skipped_with_warning(self, project):
        with mock.patch(
            "snowflake.cli._plugins.dcm.manager.cli_console.warning"
        ) as warn:
            resolved = resolve_asset_paths(
                project,
                [DCMAsset(name="a", paths=["nope/*.sql", "config.yaml"])],
            )
        # the no-match pattern is skipped (with a warning); the other resolves
        assert resolved == ["config.yaml"]
        warn.assert_called_once()

    def test_glob_matching_only_dotfiles_is_skipped(self, tmp_path):
        _make_tree(tmp_path, [".env", ".config/x"])
        with mock.patch(
            "snowflake.cli._plugins.dcm.manager.cli_console.warning"
        ) as warn:
            resolved = resolve_asset_paths(
                tmp_path, [DCMAsset(name="a", paths=["**/*"])]
            )
        assert resolved == []
        warn.assert_called_once()

    @pytest.mark.skipif(
        os.name == "nt", reason="symlink creation is unreliable on Windows CI"
    )
    def test_symlink_escaping_project_root_is_skipped(self, tmp_path):
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret.txt").write_text("s")
        project = tmp_path / "proj"
        project.mkdir()
        (project / "ok.txt").write_text("x")
        (project / "link").symlink_to(outside, target_is_directory=True)

        # 'link/secret.txt' resolves outside the project root -> excluded
        resolved = resolve_asset_paths(project, [DCMAsset(name="a", paths=["**/*"])])
        assert set(resolved) == {"ok.txt"}

    def test_from_loaded_manifest(self, tmp_path):
        _make_tree(tmp_path, _SPEC_TREE)
        with open(tmp_path / MANIFEST_FILE_NAME, "w") as f:
            yaml.dump(
                {
                    "manifest_version": 2,
                    "type": "dcm_project",
                    "assets": {
                        "docs": {"path": "apps/*"},
                        "code": {"paths": ["**/*.py", "config.yaml"]},
                    },
                },
                f,
                sort_keys=False,
            )
        manifest = DCMManifest.load(SecurePath(tmp_path))
        resolved = resolve_asset_paths(tmp_path, list(manifest.assets.values()))
        assert set(resolved) == {
            "apps/index.md",
            "scripts/build.py",
            "apps/sales/main.py",
            "apps/sales/util/helpers.py",
            "config.yaml",
        }


class TestAssetUpload:
    def test_build_upload_plan_adds_resolved_files_as_artifacts(self, tmp_path):
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "seed.csv").write_text("x")
        (tmp_path / "data" / "other.csv").write_text("y")

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            tmp_path, "@stage", assets=[DCMAsset(name="seeds", paths=["data/*.csv"])]
        )

        # concrete files added as their own artifacts (no directory re-expansion)
        srcs = [a.src for a in plan.artifacts]
        uploaded = [p.as_posix() for p in plan.relative_paths_to_upload]
        assert "data/seed.csv" in srcs
        assert "data/other.csv" in srcs
        assert "data/seed.csv" in uploaded
        assert "data/other.csv" in uploaded

    def test_build_upload_plan_excludes_dotfiles(self, tmp_path):
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "seed.csv").write_text("x")
        (tmp_path / "data" / ".secret.csv").write_text("s")

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            tmp_path, "@stage", assets=[DCMAsset(name="seeds", paths=["data/*"])]
        )

        uploaded = [p.as_posix() for p in plan.relative_paths_to_upload]
        assert "data/seed.csv" in uploaded
        assert "data/.secret.csv" not in uploaded
        assert all(".secret" not in a.src for a in plan.artifacts)

    def test_build_upload_plan_excludes_dotfiles_at_every_depth(self, tmp_path):
        # A recursive asset glob must not upload dotfiles at any depth: root
        # dotfile, subfolder dotfile, or a file inside a dot-directory.
        (tmp_path / "pub" / ".hiddendir").mkdir(parents=True)
        (tmp_path / "visible.txt").write_text("x")
        (tmp_path / ".roothidden").write_text("s")
        (tmp_path / "pub" / "visible.py").write_text("x")
        (tmp_path / "pub" / ".hidden.py").write_text("s")
        (tmp_path / "pub" / ".hiddendir" / "inside.txt").write_text("s")

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            tmp_path, "@stage", assets=[DCMAsset(name="a", paths=["**/*"])]
        )

        uploaded = [p.as_posix() for p in plan.relative_paths_to_upload]
        assert "visible.txt" in uploaded
        assert "pub/visible.py" in uploaded
        # nothing dot-prefixed reaches the plan (manifest.yml has no dot segment)
        assert all(
            not part.startswith(".") for path in uploaded for part in path.split("/")
        )
        assert all(
            ".hidden" not in a.src and ".root" not in a.src for a in plan.artifacts
        )

    def test_sources_dotfiles_upload_but_asset_dotfiles_do_not(self, tmp_path):
        # The dotfile exclusion applies to assets only. sources/ has its own
        # custom logic that uploads hidden files individually -- so in one build
        # a sources dotfile is uploaded while an asset dotfile is not.
        sources = tmp_path / SOURCES_FOLDER
        sources.mkdir()
        (sources / ".keep").write_text("s")  # sources dotfile -> uploaded
        (sources / "model.sql").write_text("x")
        (tmp_path / "assets").mkdir()
        (tmp_path / "assets" / ".secret").write_text(
            "nope"
        )  # asset dotfile -> excluded
        (tmp_path / "assets" / "seed.csv").write_text("y")

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            tmp_path, "@stage", assets=[DCMAsset(name="a", paths=["assets/**/*"])]
        )

        uploaded = [p.as_posix() for p in plan.relative_paths_to_upload]
        # sources dotfile IS uploaded (custom individual-file logic)
        assert f"{SOURCES_FOLDER}/.keep" in uploaded
        assert any(fu.file.name == ".keep" for fu in plan.individual_files)
        # asset dotfile is NOT uploaded; the visible asset file is
        assert "assets/seed.csv" in uploaded
        assert "assets/.secret" not in uploaded
        assert all(".secret" not in a.src for a in plan.artifacts)

    def test_build_upload_plan_dedups_against_sources(self, tmp_path):
        sources = tmp_path / SOURCES_FOLDER
        sources.mkdir()
        (sources / "a.sql").write_text("x")

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            # glob overlaps the sources/ tree already scheduled by _add_sources
            tmp_path,
            "@stage",
            assets=[DCMAsset(name="x", paths=["sources/*.sql"])],
        )

        uploaded = [p.as_posix() for p in plan.relative_paths_to_upload]
        assert uploaded.count(f"{SOURCES_FOLDER}/a.sql") == 1
        assert f"{SOURCES_FOLDER}/a.sql" not in [a.src for a in plan.artifacts]

    def test_build_upload_plan_no_assets_matches_today(self, tmp_path):
        # No declared assets ([]) leaves the upload plan at just the manifest.
        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            tmp_path, "@stage", assets=[]
        )

        assert [a.src for a in plan.artifacts] == [MANIFEST_FILE_NAME]
        assert [p.as_posix() for p in plan.relative_paths_to_upload] == [
            MANIFEST_FILE_NAME
        ]

    @pytest.mark.skipif(
        os.name == "nt",
        reason="'*'/'?' are illegal in Windows filenames; this re-glob hazard is POSIX-only",
    )
    def test_build_upload_plan_wildcard_filename_not_reglobbed(self, tmp_path):
        # A resolved file whose name contains '*' must be escaped so BundleMap's
        # second glob pass treats it literally -- otherwise 'backup*' re-expands
        # and pulls in the sibling 'backup_2026/' subtree. The bundle contents
        # must therefore *equal* what was reported, not merely contain it.
        project = tmp_path / "proj"
        project.mkdir()
        (project / MANIFEST_FILE_NAME).write_text(
            "manifest_version: 2\ntype: dcm_project\n"
        )
        (project / "backup*").write_text("real file")
        (project / "backup_2026").mkdir()
        (project / "backup_2026" / "leak.txt").write_text("should NOT upload")
        bundle = tmp_path / "bundle"
        bundle.mkdir()

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            project, "@stage", assets=[DCMAsset(name="a", paths=["backup*"])]
        )
        DCMProjectManager._bundle_definition_files(  # noqa: SLF001
            project_root=project, bundle_root=bundle, artifacts=plan.artifacts
        )

        bundled = sorted(
            p.relative_to(bundle).as_posix() for p in bundle.rglob("*") if p.is_file()
        )
        assert bundled == sorted(p.as_posix() for p in plan.relative_paths_to_upload)
        assert "backup_2026/leak.txt" not in bundled

    def test_bundle_definition_files_copies_asset_files(self, tmp_path):
        # deploy_root must live outside the project root, so use sibling dirs.
        project = tmp_path / "proj"
        (project / "data").mkdir(parents=True)
        (project / MANIFEST_FILE_NAME).write_text(
            "manifest_version: 2\ntype: dcm_project\n"
        )
        (project / "data" / "seed.csv").write_text("x")
        (project / "data" / ".secret").write_text("s")
        bundle = tmp_path / "bundle"
        bundle.mkdir()

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            project, "@stage", assets=[DCMAsset(name="seeds", paths=["data/*"])]
        )
        DCMProjectManager._bundle_definition_files(  # noqa: SLF001
            project_root=project, bundle_root=bundle, artifacts=plan.artifacts
        )

        assert (bundle / MANIFEST_FILE_NAME).is_file()
        assert (bundle / "data" / "seed.csv").is_file()
        # dotfile excluded during resolution -> never bundled
        assert not (bundle / "data" / ".secret").exists()

    def test_bundle_copies_bracketed_filename(self, tmp_path):
        project = tmp_path / "proj"
        project.mkdir()
        (project / MANIFEST_FILE_NAME).write_text(
            "manifest_version: 2\ntype: dcm_project\n"
        )
        (project / "data[1].csv").write_text("x")
        bundle = tmp_path / "bundle"
        bundle.mkdir()

        plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
            project, "@stage", assets=[DCMAsset(name="a", paths=["data[1].csv"])]
        )
        DCMProjectManager._bundle_definition_files(  # noqa: SLF001
            project_root=project, bundle_root=bundle, artifacts=plan.artifacts
        )

        assert (bundle / "data[1].csv").is_file()

    def test_asset_glob_matching_nothing_is_skipped(self, tmp_path):
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "seed.csv").write_text("x")
        with mock.patch("snowflake.cli._plugins.dcm.manager.cli_console.warning"):
            plan = DCMProjectManager._build_upload_plan(  # noqa: SLF001
                tmp_path,
                "@stage",
                assets=[DCMAsset(name="missing", paths=["nope/*.sql"])],
            )
        # no matching files -> nothing added beyond the manifest, no error
        assert [a.src for a in plan.artifacts] == [MANIFEST_FILE_NAME]

    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put")
    @mock.patch(
        "snowflake.cli._plugins.dcm.manager.DCMProjectManager._bundle_definition_files"
    )
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_upload_summary_renders_under_the_step_row(
        self,
        _mock_create_stage,
        _mock_bundle,
        _mock_put,
        mock_put_recursive,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        # given
        mock_put_recursive.return_value = iter([])
        source_dir = tmp_path / "project_with_summary"
        source_dir.mkdir()
        with open(source_dir / MANIFEST_FILE_NAME, "w") as f:
            yaml.dump({"manifest_version": 2, "type": "dcm_project"}, f)
        definitions_dir = source_dir / SOURCES_FOLDER / "definitions"
        definitions_dir.mkdir(parents=True)
        (definitions_dir / "a.sql").touch()
        (definitions_dir / "b.sql").touch()

        progress = MultiStepProgress([StepDefinition("upload", "UPLOAD")])

        # when
        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT,
            source_directory=str(source_dir),
            progress=progress.step_progress_updater("upload"),
        )

        # then: the widget indents every detail component by two. Rich pads the
        # tree's rows to its own width, which is invisible on screen and already
        # stripped on the printed path, so compare without it.
        lines = [
            line.rstrip() for line in capture_rendered(progress).split("\n") if line
        ]
        assert lines == [
            mock.ANY,
            f"  {DETAIL_BULLET}Create temporary stage inside "
            f"{mock_from_resource.return_value.prefix}",
            f"  {DETAIL_BULLET}Upload files",
            f"    ├─ {MANIFEST_FILE_NAME}",
            f"    └─ {SOURCES_FOLDER}",
            "       └─ definitions (2 files)",
        ]
        assert "UPLOAD" in lines[0]

    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.put")
    @mock.patch(
        "snowflake.cli._plugins.dcm.manager.DCMProjectManager._bundle_definition_files"
    )
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_bundles_given_assets(
        self,
        _mock_create_stage,
        mock_bundle,
        _mock_put,
        mock_put_recursive,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        # sync_local_files bundles the assets it is handed by the caller (the
        # command resolves them from the manifest via TargetContext).
        mock_put_recursive.return_value = iter([])
        source_dir = tmp_path / "proj"
        (source_dir / "data").mkdir(parents=True)
        (source_dir / "data" / "seed.csv").write_text("x")
        (source_dir / MANIFEST_FILE_NAME).write_text(
            "manifest_version: 2\ntype: dcm_project\n"
        )

        progress = MultiStepProgress([StepDefinition("upload", "UPLOAD")])
        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT,
            source_directory=str(source_dir),
            progress=progress.step_progress_updater("upload"),
            assets=[DCMAsset(name="seeds", paths=["data/*.csv"])],
        )

        mock_bundle.assert_called_once()
        srcs = [a.src for a in mock_bundle.call_args.kwargs["artifacts"]]
        assert MANIFEST_FILE_NAME in srcs
        # the glob is resolved to concrete files before bundling
        assert "data/seed.csv" in srcs
