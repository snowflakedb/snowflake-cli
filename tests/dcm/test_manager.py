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
from pathlib import Path, PureWindowsPath
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dcm.manager import (
    SOURCES_FOLDER,
    DCMProjectManager,
    UploadPlan,
)
from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME
from snowflake.cli._plugins.dcm.multistep_progress import (
    MultiStepProgress,
    StepDefinition,
    StepState,
)
from snowflake.cli._plugins.dcm.progress import DETAIL_BULLET, FileUploadProgress
from snowflake.cli.api.identifiers import FQN

from tests.dcm.multi_step_progress_capture import capture_rendered

execute_queries = "snowflake.cli._plugins.dcm.manager.DCMProjectManager.execute_query"
execute_query_with_params = (
    "snowflake.cli._plugins.dcm.manager.DCMProjectManager.execute_query_with_params"
)
TEST_STAGE = FQN.from_stage("@test_stage")
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
def test_analyze_project_default_no_download(mock_execute_query):
    mgr = DCMProjectManager()

    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE" in query
    assert "OUTPUT_PATH" not in query


@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
@mock.patch(execute_queries)
def test_analyze_project_with_save_output(
    mock_execute_query,
    mock_create,
    mock_get_recursive,
    mock_from_resource,
    project_directory,
):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        save_output=True,
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE" in query
    assert "OUTPUT_PATH" in query
    temp_stage_fqn = mock_from_resource()
    mock_create.assert_called_once_with(temp_stage_fqn, temporary=True)
    mock_get_recursive.assert_called_once_with(
        stage_path=f"@{str(temp_stage_fqn)}/outputs",
        dest_path=Path("out"),
    )


@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
@mock.patch(execute_queries)
def test_analyze_project_with_output_path__exception_handling(
    mock_execute_query,
    mock_create,
    mock_get_recursive,
    project_directory,
    mock_from_resource,
):
    mock_execute_query.side_effect = Exception("Query execution failed")

    mgr = DCMProjectManager()

    with pytest.raises(Exception, match="Query execution failed"):
        mgr.raw_analyze(
            project_identifier=TEST_PROJECT,
            from_stage="@test_stage",
            configuration="some_configuration",
            save_output=True,
        )

    temp_stage_fqn = mock_from_resource()
    mock_execute_query.assert_called_once()
    mock_create.assert_called_once_with(temp_stage_fqn, temporary=True)
    mock_get_recursive.assert_called_once_with(
        stage_path=f"@{str(temp_stage_fqn)}/outputs",
        dest_path=Path("out"),
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


@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
@mock.patch(execute_queries)
@mock.patch(execute_query_with_params)
def test_raw_analyze_project_with_save_output_and_env_vars(
    mock_execute_with_params,
    mock_execute_query,
    mock_create,
    mock_get_recursive,
    mock_from_resource,
    project_directory,
):
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        save_output=True,
        env_vars=env_vars,
    )

    mock_execute_with_params.assert_called_once()
    query = mock_execute_with_params.call_args.kwargs["query"]
    assert "ENVIRONMENT (?)" in query
    assert "OUTPUT_PATH" in query
    assert mock_execute_with_params.call_args.kwargs["params"] == [json.dumps(env_vars)]
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


@mock.patch(execute_queries)
def test_plan_project_default_no_download(mock_execute_query, project_directory):
    mgr = DCMProjectManager()

    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN" in query
    assert "OUTPUT_PATH" not in query


@mock.patch("snowflake.cli._plugins.dcm.manager.FQN.from_resource")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
@mock.patch(execute_queries)
def test_plan_project_with_save_output(
    mock_execute_query,
    mock_create,
    mock_get_recursive,
    project_directory,
):
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        save_output=True,
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN" in query
    assert "OUTPUT_PATH" in query
    mock_get_recursive.assert_called_once()


@mock.patch(execute_queries)
def test_plan_project_with_from_stage(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration"
        " FROM @my_stage"
    )


@mock.patch(execute_queries)
def test_plan_project_with_delta(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        delta=True,
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN DELTA FROM @my_stage"
    )


@mock.patch(execute_queries)
@mock.patch(execute_query_with_params)
def test_plan_project_with_env_vars(mock_execute_with_params, mock_execute_query):
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        env_vars=env_vars,
    )

    mock_execute_with_params.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN ENVIRONMENT (?)"
        " FROM @my_stage",
        params=[json.dumps(env_vars)],
    )
    mock_execute_query.assert_not_called()


@mock.patch(execute_queries)
@mock.patch(execute_query_with_params)
def test_plan_project_with_configuration_variables_and_env_vars(
    mock_execute_with_params, mock_execute_query
):
    # ENVIRONMENT is stacked after USING CONFIGURATION/variables and before FROM --
    # verifies the two clause-building code paths (templating vars vs. env vars)
    # compose correctly instead of one clobbering the other.
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        configuration="some_configuration",
        variables=["key=value"],
        env_vars=env_vars,
    )

    mock_execute_with_params.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION"
        " some_configuration (key=>value) ENVIRONMENT (?) FROM @my_stage",
        params=[json.dumps(env_vars)],
    )
    mock_execute_query.assert_not_called()


@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
@mock.patch(execute_queries)
@mock.patch(execute_query_with_params)
def test_plan_project_with_save_output_and_env_vars(
    mock_execute_with_params,
    mock_execute_query,
    mock_create,
    mock_get_recursive,
    mock_from_resource,
    project_directory,
):
    mgr = DCMProjectManager()
    env_vars = {"WH_SIZE": "XLARGE"}

    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        save_output=True,
        env_vars=env_vars,
    )

    mock_execute_with_params.assert_called_once()
    query = mock_execute_with_params.call_args.kwargs["query"]
    assert "ENVIRONMENT (?)" in query
    assert "OUTPUT_PATH" in query
    assert mock_execute_with_params.call_args.kwargs["params"] == [json.dumps(env_vars)]
    mock_execute_query.assert_not_called()


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
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
def test_plan_project_with_output_path__exception_handling(
    mock_create,
    mock_get_recursive,
    mock_execute_query,
    project_directory,
    mock_from_resource,
):
    mock_execute_query.side_effect = Exception("Query execution failed")

    mgr = DCMProjectManager()

    with pytest.raises(Exception, match="Query execution failed"):
        mgr.plan(
            project_identifier=TEST_PROJECT,
            from_stage="@test_stage",
            configuration="some_configuration",
            save_output=True,
        )

    # But the output should still be downloaded before exception is reraised
    temp_stage_fqn = mock_from_resource()
    mock_execute_query.assert_called_once()
    mock_create.assert_called_once_with(temp_stage_fqn, temporary=True)
    mock_get_recursive.assert_called_once_with(
        stage_path=f"@{str(temp_stage_fqn)}/outputs",
        dest_path=Path("out"),
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
            f"    ├── {MANIFEST_FILE_NAME}",
            f"    └── {SOURCES_FOLDER}",
            "        └── definitions (2 files)",
        ]
        assert "UPLOAD" in lines[0]
