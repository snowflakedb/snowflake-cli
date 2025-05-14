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
import itertools
import json
import re
import tempfile
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest.mock import Mock, call, patch

import pytest
from click import ClickException
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli._plugins.spcs.common import NoPropertiesProvidedError
from snowflake.cli._plugins.spcs.services.commands import _service_name_callback
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_string_literal
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor
from yaml import YAMLError

from tests.spcs.test_common import SPCS_OBJECT_EXISTS_ERROR
from tests_common import change_directory

SPEC_CONTENT = dedent(
    """
    spec:
        containers:
        - name: cloudbeaver
          image: /spcs_demos_db/cloudbeaver:23.2.1
        endpoints:
        - name: cloudbeaver
          port: 80
          public: true

    """
)

SPEC_DICT = {
    "spec": {
        "containers": [
            {"name": "cloudbeaver", "image": "/spcs_demos_db/cloudbeaver:23.2.1"},
        ],
        "endpoints": [{"name": "cloudbeaver", "port": 80, "public": True}],
    }
}


EXECUTE_QUERY = (
    "snowflake.cli._plugins.spcs.services.manager.ServiceManager.execute_query"
)


@pytest.fixture()
def enable_events_and_metrics_config():
    with TemporaryDirectory() as tempdir:
        config_toml = Path(tempdir) / "config.toml"
        config_toml.write_text(
            "[cli.features]\n"
            "enable_spcs_service_events = true\n"
            "enable_spcs_service_metrics = true\n"
        )
        yield config_toml


@patch(EXECUTE_QUERY)
def test_create_service(mock_execute_query, temporary_directory):
    service_name = "test_service"
    compute_pool = "test_pool"
    min_instances = 42
    max_instances = 43
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    auto_resume = True
    external_access_integrations = [
        "google_apis_access_integration",
        "salesforce_api_access_integration",
    ]
    query_warehouse = "test_warehouse"
    tags = [Tag("test_tag", "test value"), Tag("key", "value")]
    comment = "'user\\'s comment'"

    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor

    result = ServiceManager().create(
        service_name=service_name,
        compute_pool=compute_pool,
        spec_path=Path(spec_path),
        min_instances=min_instances,
        max_instances=max_instances,
        auto_resume=auto_resume,
        external_access_integrations=external_access_integrations,
        query_warehouse=query_warehouse,
        tags=tags,
        comment=comment,
        if_not_exists=False,
    )
    expected_query = " ".join(
        [
            "CREATE SERVICE test_service",
            "IN COMPUTE POOL test_pool",
            f"FROM SPECIFICATION $$ {json.dumps(SPEC_DICT)} $$",
            "MIN_INSTANCES = 42 MAX_INSTANCES = 43",
            "AUTO_RESUME = True",
            "EXTERNAL_ACCESS_INTEGRATIONS = (google_apis_access_integration,salesforce_api_access_integration)",
            "QUERY_WAREHOUSE = test_warehouse",
            "COMMENT = 'user\\'s comment'",
            "WITH TAG (test_tag='test value',key='value')",
        ]
    )
    actual_query = " ".join(mock_execute_query.mock_calls[0].args[0].split())
    assert expected_query == actual_query
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.create")
def test_create_service_cli_defaults(mock_create, temporary_directory, runner):
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = runner.invoke(
        [
            "spcs",
            "service",
            "create",
            "test_service",
            "--compute-pool",
            "test_pool",
            "--spec-path",
            f"{spec_path}",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_create.assert_called_once_with(
        service_name="test_service",
        compute_pool="test_pool",
        spec_path=spec_path,
        min_instances=1,
        max_instances=1,
        auto_resume=True,
        external_access_integrations=None,
        query_warehouse=None,
        tags=None,
        comment=None,
        if_not_exists=False,
    )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.create")
def test_create_service_cli(mock_create, temporary_directory, runner):
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = runner.invoke(
        [
            "spcs",
            "service",
            "create",
            "test_service",
            "--compute-pool",
            "test_pool",
            "--spec-path",
            f"{spec_path}",
            "--min-instances",
            "42",
            "--max-instances",
            "43",
            "--no-auto-resume",
            "--eai-name",
            "google_api",
            "--eai-name",
            "salesforce_api",
            "--query-warehouse",
            "test_warehouse",
            "--tag",
            "name=value",
            "--tag",
            '"$trange name"=normal value',
            "--comment",
            "this is a test",
            "--if-not-exists",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_create.assert_called_once_with(
        service_name="test_service",
        compute_pool="test_pool",
        spec_path=spec_path,
        min_instances=42,
        max_instances=43,
        auto_resume=False,
        external_access_integrations=["google_api", "salesforce_api"],
        query_warehouse="test_warehouse",
        tags=[Tag("name", "value"), Tag('"$trange name"', "normal value")],
        comment=to_string_literal("this is a test"),
        if_not_exists=True,
    )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._read_yaml")
def test_create_service_with_invalid_spec(mock_read_yaml):
    service_name = "test_service"
    compute_pool = "test_pool"
    spec_path = "/path/to/spec.yaml"
    min_instances = 42
    max_instances = 42
    external_access_integrations = query_warehouse = tags = comment = None
    auto_resume = False
    mock_read_yaml.side_effect = YAMLError("Invalid YAML")

    with pytest.raises(YAMLError):
        ServiceManager().create(
            service_name=service_name,
            compute_pool=compute_pool,
            spec_path=Path(spec_path),
            min_instances=min_instances,
            max_instances=max_instances,
            auto_resume=auto_resume,
            external_access_integrations=external_access_integrations,
            query_warehouse=query_warehouse,
            tags=tags,
            comment=comment,
            if_not_exists=False,
        )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._read_yaml")
@patch(EXECUTE_QUERY)
@patch("snowflake.cli._plugins.spcs.services.manager.handle_object_already_exists")
def test_create_service_already_exists(mock_handle, mock_execute, mock_read_yaml):
    service_name = "test_service"
    compute_pool = "test_pool"
    spec_path = "/path/to/spec.yaml"
    min_instances = 42
    max_instances = 42
    external_access_integrations = query_warehouse = tags = comment = None
    auto_resume = False
    mock_execute.side_effect = SPCS_OBJECT_EXISTS_ERROR
    ServiceManager().create(
        service_name=service_name,
        compute_pool=compute_pool,
        spec_path=Path(spec_path),
        min_instances=min_instances,
        max_instances=max_instances,
        auto_resume=auto_resume,
        external_access_integrations=external_access_integrations,
        query_warehouse=query_warehouse,
        tags=tags,
        comment=comment,
        if_not_exists=False,
    )
    mock_handle.assert_called_once_with(
        SPCS_OBJECT_EXISTS_ERROR, ObjectType.SERVICE, service_name
    )


@patch(EXECUTE_QUERY)
def test_create_service_if_not_exists(mock_execute_query, temporary_directory):
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = ServiceManager().create(
        service_name="test_service",
        compute_pool="test_pool",
        spec_path=spec_path,
        min_instances=1,
        max_instances=1,
        auto_resume=True,
        external_access_integrations=None,
        query_warehouse=None,
        tags=None,
        comment=None,
        if_not_exists=True,
    )
    expected_query = " ".join(
        [
            "CREATE SERVICE IF NOT EXISTS test_service",
            "IN COMPUTE POOL test_pool",
            f"FROM SPECIFICATION $$ {json.dumps(SPEC_DICT)} $$",
            "MIN_INSTANCES = 1 MAX_INSTANCES = 1",
            "AUTO_RESUME = True",
        ]
    )
    actual_query = " ".join(mock_execute_query.mock_calls[0].args[0].split())
    assert expected_query == actual_query
    assert result == cursor


def test_deploy_command_requires_pdf(runner):
    with tempfile.TemporaryDirectory() as tmpdir:
        with change_directory(tmpdir):
            result = runner.invoke(["spcs", "service", "deploy"])
            assert result.exit_code == 1
            assert "Cannot find project definition (snowflake.yml)." in result.output


@patch("snowflake.cli._plugins.stage.manager.StageManager.execute_query")
@patch(EXECUTE_QUERY)
def test_deploy_service(
    mock_execute_query,
    mock_stage_manager_execute_query,
    runner,
    project_directory,
    mock_cursor,
    os_agnostic_snapshot,
):
    mock_execute_query.return_value = mock_cursor(
        rows=[["Service TEST_SERVICE successfully created."]],
        columns=["status"],
    )

    with project_directory("spcs_service") as tmp_dir:
        result = runner.invoke(["spcs", "service", "deploy"])

        expected_query = dedent(
            """\
        CREATE SERVICE test_service
        IN COMPUTE POOL test_compute_pool
        FROM @test_stage
        SPECIFICATION_FILE = 'spec.yml'
        AUTO_RESUME = False
        MIN_INSTANCES = 1
        MAX_INSTANCES = 2
        QUERY_WAREHOUSE = xsmall
        EXTERNAL_ACCESS_INTEGRATIONS = (test_external_access_integration)
        COMMENT = 'This is a test service'
        WITH TAG (test_tag='test_value')"""
        )
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_called_once_with(expected_query)
        mock_stage_manager_execute_query.assert_has_calls(
            [
                call("create stage if not exists IDENTIFIER('test_stage')"),
                call(
                    f"put file://{Path(tmp_dir).resolve() / 'output' / 'bundle' / 'service' / 'spec.yml'} @test_stage auto_compress=false parallel=4 overwrite=True",
                    cursor_class=SnowflakeCursor,
                ),
            ]
        )
        assert not (tmp_dir / "output").exists()


@patch("snowflake.cli._plugins.stage.manager.StageManager.execute_query")
@patch(EXECUTE_QUERY)
def test_deploy_service_with_upgrade(
    mock_execute_query,
    mock_stage_manager_execute_query,
    runner,
    project_directory,
    mock_cursor,
    os_agnostic_snapshot,
):
    mock_execute_query.return_value = mock_cursor(
        rows=[["Statement completed successfully."]],
        columns=["status"],
    )

    with project_directory("spcs_service") as tmp_dir:
        result = runner.invoke(["spcs", "service", "deploy", "--upgrade"])

        expected_params_query = dedent(
            """\
            alter service test_service set
            min_instances = 1
            max_instances = 2
            query_warehouse = xsmall
            auto_resume = False
            external_access_integrations = (test_external_access_integration)
            comment = 'This is a test service'"""
        )
        expected_spec_query = dedent(
            """\
        ALTER SERVICE test_service
        FROM @test_stage
        SPECIFICATION_FILE = 'spec.yml'"""
        )
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_has_calls(
            [
                call(expected_params_query),
                call(expected_spec_query),
            ]
        )
        mock_stage_manager_execute_query.assert_has_calls(
            [
                call("create stage if not exists IDENTIFIER('test_stage')"),
                call(
                    f"put file://{Path(tmp_dir).resolve() / 'output' / 'bundle' / 'service' / 'spec.yml'} @test_stage auto_compress=false parallel=4 overwrite=True",
                    cursor_class=SnowflakeCursor,
                ),
            ]
        )


@patch("snowflake.cli._plugins.stage.manager.StageManager.execute_query")
@patch(EXECUTE_QUERY)
def test_deploy_service_already_exists(
    mock_execute_query,
    mock_stage_manager_execute_query,
    runner,
    project_directory,
    mock_cursor,
    os_agnostic_snapshot,
):
    mock_execute_query.return_value = mock_cursor(
        rows=[["Service TEST_SERVICE successfully created."]],
        columns=["status"],
    )
    mock_execute_query.side_effect = ProgrammingError(
        errno=2002, msg="Object 'test_service' already exists."
    )

    with project_directory("spcs_service") as tmp_dir:
        result = runner.invoke(["spcs", "service", "deploy"])

        expected_query = dedent(
            """\
        CREATE SERVICE test_service
        IN COMPUTE POOL test_compute_pool
        FROM @test_stage
        SPECIFICATION_FILE = 'spec.yml'
        AUTO_RESUME = False
        MIN_INSTANCES = 1
        MAX_INSTANCES = 2
        QUERY_WAREHOUSE = xsmall
        EXTERNAL_ACCESS_INTEGRATIONS = (test_external_access_integration)
        COMMENT = 'This is a test service'
        WITH TAG (test_tag='test_value')"""
        )
        assert result.exit_code == 1, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_called_once_with(expected_query)
        mock_stage_manager_execute_query.assert_has_calls(
            [
                call("create stage if not exists IDENTIFIER('test_stage')"),
                call(
                    f"put file://{Path(tmp_dir).resolve() / 'output' / 'bundle' / 'service' / 'spec.yml'} @test_stage auto_compress=false parallel=4 overwrite=True",
                    cursor_class=SnowflakeCursor,
                ),
            ]
        )


def test_deploy_no_service(runner, project_directory, mock_cursor):
    with project_directory("empty_project"):
        result = runner.invoke(["spcs", "service", "deploy"])

        assert result.exit_code == 1, result.output
        assert "No service project definition found in" in result.output


def test_deploy_not_existing_entity_id(runner, project_directory, os_agnostic_snapshot):
    with project_directory("spcs_service"):
        result = runner.invoke(["spcs", "service", "deploy", "not_existing_entity_id"])

        assert result.exit_code == 2, result.output
        assert result.output == os_agnostic_snapshot


@patch("snowflake.cli._plugins.stage.manager.StageManager.execute_query")
@patch(EXECUTE_QUERY)
def test_deploy_multiple_services(
    mock_execute_query,
    mock_stage_manager_execute_query,
    runner,
    project_directory,
    mock_cursor,
    os_agnostic_snapshot,
):
    mock_execute_query.return_value = mock_cursor(
        rows=[["Service TEST_SERVICE successfully created."]],
        columns=["status"],
    )

    with project_directory("spcs_multiple_services") as tmp_dir:
        result = runner.invoke(["spcs", "service", "deploy", "test_service"])

        expected_query = dedent(
            """\
        CREATE SERVICE test_service
        IN COMPUTE POOL test_compute_pool
        FROM @test_stage
        SPECIFICATION_FILE = 'spec.yml'
        AUTO_RESUME = True
        MIN_INSTANCES = 1
        MAX_INSTANCES = 2
        QUERY_WAREHOUSE = xsmall
        EXTERNAL_ACCESS_INTEGRATIONS = (test_external_access_integration)
        COMMENT = 'This is a test service'
        WITH TAG (test_tag='test_value')"""
        )
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_called_once_with(expected_query)
        mock_stage_manager_execute_query.assert_has_calls(
            [
                call("create stage if not exists IDENTIFIER('test_stage')"),
                call(
                    f"put file://{Path(tmp_dir).resolve() / 'output' / 'bundle' / 'service' / 'spec.yml'} @test_stage auto_compress=false parallel=4 overwrite=True",
                    cursor_class=SnowflakeCursor,
                ),
            ]
        )


def test_deploy_multiple_services_without_entity_id(
    runner,
    project_directory,
    os_agnostic_snapshot,
):
    with project_directory("spcs_multiple_services"):
        result = runner.invoke(["spcs", "service", "deploy"])

        assert result.exit_code == 2, result.output
        assert result.output == os_agnostic_snapshot


@patch("snowflake.cli._plugins.stage.manager.StageManager.execute_query")
@patch(EXECUTE_QUERY)
def test_deploy_only_required_fields(
    mock_execute_query,
    mock_stage_manager_execute_query,
    runner,
    mock_cursor,
    project_directory,
    os_agnostic_snapshot,
):
    mock_execute_query.return_value = mock_cursor(
        rows=[["Service TEST_SERVICE successfully created."]],
        columns=["status"],
    )

    with project_directory("spcs_service_only_required"):
        result = runner.invoke(["spcs", "service", "deploy"])

        expected_query = dedent(
            """\
        CREATE SERVICE test_service
        IN COMPUTE POOL test_compute_pool
        FROM @test_stage
        SPECIFICATION_FILE = 'spec.yml'
        AUTO_RESUME = True
        MIN_INSTANCES = 1
        MAX_INSTANCES = 1"""
        )
        assert result.exit_code == 0, result.output
        assert result.output == os_agnostic_snapshot
        mock_execute_query.assert_called_once_with(expected_query)


@patch(EXECUTE_QUERY)
def test_execute_job_service(mock_execute_query, temporary_directory):
    job_service_name = "test_job_service"
    compute_pool = "test_pool"
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    external_access_integrations = [
        "google_apis_access_integration",
        "salesforce_api_access_integration",
    ]
    query_warehouse = "test_warehouse"
    comment = "'user\\'s comment'"

    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor

    result = ServiceManager().execute_job(
        job_service_name=job_service_name,
        compute_pool=compute_pool,
        spec_path=Path(spec_path),
        external_access_integrations=external_access_integrations,
        query_warehouse=query_warehouse,
        comment=comment,
    )
    expected_query = " ".join(
        [
            "EXECUTE JOB SERVICE",
            "IN COMPUTE POOL test_pool",
            f"FROM SPECIFICATION $$ {json.dumps(SPEC_DICT)} $$",
            "NAME = test_job_service",
            "EXTERNAL_ACCESS_INTEGRATIONS = (google_apis_access_integration,salesforce_api_access_integration)",
            "QUERY_WAREHOUSE = test_warehouse",
            "COMMENT = 'user\\'s comment'",
        ]
    )
    actual_query = " ".join(mock_execute_query.mock_calls[0].args[0].split())
    assert expected_query == actual_query
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.execute_job")
def test_execute_job_service_cli_defaults(
    mock_execute_job, temporary_directory, runner
):
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = runner.invoke(
        [
            "spcs",
            "service",
            "execute-job",
            "test_job_service",
            "--compute-pool",
            "test_pool",
            "--spec-path",
            f"{spec_path}",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_execute_job.assert_called_once_with(
        job_service_name="test_job_service",
        compute_pool="test_pool",
        spec_path=spec_path,
        external_access_integrations=None,
        query_warehouse=None,
        comment=None,
    )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.execute_job")
def test_execute_job_service_cli(mock_execute_job, temporary_directory, runner):
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = runner.invoke(
        [
            "spcs",
            "service",
            "execute-job",
            "test_job_service",
            "--compute-pool",
            "test_pool",
            "--spec-path",
            f"{spec_path}",
            "--eai-name",
            "google_api",
            "--eai-name",
            "salesforce_api",
            "--query-warehouse",
            "test_warehouse",
            "--comment",
            "this is a test",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_execute_job.assert_called_once_with(
        job_service_name="test_job_service",
        compute_pool="test_pool",
        spec_path=spec_path,
        external_access_integrations=["google_api", "salesforce_api"],
        query_warehouse="test_warehouse",
        comment=to_string_literal("this is a test"),
    )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._read_yaml")
def test_execute_job_service_with_invalid_spec(mock_read_yaml):
    job_service_name = "test_job_service"
    compute_pool = "test_pool"
    spec_path = "/path/to/spec.yaml"
    external_access_integrations = query_warehouse = comment = None
    mock_read_yaml.side_effect = YAMLError("Invalid YAML")

    with pytest.raises(YAMLError):
        ServiceManager().execute_job(
            job_service_name=job_service_name,
            compute_pool=compute_pool,
            spec_path=Path(spec_path),
            external_access_integrations=external_access_integrations,
            query_warehouse=query_warehouse,
            comment=comment,
        )


@patch(EXECUTE_QUERY)
def test_status(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().status(service_name)
    expected_query = "CALL SYSTEM$GET_SERVICE_STATUS('test_service')"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch(EXECUTE_QUERY)
def test_status_qualified_name(mock_execute_query):
    service_name = "db.schema.test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().status(service_name)
    expected_query = f"CALL SYSTEM$GET_SERVICE_STATUS('{service_name}')"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch(EXECUTE_QUERY)
def test_logs(mock_execute_query):
    service_name = "test_service"
    container_name = "test_container"
    instance_id = "10"
    num_lines = 42

    # Test case 1: Without since_timestamp
    cursor = Mock(spec=SnowflakeCursor)
    cursor.fetchall.return_value = [("log_line_1",), ("log_line_2",)]
    mock_execute_query.return_value = cursor

    service_manager = ServiceManager()
    result_generator = service_manager.logs(
        service_name, instance_id, container_name, num_lines
    )
    result = list(result_generator)

    expected_query_1 = f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '{instance_id}', '{container_name}', {num_lines}, False, '', False);"
    expected_output = ["log_line_1", "log_line_2"]

    mock_execute_query.assert_has_calls([call(expected_query_1)])
    assert result == expected_output
    mock_execute_query.reset_mock()

    # Test case 2: With real-time since_timestamp
    since_timestamp = datetime.utcnow().isoformat() + "Z"
    result_generator = service_manager.logs(
        service_name,
        instance_id,
        container_name,
        num_lines,
        since_timestamp=since_timestamp,
    )
    result = list(result_generator)

    expected_query_2 = f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '{instance_id}', '{container_name}', {num_lines}, False, '{since_timestamp}', False);"
    expected_output = ["log_line_1", "log_line_2"]

    # Assertions for Test Case 2
    mock_execute_query.assert_has_calls([call(expected_query_2)])
    assert result == expected_output
    mock_execute_query.reset_mock()

    # Test case 3: With previous_logs=True
    previous_logs = True
    cursor.fetchall.return_value = [("previous_log_line_1",), ("previous_log_line_2",)]
    mock_execute_query.return_value = cursor

    result_generator = service_manager.logs(
        service_name,
        instance_id,
        container_name,
        num_lines,
        previous_logs=previous_logs,
    )
    result = list(result_generator)

    expected_query_3 = f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '{instance_id}', '{container_name}', {num_lines}, True, '', False);"
    expected_output = ["previous_log_line_1", "previous_log_line_2"]

    mock_execute_query.assert_has_calls([call(expected_query_3)])
    assert result == expected_output


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.logs")
@patch("time.sleep")
def test_stream_logs_with_include_timestamps_false(mock_sleep, mock_logs):
    service_name = "test_service"
    instance_id = "10"
    container_name = "test_container"
    num_lines = 0
    since_timestamp = ""
    include_timestamps = False
    interval_seconds = 1

    mock_logs.side_effect = itertools.chain(
        [
            iter(["2024-10-22T01:12:28Z log 1", "2024-10-22T01:12:28Z log 2"]),
            iter(["2024-10-22T01:12:28Z log 2", "2024-10-22T01:12:29Z log 3"]),
            iter(
                [
                    "2024-10-22T01:12:29Z log 3",
                    "2024-10-22T01:12:30Z log 4",
                    "2024-10-22T01:12:29Z log 3",
                ]
            ),
        ],
        itertools.cycle([iter([])]),
    )

    service_manager = ServiceManager()
    generated_logs = []

    for log in service_manager.stream_logs(
        service_name=service_name,
        instance_id=instance_id,
        container_name=container_name,
        num_lines=num_lines,
        since_timestamp=since_timestamp,
        include_timestamps=include_timestamps,
        interval_seconds=interval_seconds,
    ):
        if log is not None:
            generated_logs.append(log)
        if len(generated_logs) >= 5:
            break

    assert generated_logs == [
        "log 1",
        "log 2",
        "log 3",
        "log 3",
        "log 4",
    ]
    assert mock_logs.call_count == 3
    assert mock_sleep.call_count == 2
    mock_sleep.assert_has_calls([call(interval_seconds), call(interval_seconds)])


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.logs")
@patch("time.sleep")
def test_stream_logs_with_include_timestamps_true(mock_sleep, mock_logs):
    service_name = "test_service"
    instance_id = "10"
    container_name = "test_container"
    num_lines = 0
    since_timestamp = ""
    include_timestamps = True
    interval_seconds = 1

    mock_logs.side_effect = itertools.chain(
        [
            iter(["2024-10-22T01:12:28Z log 1", "2024-10-22T01:12:28Z log 2"]),
            iter(["2024-10-22T01:12:28Z log 2", "2024-10-22T01:12:29Z log 3"]),
            iter(
                [
                    "2024-10-22T01:12:29Z log 3",
                    "2024-10-22T01:12:30Z log 4",
                    "2024-10-22T01:12:29Z log 3",
                ]
            ),
        ],
        itertools.cycle([iter([])]),
    )

    service_manager = ServiceManager()
    generated_logs = []
    for log in service_manager.stream_logs(
        service_name=service_name,
        instance_id=instance_id,
        container_name=container_name,
        num_lines=num_lines,
        since_timestamp=since_timestamp,
        include_timestamps=include_timestamps,
        interval_seconds=interval_seconds,
    ):
        if log is not None:
            generated_logs.append(log)
        if len(generated_logs) >= 5:
            break

    # Expect the output to include timestamps
    assert generated_logs == [
        "2024-10-22T01:12:28Z log 1",
        "2024-10-22T01:12:28Z log 2",
        "2024-10-22T01:12:29Z log 3",
        "2024-10-22T01:12:29Z log 3",
        "2024-10-22T01:12:30Z log 4",
    ]
    assert mock_logs.call_count == 3
    assert mock_sleep.call_count == 2
    mock_sleep.assert_has_calls([call(interval_seconds), call(interval_seconds)])


@patch(EXECUTE_QUERY)
def test_logs_incompatible_flags(
    mock_execute_query, runner, enable_events_and_metrics_config
):
    result = runner.invoke(
        [
            "spcs",
            "service",
            "logs",
            "test_service",
            "--container-name",
            "test_container",
            "--instance-id",
            "0",
            "--follow",
            "--num-lines",
            "100",
        ],
    )
    assert (
        result.exit_code != 0
    ), "Expected a non-zero exit code due to incompatible flags"
    assert "Parameters '--follow' and '--num-lines' are incompatible" in result.output


@patch(EXECUTE_QUERY)
def test_logs_incompatible_flags_follow_previous_logs(mock_execute_query, runner):
    result = runner.invoke(
        [
            "spcs",
            "service",
            "logs",
            "test_service",
            "--container-name",
            "test_container",
            "--instance-id",
            "0",
            "--follow",
            "--previous-logs",
        ]
    )

    assert (
        result.exit_code != 0
    ), "Expected a non-zero exit code due to incompatible flags"

    assert (
        "Parameters '--follow' and '--previous-logs' are incompatible" in result.output
    )


def test_logs_streaming_flag_is_hidden(runner):
    result = runner.invoke(["spcs", "service", "logs", "--help"])
    assert result.exit_code == 0
    assert "--follow" not in result.output


@patch(EXECUTE_QUERY)
def test_events_all_filters(
    mock_execute_query, runner, enable_events_and_metrics_config
):
    mock_execute_query.side_effect = [
        [
            {
                "key": "EVENT_TABLE",
                "value": "event_table_db.data_schema.snowservices_logs",
            }
        ],
        Mock(
            fetchall=lambda: [
                (
                    "2024-12-14 22:27:25.420",
                    None,
                    "2024-12-14 22:27:25.420",
                    None,
                    None,
                    json.dumps(
                        {
                            "snow.compute_pool.id": 230,
                            "snow.compute_pool.name": "MY_POOL",
                            "snow.database.id": 5,
                            "snow.database.name": "TESTDB",
                            "snow.schema.id": 5,
                            "snow.schema.name": "PUBLIC",
                            "snow.service.container.name": "log-printer",
                            "snow.service.id": 1568,
                            "snow.service.instance": "0",
                            "snow.service.name": "LOG_EVENT",
                            "snow.service.type": "SERVICE",
                        }
                    ),
                    json.dumps({"name": "snow.spcs.platform"}),
                    None,
                    "LOG",
                    json.dumps({"severity_text": "INFO"}),
                    json.dumps({"event.name": "CONTAINER.STATUS_CHANGE"}),
                    json.dumps({"message": "Running", "status": "READY"}),
                    None,
                )
            ]
        ),
    ]

    result = runner.invoke_with_config_file(
        enable_events_and_metrics_config,
        [
            "spcs",
            "service",
            "events",
            "LOG_EVENT",
            "--container-name",
            "log-printer",
            "--instance-id",
            "0",
            "--since",
            "2 hours",
            "--until",
            "1 hour",
            "--last",
            "10",
            "--warehouse",
            "XSMALL",
            "--role",
            "sysadmin",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    call_0 = mock_execute_query.mock_calls[0].args[0]
    assert (
        call_0 == "show parameters like 'event_table' in account"
    ), f"Unexpected query in Call 0: {call_0}"

    actual_query = mock_execute_query.mock_calls[1].args[0]
    expected_query = (
        "                     select *\n"
        "                    from (\n"
        "                        select *\n"
        "                        from event_table_db.data_schema.snowservices_logs\n"
        "                        where (\n"
        "                            resource_attributes:\"snow.service.name\" = 'LOG_EVENT' and (resource_attributes:\"snow.service.instance\" = '0' OR resource_attributes:\"snow.service.container.instance\" = '0') and resource_attributes:\"snow.service.container.name\" = 'log-printer'\n"
        "                            and timestamp >= sysdate() - interval '2 hours'\n"
        "                            and timestamp <= sysdate() - interval '1 hour'\n"
        "                        )\n"
        "                        and record_type = 'LOG'\n"
        "                        and scope['name'] = 'snow.spcs.platform'\n"
        "                        order by timestamp desc\n"
        "                        limit 10\n"
        "                    )\n"
        "                    order by timestamp asc\n"
        "                    \n"
        "                "
    )

    assert (
        actual_query == expected_query
    ), f"Generated query does not match expected query.\n\nActual:\n{actual_query}\n\nExpected:\n{expected_query}"


def test_events_first_last_incompatibility(runner, enable_events_and_metrics_config):
    result = runner.invoke_with_config_file(
        enable_events_and_metrics_config,
        [
            "spcs",
            "service",
            "events",
            "LOG_EVENT",
            "--container-name",
            "log-printer",
            "--instance-id",
            "0",
            "--first",
            "10",
            "--last",
            "5",
            "--warehouse",
            "XSMALL",
            "--role",
            "sysadmin",
        ],
    )

    assert result.exit_code != 0, result.output

    expected_error = "Parameters '--first' and '--last' are incompatible"
    assert expected_error in result.output


@patch(EXECUTE_QUERY)
def test_latest_metrics(
    mock_execute_query, runner, snapshot, enable_events_and_metrics_config
):
    mock_execute_query.side_effect = [
        [
            {
                "key": "EVENT_TABLE",
                "value": "event_table_db.data_schema.snowservices_logs",
            }
        ],
        Mock(
            fetchall=lambda: [
                (
                    datetime(2024, 12, 10, 18, 53, 21, 809000),
                    datetime(2024, 12, 10, 18, 52, 51, 809000),
                    None,
                    None,
                    None,
                    json.dumps(
                        {
                            "snow.account.name": "XACCOUNTTEST1",
                            "snow.compute_pool.id": 20641,
                            "snow.compute_pool.name": "MY_POOL",
                            "snow.service.container.name": "log-printer",
                            "snow.service.name": "LOG_EVENT",
                        }
                    ),
                    json.dumps({"name": "snow.spcs.platform"}),
                    None,
                    "METRIC",
                    json.dumps(
                        {"metric": {"name": "container.cpu.usage", "unit": "cpu"}}
                    ),
                    None,
                    "0.0005007168666666691",
                    None,
                )
            ]
        ),
    ]

    result = runner.invoke_with_config_file(
        enable_events_and_metrics_config,
        [
            "spcs",
            "service",
            "metrics",
            "LOG_EVENT",
            "--container-name",
            "log-printer",
            "--instance-id",
            "0",
            "--warehouse",
            "XSMALL",
            "--role",
            "sysadmin",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"
    assert result.output == snapshot

    call_0 = mock_execute_query.mock_calls[0].args[0]
    assert (
        call_0 == "show parameters like 'event_table' in account"
    ), f"Unexpected query in Call 0: {call_0}"

    actual_query = mock_execute_query.mock_calls[1].args[0]
    expected_query = (
        "\n"
        "            with rankedmetrics as (\n"
        "                select \n"
        "                    *,\n"
        "                    row_number() over (\n"
        "                        partition by record['metric']['name'] \n"
        "                        order by timestamp desc\n"
        "                    ) as rank\n"
        "                from event_table_db.data_schema.snowservices_logs\n"
        "                where \n"
        "                    record_type = 'METRIC'\n"
        "                    and scope['name'] = 'snow.spcs.platform'\n"
        "                    and resource_attributes:\"snow.service.name\" = 'LOG_EVENT' and (resource_attributes:\"snow.service.instance\" = '0' OR resource_attributes:\"snow.service.container.instance\" = '0') and resource_attributes:\"snow.service.container.name\" = 'log-printer'  \n"
        "                    and timestamp > dateadd('hour', -1, current_timestamp)  \n"
        "            )\n"
        "            select *\n"
        "            from rankedmetrics\n"
        "            where rank = 1\n"
        "            order by timestamp desc;\n"
        "        "
    )

    actual_normalized = normalize_query(actual_query)
    expected_normalized = normalize_query(expected_query)

    assert actual_normalized == expected_normalized, (
        f"Generated query does not match expected query.\n\n"
        f"Actual:\n{actual_query}\n\nExpected:\n{expected_query}"
    )


def test_service_events_disabled(runner, config_file):
    with config_file("") as config:
        result = runner.invoke_with_config_file(
            config,
            [
                "spcs",
                "service",
                "events",
                "LOG_EVENT",
                "--container-name",
                "log-printer",
                "--instance-id",
                "0",
                "--since",
                "1 minute",
            ],
        )
    assert (
        result.exit_code != 0
    ), "Expected a non-zero exit code due to feature flag being disabled"
    expected_output = (
        "Usage: root spcs service [OPTIONS] COMMAND [ARGS]...\n"
        "Try 'root spcs service --help' for help.\n"
        "+- Error ----------------------------------------------------------------------+\n"
        "| No such command 'events'.                                                    |\n"
        "+------------------------------------------------------------------------------+\n"
    )
    assert (
        result.output == expected_output
    ), f"Expected formatted output not found: {result.output}"


@patch(EXECUTE_QUERY)
def test_metrics_all_filters(
    mock_execute_query, runner, enable_events_and_metrics_config, snapshot
):
    mock_execute_query.side_effect = [
        [
            {
                "key": "EVENT_TABLE",
                "value": "event_table_db.data_schema.snowservices_logs",
            }
        ],
        Mock(
            fetchall=lambda: [
                (
                    datetime(2024, 12, 10, 18, 53, 21, 809000),
                    datetime(2024, 12, 10, 18, 52, 51, 809000),
                    None,
                    None,
                    None,
                    json.dumps(
                        {
                            "snow.account.name": "XACCOUNTTEST1",
                            "snow.compute_pool.id": 20641,
                            "snow.compute_pool.name": "MY_POOL",
                            "snow.service.container.name": "log-printer",
                            "snow.service.name": "LOG_EVENT",
                        }
                    ),
                    json.dumps({"name": "snow.spcs.platform"}),
                    None,
                    "METRIC",
                    json.dumps(
                        {"metric": {"name": "container.cpu.usage", "unit": "cpu"}}
                    ),
                    None,
                    "0.0005007168666666691",
                    None,
                )
            ]
        ),
    ]

    result = runner.invoke_with_config_file(
        enable_events_and_metrics_config,
        [
            "spcs",
            "service",
            "metrics",
            "LOG_EVENT",
            "--container-name",
            "log-printer",
            "--instance-id",
            "0",
            "--since",
            "2 hour",
            "--until",
            "1 hour",
            "--warehouse",
            "XSMALL",
            "--role",
            "sysadmin",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    call_0 = mock_execute_query.mock_calls[0].args[0]
    assert (
        call_0 == "show parameters like 'event_table' in account"
    ), f"Unexpected query in Call 0: {call_0}"

    actual_query = mock_execute_query.mock_calls[1].args[0]
    assert actual_query == snapshot, actual_query


def test_read_yaml(temporary_directory):
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = ServiceManager()._read_yaml(spec_path)  # noqa: SLF001
    assert result == json.dumps(SPEC_DICT)


@patch(EXECUTE_QUERY)
def test_upgrade_spec(mock_execute_query, temporary_directory):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = ServiceManager().upgrade_spec(service_name, spec_path)
    expected_query = (
        f"alter service {service_name} from specification $$ {json.dumps(SPEC_DICT)} $$"
    )
    actual_query = " ".join(mock_execute_query.mock_calls[0].args[0].split())
    assert expected_query == actual_query
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.upgrade_spec")
def test_upgrade_spec_cli(mock_upgrade_spec, mock_cursor, runner, temporary_directory):
    cursor = mock_cursor(rows=[["Statement executed successfully"]], columns=["status"])
    mock_upgrade_spec.return_value = cursor
    service_name = "test_service"
    tmp_dir = Path(temporary_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)

    result = runner.invoke(
        ["spcs", "service", "upgrade", service_name, "--spec-path", spec_path]
    )

    mock_upgrade_spec.assert_called_once_with(
        service_name=service_name, spec_path=spec_path
    )
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output


@patch(EXECUTE_QUERY)
def test_list_endpoints(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().list_endpoints(service_name)
    expected_query = f"show endpoints in service test_service"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.list_endpoints")
def test_list_endpoints_cli(mock_list_endpoints, mock_cursor, runner):
    service_name = "test_service"
    cursor = mock_cursor(
        rows=[["endpoint", 8000, "HTTP", "true", "test-snowflakecomputing.app"]],
        columns=["name", "port", "protocol", "ingress_enabled", "ingress_url"],
    )
    mock_list_endpoints.return_value = cursor
    result = runner.invoke(["spcs", "service", "list-endpoints", service_name])

    mock_list_endpoints.assert_called_once_with(service_name=service_name)
    assert result.exit_code == 0
    assert "test-snowflakecomputing.app" in result.output


@patch(EXECUTE_QUERY)
def test_list_instances(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().list_instances(service_name)
    expected_query = f"show service instances in service test_service"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.list_instances")
def test_list_instances_cli(mock_list_instances, mock_cursor, runner):
    service_name = "test_service"
    cursor = mock_cursor(
        rows=[["TEST_DB", "TEST_SCHEMA", "TEST_SERVICE", "0", "READY"]],
        columns=[
            "database_name",
            "schema_name",
            "service_name",
            "instance_id",
            "status",
        ],
    )
    mock_list_instances.return_value = cursor
    result = runner.invoke(["spcs", "service", "list-instances", service_name])

    mock_list_instances.assert_called_once_with(service_name=service_name)
    assert result.exit_code == 0
    assert "TEST_SERVICE" in result.output, str(result.output)


@patch(EXECUTE_QUERY)
def test_list_containers(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().list_containers(service_name)
    expected_query = f"show service containers in service test_service"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.list_containers")
def test_list_containers_cli(mock_list_containers, mock_cursor, runner):
    service_name = "test_service"
    cursor = mock_cursor(
        rows=[["TEST_DB", "TEST_SCHEMA", "TEST_SERVICE", "0", "main"]],
        columns=[
            "database_name",
            "schema_name",
            "service_name",
            "instance_id",
            "container_name",
        ],
    )
    mock_list_containers.return_value = cursor
    result = runner.invoke(["spcs", "service", "list-containers", service_name])

    mock_list_containers.assert_called_once_with(service_name=service_name)
    assert result.exit_code == 0
    assert "TEST_SERVICE" in result.output, str(result.output)


@patch(EXECUTE_QUERY)
def test_list_roles(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().list_roles(service_name)
    expected_query = f"show roles in service test_service"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.list_roles")
def test_list_roles_cli(mock_list_roles, mock_cursor, runner):
    service_name = "test_service"
    cursor = mock_cursor(
        rows=[["2024-10-09 16:48:52.980000-07:00", "ALL_ENDPOINTS_USAGE", "None"]],
        columns=["created_on", "name", "comment"],
    )
    mock_list_roles.return_value = cursor
    result = runner.invoke(["spcs", "service", "list-roles", service_name])

    mock_list_roles.assert_called_once_with(service_name=service_name)
    assert result.exit_code == 0
    assert "ALL_ENDPOINTS_USAGE" in result.output, str(result.output)


@patch(EXECUTE_QUERY)
def test_suspend(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().suspend(service_name)
    expected_query = f"alter service {service_name} suspend"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.suspend")
def test_suspend_cli(mock_suspend, mock_cursor, runner):
    service_name = "test_service"
    cursor = mock_cursor(
        rows=[["Statement executed successfully."]], columns=["status"]
    )
    mock_suspend.return_value = cursor
    result = runner.invoke(["spcs", "service", "suspend", service_name])
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output


@patch(EXECUTE_QUERY)
def test_resume(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().resume(service_name)
    expected_query = f"alter service {service_name} resume"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.resume")
def test_resume_cli(mock_resume, mock_cursor, runner):
    service_name = "test_service"
    cursor = mock_cursor(
        rows=[["Statement executed successfully."]], columns=["status"]
    )
    mock_resume.return_value = cursor
    result = runner.invoke(["spcs", "service", "resume", service_name])
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output


@patch(EXECUTE_QUERY)
def test_set_property(mock_execute_query):
    service_name = "test_service"
    min_instances = 2
    max_instances = 3
    query_warehouse = "test_warehouse"
    auto_resume = False
    external_access_integrations = [
        "google_apis_access_integration",
        "salesforce_api_access_integration",
    ]
    comment = to_string_literal("this is a test")
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().set_property(
        service_name=service_name,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        external_access_integrations=external_access_integrations,
        comment=comment,
    )
    eai_list = ",".join(external_access_integrations)
    expected_query = "\n".join(
        [
            f"alter service {service_name} set",
            f"min_instances = {min_instances}",
            f"max_instances = {max_instances}",
            f"query_warehouse = {query_warehouse}",
            f"auto_resume = {auto_resume}",
            f"external_access_integrations = ({eai_list})",
            f"comment = {comment}",
        ]
    )
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


def test_set_property_no_properties():
    service_name = "test_service"
    with pytest.raises(NoPropertiesProvidedError) as e:
        ServiceManager().set_property(service_name, None, None, None, None, None, None)
    assert (
        e.value.message
        == f"No properties specified for service '{service_name}'. Please provide at least one property to set."
    )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.set_property")
def test_set_property_cli(mock_set, mock_statement_success, runner):
    cursor = mock_statement_success()
    mock_set.return_value = cursor
    service_name = "test_service"
    min_instances = 2
    max_instances = 3
    query_warehouse = "test_warehouse"
    auto_resume = False
    external_access_integrations = [
        "google_apis_access_integration",
        "salesforce_api_access_integration",
    ]
    comment = "this is a test"
    result = runner.invoke(
        [
            "spcs",
            "service",
            "set",
            service_name,
            "--min-instances",
            str(min_instances),
            "--max-instances",
            str(max_instances),
            "--query-warehouse",
            query_warehouse,
            "--no-auto-resume",
            "--eai-name",
            "google_apis_access_integration",
            "--eai-name",
            "salesforce_api_access_integration",
            "--comment",
            comment,
        ]
    )
    mock_set.assert_called_once_with(
        service_name=service_name,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        external_access_integrations=external_access_integrations,
        comment=to_string_literal(comment),
    )
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.set_property")
def test_set_property_no_properties_cli(mock_set, runner):
    service_name = "test_service"
    mock_set.side_effect = NoPropertiesProvidedError(
        f"No properties specified for service '{service_name}'. Please provide at least one property to set."
    )
    result = runner.invoke(["spcs", "service", "set", service_name])
    assert result.exit_code == 1, result.output
    assert "No properties specified" in result.output
    mock_set.assert_called_once_with(
        service_name=service_name,
        min_instances=None,
        max_instances=None,
        query_warehouse=None,
        auto_resume=None,
        external_access_integrations=None,
        comment=None,
    )


@patch(EXECUTE_QUERY)
def test_unset_property(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().unset_property(service_name, True, True, True, True, True)
    expected_query = "alter service test_service unset min_instances,max_instances,query_warehouse,auto_resume,comment"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


def test_unset_property_no_properties():
    service_name = "test_service"
    with pytest.raises(NoPropertiesProvidedError) as e:
        ServiceManager().unset_property(service_name, False, False, False, False, False)
    assert (
        e.value.message
        == f"No properties specified for service '{service_name}'. Please provide at least one property to reset to its default value."
    )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.unset_property")
def test_unset_property_cli(mock_unset, mock_statement_success, runner):
    cursor = mock_statement_success()
    mock_unset.return_value = cursor
    service_name = "test_service"
    result = runner.invoke(
        [
            "spcs",
            "service",
            "unset",
            service_name,
            "--min-instances",
            "--max-instances",
            "--query-warehouse",
            "--auto-resume",
            "--comment",
        ]
    )
    mock_unset.assert_called_once_with(
        service_name=service_name,
        min_instances=True,
        max_instances=True,
        query_warehouse=True,
        auto_resume=True,
        comment=True,
    )
    assert result.exit_code == 0, result.output
    assert "Statement executed successfully" in result.output


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager.unset_property")
def test_unset_property_no_properties_cli(mock_unset, runner):
    service_name = "test_service"
    mock_unset.side_effect = NoPropertiesProvidedError(
        f"No properties specified for service '{service_name}'. Please provide at least one property to reset to its default value."
    )
    result = runner.invoke(["spcs", "service", "unset", service_name])
    assert result.exit_code == 1, result.output
    assert "No properties specified" in result.output
    mock_unset.assert_called_once_with(
        service_name=service_name,
        min_instances=False,
        max_instances=False,
        query_warehouse=False,
        auto_resume=False,
        comment=False,
    )


def test_unset_property_with_args(runner):
    service_name = "test_service"
    result = runner.invoke(
        ["spcs", "service", "unset", service_name, "--min-instances", "1"]
    )
    assert result.exit_code == 2, result.output
    assert "Got unexpected extra argument" in result.output


def test_invalid_service_name(runner):
    invalid_service_name = "account.db.schema.name"
    result = runner.invoke(["spcs", "service", "status", invalid_service_name])
    assert result.exit_code == 1
    assert f"'{invalid_service_name}' is not valid" in result.output


@patch("snowflake.cli._plugins.spcs.services.commands.is_valid_object_name")
def test_service_name_parser(mock_is_valid_object_name):
    service_name = "db.schema.test_service"
    mock_is_valid_object_name.return_value = True
    fqn = FQN.from_string(service_name)
    assert _service_name_callback(fqn) == fqn
    mock_is_valid_object_name.assert_called_once_with(
        service_name, max_depth=2, allow_quoted=False
    )


@patch("snowflake.cli._plugins.spcs.services.commands.is_valid_object_name")
def test_service_name_parser_invalid_object_name(mock_is_valid_object_name):
    invalid_service_name = '"db.schema.test_service"'
    mock_is_valid_object_name.return_value = False
    with pytest.raises(ClickException) as e:
        _service_name_callback(FQN.from_string(invalid_service_name))
    assert f"'{invalid_service_name}' is not a valid service name." in e.value.message


@patch("snowflake.connector.connect")
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

    result = runner.invoke(["object", command, "service", *parameters])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["spcs", "service", command, *parameters], catch_exceptions=False
    )
    assert result.exit_code == 0, result.output

    queries = ctx.get_queries()
    assert queries[0] == queries[1]


def normalize_query(query):
    """Normalize SQL query by stripping extra whitespace and formatting."""
    return re.sub(r"\s+", " ", query.strip())
