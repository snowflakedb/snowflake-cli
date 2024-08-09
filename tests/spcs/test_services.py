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
from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock, patch

import pytest
from click import ClickException
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli._plugins.spcs.common import NoPropertiesProvidedError
from snowflake.cli._plugins.spcs.services.commands import _service_name_callback
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_string_literal
from snowflake.connector.cursor import SnowflakeCursor
from yaml import YAMLError

from tests.spcs.test_common import SPCS_OBJECT_EXISTS_ERROR

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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_create_service(mock_execute_query, other_directory):
    service_name = "test_service"
    compute_pool = "test_pool"
    min_instances = 42
    max_instances = 43
    tmp_dir = Path(other_directory)
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
def test_create_service_cli_defaults(mock_create, other_directory, runner):
    tmp_dir = Path(other_directory)
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
def test_create_service_cli(mock_create, other_directory, runner):
    tmp_dir = Path(other_directory)
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
@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_create_service_if_not_exists(mock_execute_query, other_directory):
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    tmp_dir = Path(other_directory)
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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_execute_job_service(mock_execute_query, other_directory):
    job_service_name = "test_job_service"
    compute_pool = "test_pool"
    tmp_dir = Path(other_directory)
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
def test_execute_job_service_cli_defaults(mock_execute_job, other_directory, runner):
    tmp_dir = Path(other_directory)
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
def test_execute_job_service_cli(mock_execute_job, other_directory, runner):
    tmp_dir = Path(other_directory)
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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_status(mock_execute_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().status(service_name)
    expected_query = "CALL SYSTEM$GET_SERVICE_STATUS('test_service')"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_status_qualified_name(mock_execute_query):
    service_name = "db.schema.test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().status(service_name)
    expected_query = f"CALL SYSTEM$GET_SERVICE_STATUS('{service_name}')"
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_logs(mock_execute_query):
    service_name = "test_service"
    container_name = "test_container"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().logs(service_name, "10", container_name, 42)
    expected_query = (
        "call SYSTEM$GET_SERVICE_LOGS('test_service', '10', 'test_container', 42);"
    )
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


def test_read_yaml(other_directory):
    tmp_dir = Path(other_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    result = ServiceManager()._read_yaml(spec_path)  # noqa: SLF001
    assert result == json.dumps(SPEC_DICT)


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_upgrade_spec(mock_execute_query, other_directory):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    tmp_dir = Path(other_directory)
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
def test_upgrade_spec_cli(mock_upgrade_spec, mock_cursor, runner, other_directory):
    cursor = mock_cursor(rows=[["Statement executed successfully"]], columns=["status"])
    mock_upgrade_spec.return_value = cursor
    service_name = "test_service"
    tmp_dir = Path(other_directory)
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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
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


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
def test_set_property(mock_execute_query):
    service_name = "test_service"
    min_instances = 2
    max_instances = 3
    query_warehouse = "test_warehouse"
    auto_resume = False
    comment = to_string_literal("this is a test")
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_query.return_value = cursor
    result = ServiceManager().set_property(
        service_name=service_name,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        comment=comment,
    )
    expected_query = "\n".join(
        [
            f"alter service {service_name} set",
            f"min_instances = {min_instances}",
            f"max_instances = {max_instances}",
            f"query_warehouse = {query_warehouse}",
            f"auto_resume = {auto_resume}",
            f"comment = {comment}",
        ]
    )
    mock_execute_query.assert_called_once_with(expected_query)
    assert result == cursor


def test_set_property_no_properties():
    service_name = "test_service"
    with pytest.raises(NoPropertiesProvidedError) as e:
        ServiceManager().set_property(service_name, None, None, None, None, None)
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
        comment=None,
    )


@patch("snowflake.cli._plugins.spcs.services.manager.ServiceManager._execute_query")
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
