from textwrap import dedent
from unittest.mock import Mock, patch
import json

from tests.spcs.test_common import SPCS_OBJECT_EXISTS_ERROR
from snowflake.cli.plugins.spcs.common import NoPropertiesProvidedError
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.plugins.spcs.services.manager import ServiceManager
from tests.testing_utils.fixtures import *
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.plugins.object.common import Tag
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_executed_successfully,
)
from tests.spcs.utils import assert_mock_execute_is_called_once_with_query

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


@pytest.fixture(scope="function")
def sample_spec_path(other_directory):
    tmp_dir = Path(other_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(SPEC_CONTENT)
    yield spec_path


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_create_service(mock_execute_schema_query, sample_spec_path):
    service_name = "test_service"
    compute_pool = "test_pool"
    min_instances = 42
    max_instances = 43
    auto_resume = True
    external_access_integrations = [
        "google_apis_access_integration",
        "salesforce_api_access_integration",
    ]
    query_warehouse = "test_warehouse"
    tags = [Tag("test_tag", "test value"), Tag("key", "value")]
    comment = "'user\\'s comment'"

    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor

    result = ServiceManager().create(
        service_name=service_name,
        compute_pool=compute_pool,
        spec_path=sample_spec_path,
        min_instances=min_instances,
        max_instances=max_instances,
        auto_resume=auto_resume,
        external_access_integrations=external_access_integrations,
        query_warehouse=query_warehouse,
        tags=tags,
        comment=comment,
    )
    expected_query = f"""
        create service test_service
        in compute pool test_pool
        from specification $$ {json.dumps(SPEC_DICT)} $$
        min_instances = 42 MAX_INSTANCES = 43
        auto_resume = True
        external_access_integrations = (google_apis_access_integration,salesforce_api_access_integration)
        query_warehouse = test_warehouse
        comment = 'user\\'s comment'
        with tag (test_tag='test value',key='value')
    """
    assert_mock_execute_is_called_once_with_query(
        mock_execute_schema_query, expected_query
    )
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.create")
def test_create_service_cli_defaults(mock_create, sample_spec_path, runner):
    result = runner.invoke(
        [
            "spcs",
            "service",
            "create",
            "test_service",
            "--compute-pool",
            "test_pool",
            "--spec-path",
            f"{sample_spec_path}",
        ]
    )
    assert result.exit_code == 0, result.output
    mock_create.assert_called_once_with(
        service_name="test_service",
        compute_pool="test_pool",
        spec_path=sample_spec_path,
        min_instances=1,
        max_instances=1,
        auto_resume=True,
        external_access_integrations=[],
        query_warehouse=None,
        tags=[],
        comment=None,
    )


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.create")
def test_create_service_cli(mock_create, sample_spec_path, runner):
    result = runner.invoke(
        [
            "spcs",
            "service",
            "create",
            "test_service",
            "--compute-pool",
            "test_pool",
            "--spec-path",
            f"{sample_spec_path}",
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
        ]
    )
    assert result.exit_code == 0, result.output
    mock_create.assert_called_once_with(
        service_name="test_service",
        compute_pool="test_pool",
        spec_path=sample_spec_path,
        min_instances=42,
        max_instances=43,
        auto_resume=False,
        external_access_integrations=["google_api", "salesforce_api"],
        query_warehouse="test_warehouse",
        tags=[Tag("name", "value"), Tag('"$trange name"', "normal value")],
        comment=to_string_literal("this is a test"),
    )


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager._read_yaml")
def test_create_service_with_invalid_spec(mock_read_yaml):
    service_name = "test_service"
    compute_pool = "test_pool"
    spec_path = "/path/to/spec.yaml"
    min_instances = 42
    max_instances = 42
    external_access_integrations = query_warehouse = tags = comment = None
    auto_resume = False
    mock_read_yaml.side_effect = strictyaml.YAMLError("Invalid YAML")

    with pytest.raises(strictyaml.YAMLError):
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
        )


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager._read_yaml")
@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
@patch("snowflake.cli.plugins.spcs.services.manager.handle_object_already_exists")
def test_create_repository_already_exists(mock_handle, mock_execute, mock_read_yaml):
    service_name = "test_object"
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
    )
    mock_handle.assert_called_once_with(
        SPCS_OBJECT_EXISTS_ERROR, ObjectType.SERVICE, service_name
    )


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_status(mock_execute_schema_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().status(service_name)
    expected_query = "call system$get_service_status('test_service')"
    mock_execute_schema_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_logs(mock_execute_schema_query):
    service_name = "test_service"
    container_name = "test_container"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().logs(service_name, "10", container_name, 42)
    expected_query = (
        "call system$get_service_logs('test_service', '10', 'test_container', 42);"
    )
    mock_execute_schema_query.assert_called_once_with(expected_query)
    assert result == cursor


def test_read_yaml(sample_spec_path):
    result = ServiceManager()._read_yaml(sample_spec_path)
    assert result == json.dumps(SPEC_DICT)


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_upgrade_spec(mock_execute_schema_query, sample_spec_path):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().upgrade_spec(
        service_name=service_name, spec_path=sample_spec_path
    )
    expected_query = (
        f"alter service {service_name} from specification $$ {json.dumps(SPEC_DICT)} $$"
    )
    mock_execute_schema_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.upgrade_spec")
def test_upgrade_spec_cli(
    mock_upgrade_spec, mock_statement_success, runner, sample_spec_path
):
    mock_upgrade_spec.return_value = mock_statement_success()
    service_name = "test_service"

    result = runner.invoke(
        ["spcs", "service", "upgrade", service_name, "--spec-path", sample_spec_path]
    )

    mock_upgrade_spec.assert_called_once_with(
        service_name=service_name, spec_path=sample_spec_path
    )
    assert_that_result_is_successful_and_executed_successfully(result)


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_list_endpoints(mock_execute_schema_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().list_endpoints(service_name)
    expected_query = f"show endpoints in service test_service"
    mock_execute_schema_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.list_endpoints")
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


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_suspend(mock_execute_schema_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().suspend(service_name)
    expected_query = f"alter service {service_name} suspend"
    mock_execute_schema_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.suspend")
def test_suspend_cli(mock_suspend, mock_statement_success, runner):
    service_name = "test_service"
    mock_suspend.return_value = mock_statement_success()
    result = runner.invoke(["spcs", "service", "suspend", service_name])
    assert_that_result_is_successful_and_executed_successfully(result)


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_resume(mock_execute_schema_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().resume(service_name)
    expected_query = f"alter service {service_name} resume"
    mock_execute_schema_query.assert_called_once_with(expected_query)
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.resume")
def test_resume_cli(mock_resume, mock_statement_success, runner):
    service_name = "test_service"
    mock_resume.return_value = mock_statement_success()
    result = runner.invoke(["spcs", "service", "resume", service_name])
    assert_that_result_is_successful_and_executed_successfully(result)


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_set_property(mock_execute_schema_query):
    service_name = "test_service"
    min_instances = 2
    max_instances = 3
    query_warehouse = "test_warehouse"
    auto_resume = False
    comment = to_string_literal("this is a test")
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().set_property(
        service_name=service_name,
        min_instances=min_instances,
        max_instances=max_instances,
        query_warehouse=query_warehouse,
        auto_resume=auto_resume,
        comment=comment,
    )
    expected_query = f"""
        alter service {service_name} set
        min_instances = {min_instances}
        max_instances = {max_instances}
        query_warehouse = {query_warehouse}
        auto_resume = {auto_resume}
        comment = {comment}
    """
    assert_mock_execute_is_called_once_with_query(
        mock_execute_schema_query, expected_query
    )
    assert result == cursor


def test_set_property_no_properties():
    service_name = "test_service"
    with pytest.raises(NoPropertiesProvidedError) as e:
        ServiceManager().set_property(service_name, None, None, None, None, None)
    assert e.value.message == ServiceManager.set_no_properties_message


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.set_property")
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
    assert_that_result_is_successful_and_executed_successfully(result)


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.set_property")
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


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
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
    assert e.value.message == ServiceManager.unset_no_properties_msg


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.unset_property")
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
    assert_that_result_is_successful_and_executed_successfully(result)


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.unset_property")
def test_unset_property_no_properties_cli(mock_unset, runner):
    service_name = "test_service"
    mock_unset.side_effect = NoPropertiesProvidedError(
        ServiceManager.unset_no_properties_msg
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
