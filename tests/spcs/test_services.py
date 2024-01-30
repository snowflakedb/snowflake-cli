from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock, patch

from click import ClickException
import pytest
import strictyaml
from snowflake.cli.plugins.spcs.services.manager import ServiceManager
from tests.testing_utils.fixtures import *
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.plugins.object.common import Tag


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_create_service(mock_execute_schema_query, other_directory):
    service_name = "test_service"
    compute_pool = "test_pool"
    min_instances = 42
    max_instances = 43
    tmp_dir = Path(other_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(
        dedent(
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
    )
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
        spec_path=Path(spec_path),
        min_instances=min_instances,
        max_instances=max_instances,
        auto_resume=auto_resume,
        external_access_integrations=external_access_integrations,
        query_warehouse=query_warehouse,
        tags=tags,
        comment=comment,
    )
    expected_query = " ".join(
        [
            "CREATE SERVICE IF NOT EXISTS test_service",
            "IN COMPUTE POOL test_pool",
            'FROM SPECIFICATION $$ {"spec": {"containers": [{"name": "cloudbeaver", "image":',
            '"/spcs_demos_db/cloudbeaver:23.2.1"}], "endpoints": [{"name": "cloudbeaver",',
            '"port": 80, "public": true}]}} $$',
            "WITH MIN_INSTANCES = 42 MAX_INSTANCES = 43",
            "AUTO_RESUME = True",
            "EXTERNAL_ACCESS_INTEGRATIONS = (google_apis_access_integration,salesforce_api_access_integration)",
            "QUERY_WAREHOUSE = test_warehouse",
            "TAG (test_tag='test value',key='value')",
            "COMMENT = 'user\\'s comment'",
        ]
    )
    actual_query = " ".join(mock_execute_schema_query.mock_calls[0].args[0].split())
    assert expected_query == actual_query
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.create")
def test_create_service_cli_defaults(mock_create, other_directory, runner):
    tmp_dir = Path(other_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(
        dedent(
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
    )
    result = runner.invoke(
        [
            "spcs",
            "service",
            "create",
            "--name",
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
        external_access_integrations=[],
        query_warehouse=None,
        tags=[],
        comment=None,
    )


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager.create")
def test_create_service_cli(mock_create, other_directory, runner):
    tmp_dir = Path(other_directory)
    spec_path = tmp_dir / "spec.yml"
    spec_path.write_text(
        dedent(
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
    )
    result = runner.invoke(
        [
            "spcs",
            "service",
            "create",
            "--name",
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
        ]
    )
    assert result.exit_code == 0, result.output
    print(mock_create.mock_calls[0])
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


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_status(mock_execute_schema_query):
    service_name = "test_service"
    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor
    result = ServiceManager().status(service_name)
    expected_query = "CALL SYSTEM$GET_SERVICE_STATUS('test_service')"
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
        "call SYSTEM$GET_SERVICE_LOGS('test_service', '10', 'test_container', 42);"
    )
    mock_execute_schema_query.assert_called_once_with(expected_query)
    assert result == cursor
