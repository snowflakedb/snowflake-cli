from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock, patch

import pytest
import strictyaml
from snowflake.cli.plugins.spcs.services.manager import ServiceManager

from tests.testing_utils.fixtures import *


@patch(
    "snowflake.cli.plugins.spcs.services.manager.ServiceManager._execute_schema_query"
)
def test_create_service(mock_execute_schema_query, other_directory):
    service_name = "test_service"
    compute_pool = "test_pool"
    num_instances = 42
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

    cursor = Mock(spec=SnowflakeCursor)
    mock_execute_schema_query.return_value = cursor

    result = ServiceManager().create(
        service_name, compute_pool, Path(spec_path), num_instances
    )
    expected_query = (
        "CREATE SERVICE IF NOT EXISTS test_service "
        "IN COMPUTE POOL test_pool "
        'FROM SPECIFICATION $$ {"spec": {"containers": [{"name": "cloudbeaver", "image": '
        '"/spcs_demos_db/cloudbeaver:23.2.1"}], "endpoints": [{"name": "cloudbeaver", '
        '"port": 80, "public": true}]}} $$ '
        "WITH MIN_INSTANCES = 42 MAX_INSTANCES = 42"
    )
    actual_query = " ".join(
        mock_execute_schema_query.mock_calls[0].args[0].replace("\n", "").split()
    )
    assert expected_query == actual_query
    assert result == cursor


@patch("snowflake.cli.plugins.spcs.services.manager.ServiceManager._read_yaml")
def test_create_service_with_invalid_spec(mock_read_yaml):
    service_name = "test_service"
    compute_pool = "test_pool"
    spec_path = "/path/to/spec.yaml"
    num_instances = 42
    mock_read_yaml.side_effect = strictyaml.YAMLError("Invalid YAML")
    with pytest.raises(strictyaml.YAMLError):
        ServiceManager().create(
            service_name, compute_pool, Path(spec_path), num_instances
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
