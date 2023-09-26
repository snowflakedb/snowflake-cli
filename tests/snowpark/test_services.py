import yaml
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from snowflake.connector.cursor import SnowflakeCursor
from snowcli.cli.snowpark.services.manager import ServiceManager


class TestServiceManager(unittest.TestCase):
    def setUp(self):
        self.service_manager = ServiceManager()

    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._read_yaml")
    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._execute_schema_query")
    def test_create_service(self, mock_execute_schema_query, mock_read_yaml):
        service_name = "test_service"
        compute_pool = "test_pool"
        spec_path = "/path/to/spec.yaml"
        num_instances = 42
        spec_content = '{"key": "value"}'

        mock_read_yaml.return_value = spec_content
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_schema_query.return_value = cursor

        result = self.service_manager.create(
            service_name, compute_pool, Path(spec_path), num_instances
        )
        expected_query = (
            "CREATE SERVICE IF NOT EXISTS test_service "
            "IN COMPUTE POOL test_pool "
            'FROM SPECIFICATION \' {"key": "value"} \' '
            "WITH MIN_INSTANCES = 42 MAX_INSTANCES = 42"
        )
        mock_read_yaml.assert_called_once_with(Path(spec_path))
        actual_query = " ".join(
            mock_execute_schema_query.mock_calls[0].args[0].replace("\n", "").split()
        )
        self.assertEqual(expected_query, actual_query)
        self.assertEqual(result, cursor)

    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._read_yaml")
    def test_create_service_with_invalid_spec(self, mock_read_yaml):
        service_name = "test_service"
        compute_pool = "test_pool"
        spec_path = "/path/to/spec.yaml"
        num_instances = 42
        mock_read_yaml.side_effect = yaml.YAMLError("Invalid YAML")
        with self.assertRaises(yaml.YAMLError):
            self.service_manager.create(
                service_name, compute_pool, Path(spec_path), num_instances
            )

    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._execute_schema_query")
    def test_desc(self, mock_execute_schema_query):
        service_name = "test_service"
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_schema_query.return_value = cursor
        result = self.service_manager.desc(service_name)
        expected_query = "desc service test_service"
        mock_execute_schema_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)

    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._execute_schema_query")
    def test_show(self, mock_execute_schema_query):
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_schema_query.return_value = cursor
        result = self.service_manager.show()
        expected_query = "show services"
        mock_execute_schema_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)

    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._execute_schema_query")
    def test_status(self, mock_execute_schema_query):
        service_name = "test_service"
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_schema_query.return_value = cursor
        result = self.service_manager.status(service_name)
        expected_query = "CALL SYSTEM$GET_SERVICE_STATUS('test_service')"
        mock_execute_schema_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)

    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._execute_schema_query")
    def test_drop(self, mock_execute_schema_query):
        service_name = "test_service"
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_schema_query.return_value = cursor
        result = self.service_manager.drop(service_name)
        expected_query = "drop service test_service"
        mock_execute_schema_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)

    @patch("snowcli.cli.snowpark.services.manager.ServiceManager._execute_schema_query")
    def test_logs(self, mock_execute_schema_query):
        service_name = "test_service"
        container_name = "test_container"
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_schema_query.return_value = cursor
        result = self.service_manager.logs(service_name, container_name)
        expected_query = (
            "call SYSTEM$GET_SERVICE_LOGS('test_service', '0', 'test_container');"
        )
        mock_execute_schema_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)


<<<<<<< HEAD
@mock.patch("snowflake.connector.connect")
def test_desc_service(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    service_name = "test_service"

    result = runner.invoke(["snowpark", "services", "desc", service_name])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == f"desc service {service_name}"


@mock.patch("snowflake.connector.connect")
def test_list_service(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "list"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show services"


@mock.patch("snowflake.connector.connect")
def test_drop_service(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "drop", "serviceName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "drop service serviceName"


@mock.patch("snowflake.connector.connect")
def test_service_status(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "status", "serviceName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "CALL SYSTEM$GET_SERVICE_STATUS('serviceName')"


@mock.patch("snowflake.connector.connect")
def test_service_logs(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "snowpark",
            "services",
            "logs",
            "--container_name",
            "containerName",
            "serviceName",
        ]
    )

    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query()
        == "call SYSTEM$GET_SERVICE_LOGS('serviceName', '0', 'containerName');"
    )

if __name__ == "__main__":
    unittest.main()
