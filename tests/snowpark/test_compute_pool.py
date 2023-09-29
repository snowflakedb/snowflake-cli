import unittest
from unittest.mock import Mock, patch
from snowflake.connector.cursor import SnowflakeCursor
from snowcli.cli.snowpark.compute_pool.manager import ComputePoolManager


class TestComputePoolManager(unittest.TestCase):
    def setUp(self):
        self.compute_pool_manager = ComputePoolManager()

    @patch(
        "snowcli.cli.snowpark.compute_pool.manager.ComputePoolManager._execute_query"
    )
    def test_create(self, mock_execute_query):
        pool_name = "test_pool"
        num_instances = 2
        instance_family = "test_family"
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_query.return_value = cursor
        result = self.compute_pool_manager.create(
            pool_name, num_instances, instance_family
        )
        expected_query = (
            "CREATE COMPUTE POOL test_pool "
            "MIN_NODES = 2 "
            "MAX_NODES = 2 "
            "INSTANCE_FAMILY = test_family;"
        )
        actual_query = " ".join(
            mock_execute_query.mock_calls[0].args[0].replace("\n", "").split()
        )
        self.assertEqual(expected_query, actual_query)
        self.assertEqual(result, cursor)

    @patch(
        "snowcli.cli.snowpark.compute_pool.manager.ComputePoolManager._execute_query"
    )
    def test_show(self, mock_execute_query):
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_query.return_value = cursor
        result = self.compute_pool_manager.show()
        expected_query = "show compute pools;"
        mock_execute_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)

    @patch(
        "snowcli.cli.snowpark.compute_pool.manager.ComputePoolManager._execute_query"
    )
    def test_drop(self, mock_execute_query):
        pool_name = "test_pool"
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_query.return_value = cursor
        result = self.compute_pool_manager.drop(pool_name)
        expected_query = "drop compute pool test_pool;"
        mock_execute_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)

    @patch(
        "snowcli.cli.snowpark.compute_pool.manager.ComputePoolManager._execute_query"
    )
    def test_stop(self, mock_execute_query):
        pool_name = "test_pool"
        cursor = Mock(spec=SnowflakeCursor)
        mock_execute_query.return_value = cursor
        result = self.compute_pool_manager.stop(pool_name)
        expected_query = "alter compute pool test_pool stop all;"
        mock_execute_query.assert_called_once_with(expected_query)
        self.assertEqual(result, cursor)


if __name__ == "__main__":
    unittest.main()
