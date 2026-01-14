# Copyright (c) 2025 Snowflake Inc.
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

from unittest import mock

import pytest
from click import ClickException
from snowflake.cli._plugins.spark.manager import SparkManager

SCLS_MANAGER = "snowflake.cli._plugins.spark.manager.SparkManager"


class TestSclsManager:
    @mock.patch(f"{SCLS_MANAGER}._set_session_config")
    @mock.patch(f"{SCLS_MANAGER}.execute_query")
    def test_submit_success(
        self, mock_execute_query, mock_set_session_config, mock_cursor
    ):
        """Test successful submission of a Spark application."""
        mock_execute_query.return_value = mock_cursor(
            rows=[("Spark Application submitted successfully. ID: app-123",)],
            columns=["result"],
        )
        mock_set_session_config.return_value = None

        manager = SparkManager()
        result = manager.submit(
            submit_query="EXECUTE SPARK APPLICATION ENVIRONMENT_RUNTIME_VERSION='1.0-preview' STAGE_MOUNTS=('@my_stage/jars:/tmp/entrypoint') ENTRYPOINT_FILE='/tmp/entrypoint/app.jar' CLASS = 'com.example.Main' SPARK_CONFIGURATIONS=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle', 'spark.eventLog.enabled' = 'false') RESOURCE_CONSTRAINT='CPU_2X_X86'",
            image=None,
        )

        assert result == "Spark Application submitted successfully. ID: app-123"
        mock_execute_query.assert_any_call(
            "EXECUTE SPARK APPLICATION ENVIRONMENT_RUNTIME_VERSION='1.0-preview' STAGE_MOUNTS=('@my_stage/jars:/tmp/entrypoint') ENTRYPOINT_FILE='/tmp/entrypoint/app.jar' CLASS = 'com.example.Main' SPARK_CONFIGURATIONS=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle', 'spark.eventLog.enabled' = 'false') RESOURCE_CONSTRAINT='CPU_2X_X86'",
        )

    @mock.patch(f"{SCLS_MANAGER}._set_session_config")
    @mock.patch(f"{SCLS_MANAGER}.execute_query")
    def test_submit_failure_raises_click_exception(
        self, mock_execute_query, mock_set_session_config
    ):
        """Test that submit raises ClickException on failure."""
        mock_execute_query.side_effect = Exception("Connection failed")
        mock_set_session_config.return_value = None
        manager = SparkManager()
        with pytest.raises(ClickException) as exc_info:
            manager.submit(
                submit_query="EXECUTE SPARK APPLICATION ENVIRONMENT_RUNTIME_VERSION='1.0-preview' STAGE_MOUNTS=('@my_stage/jars:/tmp/entrypoint') ENTRYPOINT_FILE='/tmp/entrypoint/app.jar' CLASS = 'com.example.Main' SPARK_CONFIGURATIONS=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle', 'spark.eventLog.enabled' = 'false') RESOURCE_CONSTRAINT='CPU_2X_X86'",
                image=None,
            )

        assert "Failed to submit Spark application" in str(exc_info.value.message)
        assert "Connection failed" in str(exc_info.value.message)

    @mock.patch(f"{SCLS_MANAGER}.execute_query")
    def test_upload_file_to_stage_success(self, mock_execute_query, mock_cursor):
        """Test successful file upload to stage."""
        mock_execute_query.return_value = mock_cursor(
            rows=[
                (
                    "/path/to/app.jar",
                    "app.jar",
                    1024,
                    1024,
                    "none",
                    "none",
                    "UPLOADED",
                    "",
                )
            ],
            columns=[
                "source",
                "target",
                "source_size",
                "target_size",
                "source_compression",
                "target_compression",
                "status",
                "message",
            ],
        )

        manager = SparkManager()
        result = manager.upload_file_to_stage("/path/to/app.jar", "@my_stage/jars")

        assert result == "app.jar"
        mock_execute_query.assert_called_once_with(
            "PUT file:///path/to/app.jar @my_stage/jars AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )

    @mock.patch(f"{SCLS_MANAGER}.execute_query")
    def test_upload_file_to_stage_failure_raises_click_exception(
        self, mock_execute_query
    ):
        """Test that upload_file_to_stage raises ClickException on failure."""
        mock_execute_query.side_effect = Exception("Stage not found")

        manager = SparkManager()
        with pytest.raises(ClickException) as exc_info:
            manager.upload_file_to_stage("/path/to/app.jar", "@my_stage")

        assert "Failed to upload" in str(exc_info.value.message)
        assert "Stage not found" in str(exc_info.value.message)

    @mock.patch(f"{SCLS_MANAGER}.execute_query")
    def test_check_status_success(self, mock_execute_query, mock_cursor):
        """Test successful status check of a Spark application."""
        expected_cursor = mock_cursor(
            rows=[
                (
                    "app-123",
                    "test-app",
                    "query-123",
                    "account-123",
                    "2025-01-01",
                    "2025-01-01 10:00:00",
                    "2025-01-01 10:00:00",
                    "RUNNING",
                    "None",
                    "user",
                    "USER",
                    "db",
                    "123",
                    "schema",
                    "123",
                    "None",
                    "None",
                )
            ],
            columns=[
                "ID",
                "NAME",
                "QUERY_ID",
                "ACCOUNT_ID",
                "CREATED_ON",
                "STARTED_ON",
                "COMPLETED_ON",
                "EXECUTION_STATUS",
                "ERROR_MESSAGE",
                "OWNER",
                "OWNER_ROLE_TYPE",
                "DATABASE_NAME",
                "DATABASE_ID",
                "SCHEMA_NAME",
                "SCHEMA_ID",
                "ERROR_CODE",
                "EXIT_CODE",
            ],
        )
        mock_execute_query.return_value = expected_cursor

        manager = SparkManager()
        result = manager.check_status("app-123")

        assert (
            result
            == "ID: app-123\nExecution Status: RUNNING\nError Message: None\nError Code: None\nExit Code: None"
        )
        mock_execute_query.assert_called_once_with(
            "SELECT * FROM TABLE(snowflake.spark.GET_SPARK_APPLICATION_HISTORY()) WHERE ID = 'app-123'"
        )

    @mock.patch(f"{SCLS_MANAGER}.execute_query")
    def test_check_status_failure_raises_click_exception(self, mock_execute_query):
        """Test that check_status raises ClickException on failure."""
        mock_execute_query.side_effect = Exception("Query execution failed")

        manager = SparkManager()
        with pytest.raises(ClickException) as exc_info:
            manager.check_status("app-123")

        assert "Failed to check status of app-123" in str(exc_info.value.message)
        assert "Query execution failed" in str(exc_info.value.message)
