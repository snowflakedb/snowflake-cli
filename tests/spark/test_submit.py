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

SCLS_MANAGER = "snowflake.cli._plugins.spark.commands.SparkManager"


class TestSclsSubmit:
    @mock.patch(SCLS_MANAGER)
    def test_submit_with_status_flag(self, mock_manager, runner, mock_cursor):
        """Test submit with status --status flag to check the status of the Spark application"""
        mock_manager().check_status.return_value = "ID: app-123\nExecution Status: RUNNING\nError Message: None\nError Code: None\nExit Code: None"

        result = runner.invoke(["spark", "submit", "--status", "app-123"])

        assert result.exit_code == 0, result.output
        mock_manager().check_status.assert_called_once_with("app-123")

    @mock.patch(SCLS_MANAGER)
    def test_submit_spark_application(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application."""
        # Create a temp entrypoint file
        entrypoint = tmp_path / "test.jar"
        entrypoint.write_text("print('hello')")

        mock_manager().upload_file_to_stage.return_value = "test.jar"
        mock_manager().submit.return_value = (
            "Spark Application submitted successfully. Spark Application ID: app-456"
        )

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--class",
                "com.example.Main",
                "--snow-file-stage",
                "@my_stage/jars",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            "Spark Application submitted successfully. Spark Application ID: app-456"
            in result.output
        )

        mock_manager().upload_file_to_stage.assert_called_once_with(
            str(entrypoint), "@my_stage/jars"
        )
        mock_manager().submit.assert_called_once_with(
            "EXECUTE SPARK APPLICATION ENVIRONMENT_RUNTIME_VERSION='1.0-preview' STAGE_MOUNTS=('@my_stage/jars:/tmp/entrypoint') ENTRYPOINT_FILE='/tmp/entrypoint/test.jar' CLASS = 'com.example.Main' SPARK_CONFIGURATION=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle') RESOURCE_CONSTRAINT='CPU_2X_X86'"
        )

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_application_arguments(self, mock_manager, runner, tmp_path):
        """Test submitting with application arguments."""
        entrypoint = tmp_path / "app.jar"
        entrypoint.write_text("")

        mock_manager().upload_file_to_stage.return_value = "app.jar"
        mock_manager().submit.return_value = "Spark Application ID: app-789"

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--class",
                "com.example.Main",
                "--snow-file-stage",
                "@stage",
                "arg1",
                "arg2",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_manager().submit.assert_called_once_with(
            "EXECUTE SPARK APPLICATION ENVIRONMENT_RUNTIME_VERSION='1.0-preview' STAGE_MOUNTS=('@stage:/tmp/entrypoint') ENTRYPOINT_FILE='/tmp/entrypoint/app.jar' CLASS = 'com.example.Main' ARGUMENTS = ('arg1','arg2') SPARK_CONFIGURATION=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle') RESOURCE_CONSTRAINT='CPU_2X_X86'"
        )

    def test_submit_missing_entrypoint_file(self, runner):
        """Test that submit fails when entrypoint file is missing."""
        result = runner.invoke(["spark", "submit", "--snow-file-stage", "@my_stage"])

        # Should fail because entrypoint_file is required when not using --status
        assert result.exit_code != 0
        assert "Entrypoint file path is required" in result.output

    def test_submit_missing_stage(self, runner, tmp_path):
        """Test that submit fails when stage is missing."""
        entrypoint = tmp_path / "main.py"
        entrypoint.write_text("print('hello')")

        result = runner.invoke(["spark", "submit", str(entrypoint)])

        # Should fail because --snow-file-stage is required
        assert result.exit_code != 0
        assert "--snow-file-stage is required" in result.output

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_jars_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --jars option."""
        # Create temp entrypoint file and jar files
        entrypoint = tmp_path / "app.jar"

        # Mock upload to return file names in order: entrypoint, jar1, jar2
        mock_manager().upload_file_to_stage.side_effect = [
            "app.jar",
            "lib1.jar",
            "lib2.jar",
        ]
        mock_manager().submit.return_value = "Spark Application ID: app-with-jars"

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--class",
                "com.example.Main",
                "--snow-file-stage",
                "@my_stage",
                "--jars",
                "lib1.jar,lib2.jar",
            ]
        )

        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-jars" in result.output

        # Verify upload_file_to_stage was called 3 times (entrypoint + 2 jars)
        assert mock_manager().upload_file_to_stage.call_count == 3

        # Verify the submit query contains spark.jars configuration
        mock_manager().submit.assert_called_once()
        submit_query = mock_manager().submit.call_args[0][0]
        assert "spark.jars" in submit_query
        assert "/tmp/entrypoint/lib1.jar" in submit_query
        assert "/tmp/entrypoint/lib2.jar" in submit_query
