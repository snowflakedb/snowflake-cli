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
                "--scls-file-stage",
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
            "test.jar", None, "com.example.Main", "@my_stage/jars"
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
                "--scls-file-stage",
                "@stage",
                "arg1",
                "arg2",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_manager().submit.assert_called_once_with(
            "app.jar", ["arg1", "arg2"], "com.example.Main", "@stage"
        )

    def test_submit_missing_entrypoint_file(self, runner):
        """Test that submit fails when entrypoint file is missing."""
        result = runner.invoke(["spark", "submit", "--scls-file-stage", "@my_stage"])

        # Should fail because entrypoint_file is required when not using --status
        assert result.exit_code != 0
        assert "Entrypoint file path is required" in result.output

    def test_submit_missing_stage(self, runner, tmp_path):
        """Test that submit fails when stage is missing."""
        entrypoint = tmp_path / "main.py"
        entrypoint.write_text("print('hello')")

        result = runner.invoke(["spark", "submit", str(entrypoint)])

        # Should fail because --scls-file-stage is required
        assert result.exit_code != 0
        assert "--scls-file-stage is required" in result.output
