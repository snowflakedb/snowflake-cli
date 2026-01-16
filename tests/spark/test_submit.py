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
            "EXECUTE SPARK APPLICATION ENVIRONMENT_RUNTIME_VERSION='1.0-preview' STAGE_MOUNTS=('@my_stage/jars:/tmp/entrypoint') ENTRYPOINT_FILE='/tmp/entrypoint/test.jar' CLASS = 'com.example.Main' SPARK_CONFIGURATION=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle') RESOURCE_CONSTRAINT='CPU_2X_X86'",
            None,
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
            "EXECUTE SPARK APPLICATION ENVIRONMENT_RUNTIME_VERSION='1.0-preview' STAGE_MOUNTS=('@stage:/tmp/entrypoint') ENTRYPOINT_FILE='/tmp/entrypoint/app.jar' CLASS = 'com.example.Main' ARGUMENTS = ('arg1','arg2') SPARK_CONFIGURATION=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle') RESOURCE_CONSTRAINT='CPU_2X_X86'",
            None,
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

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_py_files_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --py-files option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.side_effect = [
            "app.py",
            "app.zip",
            "app.egg",
        ]
        mock_manager().submit.return_value = "Spark Application ID: app-with-py-files"

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--py-files",
                "app.zip,app.egg",
            ]
        )

        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-py-files" in result.output

        assert mock_manager().upload_file_to_stage.call_count == 3
        assert mock_manager().submit.call_count == 1
        submit_query = mock_manager().submit.call_args[0][0]
        assert "spark.submit.pyFiles" in submit_query
        assert "/tmp/entrypoint/app.zip" in submit_query
        assert "/tmp/entrypoint/app.egg" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_conf_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --conf option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = "Spark Application ID: app-with-conf"

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--conf",
                "spark.eventLog.enabled=false",
                "--conf",
                "spark.sql.shuffle.partitions=200",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-conf" in result.output
        submit_query = mock_manager().submit.call_args[0][0]
        assert "'spark.eventLog.enabled' = 'false'" in submit_query
        assert "'spark.sql.shuffle.partitions' = '200'" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_name_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --name option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = "Spark Application ID: app-with-name"

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--name",
                "app-name",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-name" in result.output
        submit_query = mock_manager().submit.call_args[0][0]
        assert "'spark.app.name' = 'app-name'" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_files_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --files option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.side_effect = [
            "app.py",
            "data1.txt",
            "data2.txt",
        ]
        mock_manager().submit.return_value = "Spark Application ID: app-with-files"

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--files",
                "data1.txt,data2.txt",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-files" in result.output
        submit_query = mock_manager().submit.call_args[0][0]
        assert "spark.files" in submit_query
        assert "/tmp/entrypoint/data1.txt" in submit_query
        assert "/tmp/entrypoint/data2.txt" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_properties_file_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --properties-file option."""
        entrypoint = tmp_path / "app.py"
        properties_file = tmp_path / "test.conf"
        entrypoint.write_text("print('hello')")
        properties_file.write_text(
            """
        # comment 1
        spark.a 1
        spark.c    "hello"
        # comment 2
        spark.b     true
        """
        )

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = (
            "Spark Application ID: app-with-properties-file"
        )

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--properties-file",
                str(properties_file),
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-properties-file" in result.output
        submit_query = mock_manager().submit.call_args[0][0]
        assert "'spark.a' = '1'" in submit_query
        assert "'spark.b' = 'true'" in submit_query
        assert "'spark.c' = 'hello'" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_driver_java_options_option(
        self, mock_manager, runner, tmp_path
    ):
        """Test submitting a Spark application with --driver-java-options option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = (
            "Spark Application ID: app-with-driver-java-options"
        )

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--driver-java-options",
                "-Xmx1024m",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-driver-java-options" in result.output
        submit_query = mock_manager().submit.call_args[0][0]
        assert "'spark.driver.extraJavaOptions' = '-Xmx1024m'" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_kill_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --kill option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().kill.return_value = "Spark Application killed successfully"

        result = runner.invoke(
            [
                "spark",
                "submit",
                "--kill",
                "app-123",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application killed successfully" in result.output
        mock_manager().kill.assert_called_once_with("app-123")

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_snow_stage_mount_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --snow-stage-mount option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = (
            "Spark Application ID: app-with-snow-stage-mount"
        )

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--snow-stage-mount",
                "@stage1:path1,@stage2:path2",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-snow-stage-mount" in result.output
        submit_query = mock_manager().submit.call_args[0][0]
        assert (
            "STAGE_MOUNTS=('@stage1:path1','@stage2:path2','@my_stage:/tmp/entrypoint')"
            in submit_query
        )

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_snow_environment_runtime_version_option(
        self, mock_manager, runner, tmp_path
    ):
        """Test submitting a Spark application with --snow-environment-runtime-version option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = (
            "Spark Application ID: app-with-snow-environment-runtime-version"
        )

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--snow-environment-runtime-version",
                "1.0",
            ]
        )
        assert result.exit_code == 0, result.output
        assert (
            "Spark Application ID: app-with-snow-environment-runtime-version"
            in result.output
        )
        submit_query = mock_manager().submit.call_args[0][0]
        assert "ENVIRONMENT_RUNTIME_VERSION='1.0'" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_snow_packages_option(self, mock_manager, runner, tmp_path):
        """Test submitting a Spark application with --snow-packages option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = (
            "Spark Application ID: app-with-snow-packages"
        )

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--snow-packages",
                "package1,package2",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Spark Application ID: app-with-snow-packages" in result.output
        submit_query = mock_manager().submit.call_args[0][0]
        assert "PACKAGES=('package1','package2')" in submit_query

    @mock.patch(SCLS_MANAGER)
    def test_submit_with_snow_external_access_integrations_option(
        self, mock_manager, runner, tmp_path
    ):
        """Test submitting a Spark application with --snow-external-access-integrations option."""
        entrypoint = tmp_path / "app.py"

        mock_manager().upload_file_to_stage.return_value = "app.py"
        mock_manager().submit.return_value = (
            "Spark Application ID: app-with-snow-external-access-integrations"
        )

        result = runner.invoke(
            [
                "spark",
                "submit",
                str(entrypoint),
                "--snow-file-stage",
                "@my_stage",
                "--snow-external-access-integrations",
                "eai1,eai2",
            ]
        )
        assert result.exit_code == 0, result.output
        assert (
            "Spark Application ID: app-with-snow-external-access-integrations"
            in result.output
        )
        submit_query = mock_manager().submit.call_args[0][0]
        assert "EXTERNAL_ACCESS_INTEGRATIONS=(eai1,eai2)" in submit_query
