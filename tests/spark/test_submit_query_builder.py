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

import pytest
from click import ClickException
from snowflake.cli._plugins.spark.manager import SubmitQueryBuilder


class TestSubmitQueryBuilder:
    def test_build_python_file_basic(self):
        """Test building query for a Python file without optional parameters."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(None)

        query = builder.build()

        assert "EXECUTE SPARK APPLICATION" in query
        assert "ENVIRONMENT_RUNTIME_VERSION='1.0-preview'" in query
        assert "STAGE_MOUNTS=('@my_stage:/tmp/entrypoint')" in query
        assert "ENTRYPOINT_FILE='/tmp/entrypoint/app.py'" in query
        assert "CLASS" not in query  # Python files don't require class
        assert "ARGUMENTS" not in query
        assert "SPARK_CONFIGURATIONS=" in query
        assert "RESOURCE_CONSTRAINT='CPU_2X_X86'" in query

    def test_build_jar_file_with_class_name(self):
        """Test building query for a JAR file with class name."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.jar", scls_file_stage="@my_stage/jars"
        )
        builder.with_class_name("com.example.Main").with_application_arguments(None)

        query = builder.build()

        assert "EXECUTE SPARK APPLICATION" in query
        assert "ENTRYPOINT_FILE='/tmp/entrypoint/app.jar'" in query
        assert "CLASS = 'com.example.Main'" in query

    def test_build_jar_file_without_class_name_raises_exception(self):
        """Test that building a JAR query without class name raises ClickException."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.jar", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(None)

        with pytest.raises(ClickException) as exc_info:
            builder.build()

        assert "Main class name is required for Scala/Java applications" in str(
            exc_info.value.message
        )

    def test_build_with_application_arguments(self):
        """Test building query with application arguments."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(["arg1", "arg2"])

        query = builder.build()

        assert "ARGUMENTS = ('arg1','arg2')" in query

    def test_build_with_single_application_argument(self):
        """Test building query with a single application argument."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(["only_arg"])

        query = builder.build()

        assert "ARGUMENTS = ('only_arg')" in query

    def test_build_with_empty_application_arguments(self):
        """Test building query with empty application arguments list."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments([])

        query = builder.build()

        assert "ARGUMENTS" not in query

    def test_build_with_arguments_containing_single_quotes(self):
        """Test that single quotes in arguments are escaped."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(
            ["it's", "arg'with'quotes"]
        )

        query = builder.build()

        assert "ARGUMENTS = ('it\\'s','arg\\'with\\'quotes')" in query

    def test_build_stage_name_with_trailing_slash(self):
        """Test that trailing slash in stage name is handled correctly."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage/"
        )
        builder.with_class_name(None).with_application_arguments(None)

        query = builder.build()

        assert "STAGE_MOUNTS=('@@my_stage:/tmp/entrypoint')" in query

    def test_build_stage_name_without_trailing_slash(self):
        """Test that stage name without trailing slash is used as-is."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(None)

        query = builder.build()

        assert "STAGE_MOUNTS=('@my_stage:/tmp/entrypoint')" in query

    def test_build_with_nested_stage_path(self):
        """Test building query with a nested stage path."""
        builder = SubmitQueryBuilder(
            file_on_stage="main.py", scls_file_stage="@db.schema.stage/path/to/files"
        )
        builder.with_class_name(None).with_application_arguments(None)

        query = builder.build()

        assert (
            "STAGE_MOUNTS=('@db.schema.stage/path/to/files:/tmp/entrypoint')" in query
        )
        assert "ENTRYPOINT_FILE='/tmp/entrypoint/main.py'" in query

    def test_builder_method_chaining(self):
        """Test that builder methods return self for chaining."""
        builder = SubmitQueryBuilder(file_on_stage="app.jar", scls_file_stage="@stage")

        result = builder.with_class_name("com.Main").with_application_arguments(
            ["arg1"]
        )

        assert result is builder

    def test_build_full_query_structure(self):
        """Test the complete query structure with all options."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.jar", scls_file_stage="@my_stage"
        )
        builder.with_class_name("com.example.Main").with_application_arguments(
            ["arg1", "arg2"]
        )

        query = builder.build()

        expected = (
            "EXECUTE SPARK APPLICATION "
            "ENVIRONMENT_RUNTIME_VERSION='1.0-preview' "
            "STAGE_MOUNTS=('@my_stage:/tmp/entrypoint') "
            "ENTRYPOINT_FILE='/tmp/entrypoint/app.jar' "
            "CLASS = 'com.example.Main' "
            "ARGUMENTS = ('arg1','arg2') "
            "SPARK_CONFIGURATIONS=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', "
            "'spark.snowflake.backend' = 'sparkle', 'spark.eventLog.enabled' = 'false') "
            "RESOURCE_CONSTRAINT='CPU_2X_X86'"
        )
        assert query == expected
