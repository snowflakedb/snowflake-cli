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
            "RESOURCE_CONSTRAINT='CPU_2X_X86'"
        )
        assert query == expected

    def test_build_with_single_jar(self):
        """Test building query with a single jar dependency."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(None).with_jars(
            ["dependency.jar"]
        )

        query = builder.build()

        assert "'spark.jars' = '/tmp/entrypoint/dependency.jar'" in query

    def test_build_with_multiple_jars(self):
        """Test building query with multiple jar dependencies."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(None).with_jars(
            ["lib1.jar", "lib2.jar", "lib3.jar"]
        )

        query = builder.build()

        assert (
            "'spark.jars' = '/tmp/entrypoint/lib1.jar,/tmp/entrypoint/lib2.jar,/tmp/entrypoint/lib3.jar'"
            in query
        )

    def test_build_with_empty_jars_list(self):
        """Test building query with empty jars list does not add spark.jars."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(None).with_jars([])

        query = builder.build()

        assert "spark.jars" not in query

    def test_build_with_none_jars(self):
        """Test building query with None jars does not add spark.jars."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_class_name(None).with_application_arguments(None).with_jars(None)

        query = builder.build()

        assert "spark.jars" not in query

    def test_with_jars_returns_self_for_chaining(self):
        """Test that with_jars returns self for method chaining."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )

        result = builder.with_jars(["test.jar"])

        assert result is builder

    def test_build_with_py_files(self):
        """Test building query with py files."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_py_files(["app.zip", "app.egg"])

        query = builder.build()
        assert "spark.submit.pyFiles" in query
        assert "/tmp/entrypoint/app.zip" in query
        assert "/tmp/entrypoint/app.egg" in query

    def test_build_with_empty_py_files_list(self):
        """Test building query with empty py files list does not add spark.submit.pyFiles."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_py_files([])

        query = builder.build()
        assert "spark.submit.pyFiles" not in query

    def test_build_with_conf(self):
        """Test building query with conf."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_conf(
            ["spark.eventLog.enabled=false", "spark.sql.shuffle.partitions=200"]
        )

        query = builder.build()
        assert "'spark.eventLog.enabled' = 'false'" in query
        assert "'spark.sql.shuffle.partitions' = '200'" in query

    def test_build_with_empty_conf_list(self):
        """Test building query with empty conf list does not add spark.conf."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_conf([])

        query = builder.build()
        assert "spark.conf" not in query

    def test_build_with_name(self):
        """Test building query with name."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_name("app-name")

        query = builder.build()
        assert "'spark.app.name' = 'app-name'" in query

    def test_build_with_empty_name(self):
        """Test building query with empty name does not add spark.app.name."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_name("")

        query = builder.build()
        assert "spark.app.name" not in query

    def test_build_with_files(self):
        """Test building query with files."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_files(["data1.txt", "data2.txt"])

        query = builder.build()
        assert (
            "'spark.files' = '/tmp/entrypoint/data1.txt,/tmp/entrypoint/data2.txt'"
            in query
        )

    def test_build_with_empty_files_list(self):
        """Test building query with empty files list does not add spark.files."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_files([])

        query = builder.build()
        assert "spark.files" not in query

    def test_build_with_quoted_value(self):
        """Test building query with quoted value."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_conf(["spark.a='1'"])

        query = builder.build()
        assert "'spark.a' = '1'" in query

    def test_build_with_properties_file(self):
        """Test building query with properties file."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        conf_dict = {
            "spark.a": "1",
            "spark.c": "hello",
            "spark.b": "true",
        }
        builder.with_conf(conf_dict)

        query = builder.build()
        assert "'spark.a' = '1'" in query
        assert "'spark.c' = 'hello'" in query
        assert "'spark.b' = 'true'" in query

    def test_build_with_driver_java_options(self):
        """Test building query with driver java options."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_driver_java_options("-Xmx1024m")

        query = builder.build()
        assert "'spark.driver.extraJavaOptions' = '-Xmx1024m'" in query

    def test_build_with_empty_driver_java_options(self):
        """Test building query with empty driver java options does not add spark.driver.extraJavaOptions."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_driver_java_options("")

        query = builder.build()
        assert "spark.driver.extraJavaOptions" not in query

    def test_build_with_snow_stage_mount(self):
        """Test building query with snow stage mount."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_stage_mount("@stage1:path1,@stage2:path2")

        query = builder.build()
        assert (
            "STAGE_MOUNTS=('@stage1:path1','@stage2:path2','@my_stage:/tmp/entrypoint')"
            in query
        )

    def test_build_with_empty_snow_stage_mount(self):
        """Test building query with empty snow stage mount does not add snow stage mount."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_stage_mount("")

        query = builder.build()
        assert "STAGE_MOUNTS=('@my_stage:/tmp/entrypoint')" in query

    def test_build_with_snow_environment_runtime_version(self):
        """Test building query with snow environment runtime version."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_environment_runtime_version("1.0")

        query = builder.build()
        assert "ENVIRONMENT_RUNTIME_VERSION='1.0'" in query

    def test_build_with_empty_snow_environment_runtime_version(self):
        """Test building query with empty snow environment runtime version does not add snow environment runtime version."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_environment_runtime_version("")

        query = builder.build()
        assert "ENVIRONMENT_RUNTIME_VERSION='1.0-preview'" in query

    def test_build_with_snow_packages(self):
        """Test building query with snow packages."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_packages("package1,package2")

        query = builder.build()
        assert "PACKAGES=('package1','package2')" in query

    def test_build_with_empty_snow_packages(self):
        """Test building query with empty snow packages does not add snow packages."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_packages("")

        query = builder.build()
        assert "PACKAGES" not in query

    def test_build_with_snow_external_access_integrations(self):
        """Test building query with snow external access integrations."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_external_access_integrations("eai1,eai2")

        query = builder.build()
        assert "EXTERNAL_ACCESS_INTEGRATIONS=(eai1,eai2)" in query

    def test_build_with_empty_snow_external_access_integrations(self):
        """Test building query with empty snow external access integrations does not add snow external access integrations."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_external_access_integrations("")

        query = builder.build()
        assert "EXTERNAL_ACCESS_INTEGRATIONS" not in query

    def test_build_with_snow_secrets(self):
        """Test building query with snow secrets."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_secrets("secret1=secret1_value,secret2=secret2_value")

        query = builder.build()
        assert "SECRETS=('secret1' = secret1_value, 'secret2' = secret2_value)" in query

    def test_build_with_empty_snow_secrets(self):
        """Test building query with empty snow secrets does not add snow secrets."""
        builder = SubmitQueryBuilder(
            file_on_stage="app.py", scls_file_stage="@my_stage"
        )
        builder.with_snow_secrets("")

        query = builder.build()
        assert "SECRETS" not in query
