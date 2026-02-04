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

import logging
from typing import Dict, List, Optional, Union

from click import ClickException
from snowflake.cli.api.sql_execution import SqlExecutionMixin

log = logging.getLogger(__name__)


class SubmitQueryBuilder:
    def __init__(self, file_on_stage: str, scls_file_stage: str):
        self.file_on_stage = file_on_stage
        self.snow_file_stage = scls_file_stage
        self.spark_configurations: dict[str, str] = {}
        self.class_name: Optional[str] = None
        self.application_arguments: Optional[List[str]] = None
        self.jars: Optional[List[str]] = None
        self.py_files: Optional[List[str]] = None
        self.name: Optional[str] = None
        self.files: Optional[List[str]] = None
        self.driver_java_options: Optional[str] = None
        self.snow_stage_mount: dict[str, str] = {}
        self.snow_environment_runtime_version: str = "0.1"
        self.snow_packages: List[str] = []
        self.snow_external_access_integrations: List[str] = []
        self.snow_secrets: dict[str, str] = {}

    def _quote_value(self, value: str) -> str:
        if value.startswith('"') and value.endswith('"'):
            value = value.strip('"')
        elif value.startswith("'") and value.endswith("'"):
            value = value.strip("'")
        return "'" + value.replace("'", "\\'") + "'"

    def with_class_name(self, class_name: Optional[str]) -> "SubmitQueryBuilder":
        self.class_name = class_name
        return self

    def with_application_arguments(
        self, application_arguments: Optional[List[str]]
    ) -> "SubmitQueryBuilder":
        self.application_arguments = application_arguments
        return self

    def with_jars(self, jars: Optional[List[str]]) -> "SubmitQueryBuilder":
        self.jars = jars
        if jars and len(jars) > 0:
            self.spark_configurations["spark.jars"] = ",".join(
                f"/tmp/entrypoint/{jar}" for jar in jars
            )
        return self

    def with_py_files(self, py_files: Optional[List[str]]) -> "SubmitQueryBuilder":
        self.py_files = py_files
        if py_files and len(py_files) > 0:
            self.spark_configurations["spark.submit.pyFiles"] = ",".join(
                f"/tmp/entrypoint/{py_file}" for py_file in py_files
            )
        return self

    def with_conf(
        self, confs: Optional[Union[List[str], Dict[str, str]]]
    ) -> "SubmitQueryBuilder":
        if confs:
            if isinstance(confs, dict):
                for key, value in confs.items():
                    self.spark_configurations[key] = value
            else:
                for conf in confs:
                    key, value = conf.split("=", 1)
                    self.spark_configurations[key] = value
        return self

    def with_name(self, name: Optional[str]) -> "SubmitQueryBuilder":
        self.name = name
        if name:
            self.spark_configurations["spark.app.name"] = name
        return self

    def with_files(self, files: Optional[List[str]]) -> "SubmitQueryBuilder":
        self.files = files
        if files and len(files) > 0:
            self.spark_configurations["spark.files"] = ",".join(
                [f"/tmp/entrypoint/{file}" for file in files]
            )
        return self

    def with_driver_java_options(
        self, driver_java_options: Optional[str]
    ) -> "SubmitQueryBuilder":
        self.driver_java_options = driver_java_options
        if driver_java_options:
            self.spark_configurations[
                "spark.driver.extraJavaOptions"
            ] = driver_java_options
        return self

    def with_snow_stage_mount(self, mount: Optional[str]) -> "SubmitQueryBuilder":
        if mount:
            mount_list = mount.split(",")
            for mount in mount_list:
                stage_name, path = mount.split(":")
                self.snow_stage_mount[stage_name] = path
        return self

    def with_snow_environment_runtime_version(
        self, version: Optional[str]
    ) -> "SubmitQueryBuilder":
        if version:
            self.snow_environment_runtime_version = version
        return self

    def with_snow_packages(self, packages: Optional[str]) -> "SubmitQueryBuilder":
        if packages:
            package_list = packages.split(",")
            for package in package_list:
                self.snow_packages.append(package)
        return self

    def with_snow_external_access_integrations(
        self, eais: Optional[str]
    ) -> "SubmitQueryBuilder":
        if eais:
            eai_list = eais.split(",")
            for eai in eai_list:
                self.snow_external_access_integrations.append(eai)
        return self

    def with_snow_secrets(self, secrets: Optional[str]) -> "SubmitQueryBuilder":
        if secrets:
            secret_list = secrets.split(",")
            for secret in secret_list:
                reference_name, secret_name = secret.split("=")
                self.snow_secrets[reference_name] = secret_name
        return self

    def build(self) -> str:
        stage_name = (
            self.snow_file_stage
            if not self.snow_file_stage.endswith("/")
            else f"@{self.snow_file_stage.rstrip('/')}"
        )

        self.snow_stage_mount[stage_name] = "/tmp/entrypoint"

        query_parts = [
            "EXECUTE SPARK APPLICATION",
            f"ENVIRONMENT_RUNTIME_VERSION='{self.snow_environment_runtime_version}'",
        ]
        mount_str = ",".join(
            f"'{stage_name}:{path}'"
            for stage_name, path in self.snow_stage_mount.items()
        )
        query_parts.append(f"STAGE_MOUNTS=({mount_str})")

        query_parts.append(f"ENTRYPOINT_FILE='/tmp/entrypoint/{self.file_on_stage}'")

        # Scala/Java applications require a main class name
        if self.file_on_stage.endswith(".jar"):
            if not self.class_name:
                raise ClickException(
                    "Main class name is required for Scala/Java applications"
                )
            query_parts.append(f"CLASS = '{self.class_name}'")

        if self.application_arguments and len(self.application_arguments) > 0:
            escaped_args = [
                self._quote_value(arg) for arg in self.application_arguments
            ]
            query_parts.append(f"ARGUMENTS = ({','.join(escaped_args)})")

        if len(self.spark_configurations) > 0:
            spark_configurations = [
                f"{self._quote_value(key)} = {self._quote_value(value)}"
                for key, value in self.spark_configurations.items()
            ]
            query_parts.append(
                f"SPARK_CONFIGURATION=({', '.join(spark_configurations)})"
            )

        if self.snow_packages:
            packages = [f"'{package}'" for package in self.snow_packages]
            query_parts.append(f"PACKAGES=({','.join(packages)})")

        if self.snow_external_access_integrations:
            query_parts.append(
                f"EXTERNAL_ACCESS_INTEGRATIONS=({','.join(self.snow_external_access_integrations)})"
            )

        if self.snow_secrets:
            secrets = [
                f"{self._quote_value(reference_name)} = {secret_name}"
                for reference_name, secret_name in self.snow_secrets.items()
            ]
            query_parts.append(f"SECRETS=({', '.join(secrets)})")

        query_parts.extend(
            [
                "RESOURCE_CONSTRAINT='CPU_2X_X86'",
            ]
        )
        return " ".join(query_parts)


class SparkManager(SqlExecutionMixin):
    # todo: remove this once the image is released
    def _set_session_config(self, image: Optional[str]):
        #  image = "preprod8-scls.awsuswest2preprod8.registry-dev.snowflakecomputing.com/scls_cli_db/test_schema/cli_test_repo/cli_test:2.0"
        session_config = []
        if image:
            session_config = [
                f"""alter session set SPARK_APPLICATION_SPARK_IMAGES = '{{"1.0.0":"{image}"}}'"""
            ]
        for session_config_query in session_config:
            self.execute_query(session_config_query).fetchone()

    def submit(
        self,
        submit_query: str,
        image: Optional[str],
    ):
        try:
            self._set_session_config(image)
            result = self.execute_query(submit_query).fetchone()
            log.debug("Spark application submitted successfully")
            log.debug("Result: %s", result)
            return result[0]
        except Exception as e:
            raise ClickException(f"Failed to submit Spark application: {e}")

    def upload_file_to_stage(self, entrypoint_file: str, scls_file_stage: str):
        query = f"PUT file://{entrypoint_file} {scls_file_stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        log.debug("Uploading %s to %s", entrypoint_file, scls_file_stage)
        try:
            result = self.execute_query(query).fetchone()
            # schema: source, target, source_size, target_size, source_compression, target_compression, status, message
            file_name = result[1]
            log.debug("Result: %s", result)
            log.debug(
                "Uploaded %s to %s. file name: %s",
                entrypoint_file,
                scls_file_stage,
                file_name,
            )
            return file_name
        except Exception as e:
            raise ClickException(
                f"Failed to upload {entrypoint_file} to {scls_file_stage}: {e}"
            )

    def check_status(self, spark_application_id: str):
        unquoted_id = spark_application_id.strip("'").strip('"')
        query = f"SELECT * FROM TABLE(snowflake.spark.GET_SPARK_APPLICATION_HISTORY()) WHERE ID = '{unquoted_id}'"
        try:
            result = self.execute_query(query).fetchone()
            status = [
                f"ID: {result[0]}",
                f"Execution Status: {result[7]}",
                f"Error Message: {result[8]}",
                f"Error Code: {result[15]}",
                f"Exit Code: {result[16]}",
            ]
            return "\n".join(status)
        except Exception as e:
            raise ClickException(
                f"Failed to check status of {spark_application_id}: {e}"
            )

    def kill(self, spark_application_id: str):
        unquoted_id = spark_application_id.strip("'").strip('"')
        query = f"CALL SYSTEM$CANCEL_SPARK_APPLICATION('{unquoted_id}')"
        try:
            result = self.execute_query(query).fetchone()
            return result[0]
        except Exception as e:
            raise ClickException(f"Failed to kill {spark_application_id}: {e}")
