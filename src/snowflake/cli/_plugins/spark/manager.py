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
from typing import List, Optional

from click import ClickException
from snowflake.cli.api.sql_execution import SqlExecutionMixin

log = logging.getLogger(__name__)


class SubmitQueryBuilder:
    def __init__(self, file_on_stage: str, scls_file_stage: str):
        self.file_on_stage = file_on_stage
        self.scls_file_stage = scls_file_stage

    def with_class_name(self, class_name: Optional[str]) -> "SubmitQueryBuilder":
        self.class_name = class_name
        return self

    def with_application_arguments(
        self, application_arguments: Optional[List[str]]
    ) -> "SubmitQueryBuilder":
        self.application_arguments = application_arguments
        return self

    def build(self) -> str:
        stage_name = (
            self.scls_file_stage
            if not self.scls_file_stage.endswith("/")
            else f"@{self.scls_file_stage.rstrip('/')}"
        )

        query_parts = [
            "EXECUTE SPARK APPLICATION",
            "ENVIRONMENT_RUNTIME_VERSION='1.0-preview'",
            f"STAGE_MOUNTS=('{stage_name}:/tmp/entrypoint')",
            f"ENTRYPOINT_FILE='/tmp/entrypoint/{self.file_on_stage}'",
        ]

        # Scala/Java applications require a main class name
        if self.file_on_stage.endswith(".jar"):
            if not self.class_name:
                raise ClickException(
                    "Main class name is required for Scala/Java applications"
                )
            query_parts.append(f"CLASS = '{self.class_name}'")

        if self.application_arguments and len(self.application_arguments) > 0:
            escaped_args = [
                "'" + arg.replace("'", "\\'") + "'"
                for arg in self.application_arguments
            ]
            query_parts.append(f"ARGUMENTS = ({','.join(escaped_args)})")

        query_parts.extend(
            [
                "SPARK_CONFIGURATIONS=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle', 'spark.eventLog.enabled' = 'false')",
                "RESOURCE_CONSTRAINT='CPU_2X_X86'",
            ]
        )
        return " ".join(query_parts)


class SparkManager(SqlExecutionMixin):
    # todo: remove this once the image is released
    def _set_session_config(self):
        session_config = [
            """alter session set SPARK_APPLICATION_SPARK_IMAGES = '{"1.0.0":"qa6-scls.awsuswest2qa6.registry-dev.snowflakecomputing.com/scls_test_db/test_schema/scls_test_repo/cli_test:1.0"}'""",
        ]
        for session_config_query in session_config:
            self.execute_query(session_config_query).fetchone()

    def submit(
        self,
        submit_query: str,
    ):
        try:
            self._set_session_config()
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
        query = f"SELECT * FROM TABLE(snowflake.spark.GET_SPARK_APPLICATION_HISTORY()) WHERE ID = '{spark_application_id}'"
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
