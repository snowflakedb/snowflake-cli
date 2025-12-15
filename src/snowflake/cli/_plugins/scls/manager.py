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


class SclsManager(SqlExecutionMixin):
    def _set_session_config(self):
        session_config = [
            """alter session set SPARK_APPLICATION_SPARK_IMAGES = '{"1.0.0":"qa6-scls.awsuswest2qa6.registry-dev.snowflakecomputing.com/scls_test_db/test_schema/scls_test_repo/cli_test:1.0"}'""",
        ]
        for session_config_query in session_config:
            self.execute_query(session_config_query).fetchone()

    def submit(
        self,
        file_on_stage: str,
        application_arguments: Optional[List[str]],
        class_name: Optional[str],
        scls_file_stage: str,
    ):
        stage_name = (
            scls_file_stage
            if not scls_file_stage.endswith("/")
            else f"@{scls_file_stage.rstrip('/')}"
        )
        log.debug("Submitting Spark application")
        query_parts = [
            "EXECUTE SPARK APPLICATION",
            "ENVIRONMENT_RUNTIME_VERSION='1.0-preview'",
            f"STAGE_MOUNTS=('{stage_name}:/tmp/entrypoint')",
            f"ENTRYPOINT_FILE='/tmp/entrypoint/{file_on_stage}'",
            f"CLASS = '{class_name}'",  # todo: support python
            "SPARK_CONFIGURATIONS=('spark.plugins' = 'com.snowflake.spark.SnowflakePlugin', 'spark.snowflake.backend' = 'sparkle', 'spark.eventLog.enabled' = 'false')",
            "RESOURCE_CONSTRAINT='CPU_2X_X86'",
        ]
        query = " ".join(query_parts)
        try:
            self._set_session_config()
            result = self.execute_query(query).fetchone()
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
            return self.execute_query(query)
        except Exception as e:
            raise ClickException(
                f"Failed to check status of {spark_application_id}: {e}"
            )
