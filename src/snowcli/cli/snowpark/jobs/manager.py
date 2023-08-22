import hashlib
import os
from pathlib import Path

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin


class JobManager(SqlExecutionMixin):
    def create(self, compute_pool: str, spec_path: Path, stage: str) -> SnowflakeCursor:
        spec_filename = os.path.basename(spec_path)
        file_hash = hashlib.md5(open(spec_path, "rb").read()).hexdigest()
        stage_dir = os.path.join("jobs", file_hash)
        return self._execute_query(
            f"""\
        EXECUTE SERVICE
        COMPUTE_POOL =  {compute_pool}
        spec=@{stage}/{stage_dir}/{spec_filename};
        """
        )

    def desc(self, job_name: str) -> SnowflakeCursor:
        return self._execute_query(f"desc service {job_name}")

    def status(self, job_name: str) -> SnowflakeCursor:
        return self._execute_query(f"CALL SYSTEM$GET_JOB_STATUS('{job_name}')")

    def drop(self, job_name: str) -> SnowflakeCursor:
        return self._execute_query(f"CALL SYSTEM$CANCEL_JOB('{job_name}')")

    def logs(self, job_name: str, container_name: str):
        return self._execute_query(
            f"call SYSTEM$GET_JOB_LOGS('{job_name}', '{container_name}')"
        )
