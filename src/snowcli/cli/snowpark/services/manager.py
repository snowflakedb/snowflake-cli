import hashlib
import os
from pathlib import Path

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin


class ServiceManager(SqlExecutionMixin):
    def create(
        self,
        service_name: str,
        compute_pool: str,
        spec_path: Path,
        num_instances: int,
        stage: str,
    ) -> SnowflakeCursor:
        spec_filename = os.path.basename(spec_path)
        file_hash = hashlib.md5(open(spec_path, "rb").read()).hexdigest()
        stage_dir = os.path.join("jobs", file_hash)
        return self._execute_query(
            f"""\
            CREATE SERVICE IF NOT EXISTS {service_name}
            MIN_INSTANCES = {num_instances}
            MAX_INSTANCES = {num_instances}
            COMPUTE_POOL =  {compute_pool}
            spec=@{stage}/{stage_dir}/{spec_filename};
            """
        )

    def desc(self, service_name: str) -> SnowflakeCursor:
        return self._execute_query(f"desc service {service_name}")

    def show(self) -> SnowflakeCursor:
        return self._execute_query("show services")

    def status(self, service_name: str) -> SnowflakeCursor:
        return self._execute_query(f"CALL SYSTEM$GET_SERVICE_STATUS(('{service_name}')")

    def drop(self, service_name: str) -> SnowflakeCursor:
        return self._execute_query(f"drop service {service_name}")

    def logs(self, service_name: str, container_name: str):
        return self._execute_query(
            f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '0', '{container_name}');"
        )
