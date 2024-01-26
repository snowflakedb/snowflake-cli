from pathlib import Path

from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class JobManager(SqlExecutionMixin):
    def create(self, compute_pool: str, spec_path: Path) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)
        return self._execute_schema_query(
            f"""\
            EXECUTE SERVICE
            IN COMPUTE POOL {compute_pool}
            FROM SPECIFICATION $$
            {spec}
            $$
            """
        )

    def _read_yaml(self, path: Path) -> str:
        # TODO(aivanou): Add validation towards schema
        # TODO(aivanou): Combine this with service manager
        import json

        import yaml

        with open(path) as fh:
            data = yaml.safe_load(fh)
        return json.dumps(data)

    def status(self, job_name: str) -> SnowflakeCursor:
        return self._execute_query(f"CALL SYSTEM$GET_JOB_STATUS('{job_name}')")

    def logs(self, job_name: str, container_name: str):
        return self._execute_query(
            f"call SYSTEM$GET_JOB_LOGS('{job_name}', '{container_name}')"
        )
