from pathlib import Path

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class ServiceManager(SqlExecutionMixin):
    def create(
        self,
        service_name: str,
        compute_pool: str,
        spec_path: Path,
        num_instances: int,
    ) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)
        return self._execute_schema_query(
            f"""\
            CREATE SERVICE IF NOT EXISTS {service_name}
            IN COMPUTE POOL {compute_pool}
            FROM SPECIFICATION '
            {spec}
            '
            WITH
            MIN_INSTANCES = {num_instances}
            MAX_INSTANCES = {num_instances}
            """
        )

    def _read_yaml(self, path: Path) -> str:
        # TODO(aivanou): Add validation towards schema
        import yaml
        import json

        with open(path) as fh:
            data = yaml.safe_load(fh)
        return json.dumps(data)

    def desc(self, service_name: str) -> SnowflakeCursor:
        return self._execute_schema_query(f"desc service {service_name}")

    def show(self) -> SnowflakeCursor:
        return self._execute_schema_query("show services")

    def status(self, service_name: str) -> SnowflakeCursor:
        return self._execute_schema_query(
            f"CALL SYSTEM$GET_SERVICE_STATUS('{service_name}')"
        )

    def drop(self, service_name: str) -> SnowflakeCursor:
        return self._execute_schema_query(f"drop service {service_name}")

    def logs(
        self, service_name: str, instance_id: str, container_name: str, num_lines: int
    ):
        return self._execute_schema_query(
            f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '{instance_id}', '{container_name}', {num_lines});"
        )
