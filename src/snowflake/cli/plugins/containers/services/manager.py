from pathlib import Path
from typing import List, Optional

from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class ServiceManager(SqlExecutionMixin):
    def create(
        self,
        service_name: str,
        compute_pool: str,
        spec_path: Path,
        num_instances: int,
        external_access_integrations: Optional[List[str]]
    ) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)
        external_access_integration_name = ",".join(
            f"{e}" for e in external_access_integrations
        ) if external_access_integrations else ""

        return self._execute_schema_query(
            f"""\
            CREATE SERVICE IF NOT EXISTS {service_name}
            IN COMPUTE POOL {compute_pool}
            FROM SPECIFICATION $$
            {spec}
            $$
            WITH
            MIN_INSTANCES = {num_instances}
            MAX_INSTANCES = {num_instances}
            EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_name})
            """
        )

    def _read_yaml(self, path: Path) -> str:
        # TODO(aivanou): Add validation towards schema
        import json

        import yaml

        with open(path) as fh:
            data = yaml.safe_load(fh)
        return json.dumps(data)

    def status(self, service_name: str) -> SnowflakeCursor:
        return self._execute_schema_query(
            f"CALL SYSTEM$GET_SERVICE_STATUS('{service_name}')"
        )

    def logs(
        self, service_name: str, instance_id: str, container_name: str, num_lines: int
    ):
        return self._execute_schema_query(
            f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '{instance_id}', '{container_name}', {num_lines});"
        )
