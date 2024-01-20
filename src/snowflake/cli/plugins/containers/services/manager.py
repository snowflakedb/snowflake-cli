from pathlib import Path
from typing import List, Optional, Tuple

from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor

from snowflake.cli.api.project.util import to_string_literal

class ServiceManager(SqlExecutionMixin):

    def create(
            self,
            service_name: str,
            compute_pool: str,
            spec_path: Path,
            num_instances: int,
            auto_resume: bool,
            external_access_integrations: Optional[List[str]],
            query_warehouse: Optional[str],
            tags: Optional[List[Tuple[str, str]]],
            comment: Optional[str]
    ) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)

        query = f"""\
            CREATE SERVICE IF NOT EXISTS {service_name}
            IN COMPUTE POOL {compute_pool}
            FROM SPECIFICATION $$
            {spec}
            $$
            WITH
            MIN_INSTANCES = {num_instances}
            MAX_INSTANCES = {num_instances}
            AUTO_RESUME = {auto_resume}
            """.split("\n")

        if external_access_integrations:
            external_access_integration_list = ",".join(
                f"{e}" for e in external_access_integrations
            )
            query.append(f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_list})")

        if query_warehouse:
            query.append(f"QUERY_WAREHOUSE = {query_warehouse}")

        if tags:
            tag_list = ",".join(
                f"{tag_name}={to_string_literal(tag_value)}" for tag_name, tag_value in tags
            )
            query.append(f"TAG ({tag_list})")

        if comment:
            query.append(f"COMMENT = {to_string_literal(comment)}")
        return self._execute_schema_query("\n".join(query))

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
