from pathlib import Path
from typing import List, Optional

from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.object.common import Tag
from snowflake.cli.plugins.spcs.common import (
    handle_object_already_exists,
    strip_empty_lines,
)
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


class ServiceManager(SqlExecutionMixin):
    def create(
        self,
        service_name: str,
        compute_pool: str,
        spec_path: Path,
        min_instances: int,
        max_instances: int,
        auto_resume: bool,
        external_access_integrations: Optional[List[str]],
        query_warehouse: Optional[str],
        tags: Optional[List[Tag]],
        comment: Optional[str],
    ) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)

        query = f"""\
            CREATE SERVICE {service_name}
            IN COMPUTE POOL {compute_pool}
            FROM SPECIFICATION $$
            {spec}
            $$
            MIN_INSTANCES = {min_instances}
            MAX_INSTANCES = {max_instances}
            AUTO_RESUME = {auto_resume}
            """.splitlines()

        if external_access_integrations:
            external_access_integration_list = ",".join(
                f"{e}" for e in external_access_integrations
            )
            query.append(
                f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_list})"
            )

        if query_warehouse:
            query.append(f"QUERY_WAREHOUSE = {query_warehouse}")

        if comment:
            query.append(f"COMMENT = {comment}")

        if tags:
            tag_list = ",".join(f"{t.name}={t.value_string_literal()}" for t in tags)
            query.append(f"WITH TAG ({tag_list})")

        try:
            return self._execute_schema_query(strip_empty_lines(query))
        except ProgrammingError as e:
            handle_object_already_exists(e, ObjectType.SERVICE, service_name)

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

    def suspend(self, service_name: str):
        return self._execute_schema_query(f"alter service {service_name} suspend")

    def resume(self, service_name: str):
        return self._execute_schema_query(f"alter service {service_name} resume")

    def upgrade_spec(self, service_name: str, spec_path: Path):
        spec = self._read_yaml(spec_path)
        query = f"""
        alter service {service_name}
        from specification $$
        {spec}
        $$
        """.splitlines()
        return self._execute_schema_query(strip_empty_lines(query))
