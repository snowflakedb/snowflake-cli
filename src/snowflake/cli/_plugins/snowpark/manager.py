from __future__ import annotations

from click import UsageError
from snowflake.cli._plugins.snowpark.common import DEFAULT_RUNTIME, SnowparkObject
from snowflake.cli.api.project.schemas.entities.snowpark_entity import (
    ProcedureEntityModel,
    SnowparkEntityModel,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class SnowparkObjectManager(SqlExecutionMixin):
    def execute(
        self, execution_identifier: str, object_type: SnowparkObject
    ) -> SnowflakeCursor:
        if object_type == SnowparkObject.FUNCTION:
            return self._execute_query(f"select {execution_identifier}")
        if object_type == SnowparkObject.PROCEDURE:
            return self._execute_query(f"call {execution_identifier}")
        raise UsageError(f"Unknown object type: {object_type}.")

    def create_or_replace(
        self,
        entity: SnowparkEntityModel,
        artifact_files: set[str],
        snowflake_dependencies: list[str],
    ) -> str:
        entity.imports.extend(artifact_files)
        imports = [f"'{x}'" for x in entity.imports]
        packages_list = ",".join(f"'{p}'" for p in snowflake_dependencies)

        object_type = entity.get_type()

        query = [
            f"create or replace {object_type} {entity.udf_sproc_identifier.identifier_for_sql}",
            f"copy grants",
            f"returns {entity.returns}",
            "language python",
            f"runtime_version={entity.runtime or DEFAULT_RUNTIME}",
            f"imports=({', '.join(imports)})",
            f"handler='{entity.handler}'",
            f"packages=({packages_list})",
        ]

        if entity.external_access_integrations:
            query.append(entity.get_external_access_integrations_sql())

        if entity.secrets:
            query.append(entity.get_secrets_sql())

        if isinstance(entity, ProcedureEntityModel) and entity.execute_as_caller:
            query.append("execute as caller")

        return self._execute_query("\n".join(query))
