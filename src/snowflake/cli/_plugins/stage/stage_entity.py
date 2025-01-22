from typing import Optional

from snowflake.cli._plugins.stage.stage_entity_model import KindType, StageEntityModel
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.entities.common import EntityBase
from snowflake.connector import SnowflakeConnection
from snowflake.core import CreateMode, Root
from snowflake.core.stage import Stage


class StageEntity(EntityBase[StageEntityModel]):
    def create(
        self, name: str, comment: Optional[str] = None, temporary: bool = False
    ) -> StageEntityModel:
        # TODO: access root_object from EntityBase
        conn: SnowflakeConnection = get_cli_context().connection
        root = Root(conn)

        stages_collection = root.databases[conn.database].schemas[conn.schema].stages
        stage = Stage(
            name=name,
            kind=KindType.TEMPORARY if temporary else KindType.PERMANENT,
            comment=comment,
        )
        stage_resource = stages_collection.create(stage, mode=CreateMode.if_not_exists)
        stage = stage_resource.fetch()
        return self.model(api_resource=stage)
