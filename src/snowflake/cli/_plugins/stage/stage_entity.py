from typing import Optional

from snowflake.cli._plugins.stage.stage_entity_model import KindType, StageEntityModel
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.entities.common import EntityBase
from snowflake.connector import SnowflakeConnection
from snowflake.core import CreateMode, Root
from snowflake.core.stage import Stage, StageCollection, StageResource


class StageEntity(EntityBase[StageEntityModel]):
    # TODO: discuss: twose 2 methods might go to parent class
    @staticmethod
    def get_root() -> Root:
        conn: SnowflakeConnection = get_cli_context().connection
        return Root(conn)

    # @contextmanager
    # def with_assumed_role(self, role: Optional[str]) -> Root:
    #     # TODO: consider creating a new connection instead
    #     if role is None:
    #         yield self.get_root()
    #     else:
    #         conn: SnowflakeConnection = get_cli_context().connection
    #         if conn.role.lower() == role.lower():
    #             yield self.get_root()
    #         else:
    #             conn = SnowflakeConnection()
    #             conn._role = role
    #             yield Root(conn)

    # TODO: discuss: those 2 methods could go to a mixing class for resources that support using snowapis
    #       figure out typing as we might often deal with <T>Collection, <T>Resource and <T>
    @classmethod
    def get_collection(cls, root: Optional[Root] = None) -> StageCollection:
        conn: SnowflakeConnection = get_cli_context().connection
        if root is None:
            root = cls.get_root()
        return root.databases[conn.database].schemas[conn.schema].stages

    def get_resource(self, resource_name: str, root: Optional[Root]) -> StageResource:
        return self.get_collection(root)[resource_name]

    # TODO: discuss: this code looks mostly generic so might go as well to the mixin class. Don't do it too fast to
    #       avoid issues of premature abstraction
    @classmethod
    def create(
        cls, name: str, comment: Optional[str] = None, temporary: bool = False
    ) -> StageEntityModel:
        stage_collection = cls.get_collection()
        stage = Stage(
            name=name,
            kind=KindType.TEMPORARY.value if temporary else KindType.PERMANENT.value,
            comment=comment,
        )
        stage_resource = stage_collection.create(stage, mode=CreateMode.if_not_exists)
        entity_model_cls = cls.get_entity_model_type()
        temp = stage_resource.fetch()

        entity_model = entity_model_cls(type="stage", **temp.to_dict())
        return entity_model
        # return entity_model(type="stage", api_resource=temp)

    # def remove(self, role: Optional[str] = None) -> None:
    #     with self.with_assumed_role(role) as root:
    #         stage_resource: StageResource = self.get_resource(self.model.name, root=root)
    #         stage_resource.drop()
    #
    # def clone(self):
    #     pass
    #
    # def get(self):
    #     pass
    #
    # def put(self):
    #     pass
