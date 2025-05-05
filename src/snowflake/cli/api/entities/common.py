import functools
import logging
from pathlib import Path
from typing import Generic, List, Optional, Type, TypeVar, get_args

from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.cli_global_context import get_cli_context, span
from snowflake.cli.api.entities.resolver import Dependency, DependencyResolver
from snowflake.cli.api.entities.utils import EntityActions, get_sql_executor
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.cli.api.utils.path_utils import change_directory
from snowflake.cli.api.utils.python_api_utils import StageEncryptionType
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.core import CreateMode
from snowflake.core.stage import Stage, StageEncryption, StageResource

T = TypeVar("T")

log = logging.getLogger(__name__)


def attach_spans_to_entity_actions(entity_name: str):
    """
    Class decorator for EntityBase subclasses to automatically wrap
    every implemented entity action method with a metrics span

    Args:
        entity_name (str): Custom name for entity type to be displayed in metrics
    """

    def decorator(cls: type[T]) -> type[T]:
        for attr_name, attr_value in vars(cls).items():
            is_entity_action = attr_name in [
                enum_member for enum_member in EntityActions
            ]

            if is_entity_action and callable(attr_value):
                attr_name_without_action_prefix = attr_name.partition("_")[2]
                span_name = f"action.{entity_name}.{attr_name_without_action_prefix}"
                action_with_span = span(span_name)(attr_value)
                setattr(cls, attr_name, action_with_span)
        return cls

    return decorator


class EntityBase(Generic[T]):
    """
    Base class for the fully-featured entity classes.
    """

    def __init__(self, entity_model: T, workspace_ctx: WorkspaceContext):
        self._entity_model = entity_model
        self._workspace_ctx = workspace_ctx
        self.dependency_resolver = DependencyResolver(entity_model)

        self._stage_object = None

    @property
    def entity_id(self) -> str:
        return self._entity_model.entity_id  # type: ignore

    @classmethod
    def get_entity_model_type(cls) -> Type[T]:
        """
        Returns the generic model class specified in each entity class.

        For example, calling ApplicationEntity.get_entity_model_type() will return the ApplicationEntityModel class.
        """
        return get_args(cls.__orig_bases__[0])[0]  # type: ignore[attr-defined]

    def supports(self, action: EntityActions) -> bool:
        """
        Checks whether this entity supports the given action. An entity is considered to support an action if it implements a method with the action name.
        """
        return callable(getattr(self, action, None))

    def perform(
        self, action: EntityActions, action_ctx: ActionContext, *args, **kwargs
    ):
        """
        Performs the requested action.
        This is a preferred way to perform actions on entities, over calling actions directly,
        as it will also call the dependencies in the correct order.
        """
        self.dependency_resolver.perform_for_dep(action, action_ctx, *args, **kwargs)
        return getattr(self, action)(action_ctx, *args, **kwargs)

    @property
    def root(self) -> Path:
        return self._workspace_ctx.project_root

    @property
    def identifier(self) -> str:
        return self.model.fqn.sql_identifier  # type: ignore[attr-defined]

    @property
    def fqn(self) -> FQN:
        return self._entity_model.fqn  # type: ignore[attr-defined]

    @functools.cached_property
    def _sql_executor(
        self,
    ) -> SqlExecutor:
        return get_sql_executor()

    def _execute_query(self, sql: str) -> SnowflakeCursor:
        return self._sql_executor.execute_query(sql)

    @functools.cached_property
    def _conn(self) -> SnowflakeConnection:
        return self._sql_executor._conn  # noqa

    @property
    def database(self) -> Optional[str]:
        return self.get_from_fqn_or_conn("database")

    @property
    def schema(self) -> Optional[str]:
        return self.get_from_fqn_or_conn("schema")

    @property
    def snow_api_root(self) -> Optional[object]:
        root = get_cli_context().snow_api_root
        if root is None:
            raise ValueError("snow_api_root is not set")
        return root

    @property
    def stage_object(self) -> "StageResource":
        if self._stage_object is None:
            self._stage_object = self._create_stage_if_not_exists()

        return self._stage_object

    @property
    def model(self) -> T:
        return self._entity_model

    def dependent_entities(self, action_ctx: ActionContext) -> List[Dependency]:
        return self.dependency_resolver.depends_on(action_ctx)

    def get_usage_grant_sql(self, app_role: str) -> str:
        return f"GRANT USAGE ON {self.model.type.upper()} {self.identifier} TO ROLE {app_role};"  # type: ignore[attr-defined]

    def get_describe_sql(self) -> str:
        return f"DESCRIBE {self.model.type.upper()} {self.identifier};"  # type: ignore[attr-defined]

    def get_drop_sql(self) -> str:
        return f"DROP {self.model.type.upper()} {self.identifier};"  # type: ignore[attr-defined]

    def _create_stage_if_not_exists(
        self, stage_name: Optional[str] = None
    ) -> StageResource:
        if stage_name is None:
            stage_name = self.model.stage  # type: ignore[attr-defined]

        stage_collection = (
            self.snow_api_root.databases[self.database].schemas[self.schema].stages  # type: ignore[attr-defined]
        )
        stage_object = Stage(
            name=stage_name,
            encryption=StageEncryption(type=StageEncryptionType.SNOWFLAKE_SSE.value),
        )

        return stage_collection.create(stage_object, mode=CreateMode.if_not_exists)

    def _get_identifier(
        self, schema: Optional[str] = None, database: Optional[str] = None
    ) -> str:
        schema_to_use = schema or self._entity_model.fqn.schema or self._conn.schema  # type: ignore
        db_to_use = database or self._entity_model.fqn.database or self._conn.database  # type: ignore
        return f"{self._entity_model.fqn.set_schema(schema_to_use).set_database(db_to_use).sql_identifier}"  # type: ignore

    def _upload_files_to_stage(
        self,
        stage: StageResource,
        bundle_map: BundleMap,
        stage_root: Optional[str] = None,
    ) -> None:
        with change_directory(self.root):
            for src, dest in bundle_map.all_mappings(
                absolute=True, expand_directories=True
            ):
                if src.is_file():
                    upload_dst = (
                        f"{stage_root}/{dest.relative_to(self.root)}"
                        if stage_root
                        else f"/{self.fqn.name}/{get_parent_path_for_stage_deployment(dest.relative_to(bundle_map.deploy_root()))}"
                    )

                    stage.put(
                        local_file_name=src.relative_to(self.root),
                        stage_location=upload_dst,
                        overwrite=True,
                        auto_compress=False,
                    )

    def get_from_fqn_or_conn(self, attribute_name: str) -> str:
        attribute = getattr(self.fqn, attribute_name, None) or getattr(
            self._conn, attribute_name, None
        )
        if not attribute:
            raise ValueError(
                f"Could not determine {attribute_name} for {self.entity_id}"
            )
        return attribute


def get_parent_path_for_stage_deployment(path: Path) -> str:
    return "/".join(path.parent.parts) if path.parent != Path(".") else ""
