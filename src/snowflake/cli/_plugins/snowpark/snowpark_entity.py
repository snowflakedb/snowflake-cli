import functools
from pathlib import Path
from typing import Generic, TypeVar, List, Optional

from requests.packages import package

from snowflake.cli._plugins.snowpark import package_utils
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.secure_path import SecurePath
from test_data.test_data import requirements, anaconda_response

T = TypeVar("T")


class SnowparkEntity(EntityBase[Generic[T]]):

    @property
    def root(self):
        return self._workspace_ctx.project_root

    @property
    def identifier(self):
        return self.model.fqn.sql_identifier

    @property
    def fqn(self):
        return self.model.fqn

    @functools.cached_property
    def _sql_executor(self): # maybe this could be moved to parent class, as it is used in streamlit entity as well
        return get_sql_executor()

    @functools.cached_property
    def _conn(self):
        return self._sql_executor._conn  # noqa

    @property
    def model(self):
        return self._entity_model  # noqa

    def action_bundle(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._sql_executor.execute_query(self.get_drop_sql())

    def action_describe(self, action_ctx: ActionContext, *args, **kwargs):
        return self._sql_executor.execute_query(self.get_describe_sql())

    def action_execute(self, action_ctx: ActionContext, execution_arguments: List[str] = None, *args, **kwargs):
        return self._sql_executor.execute_query(self.get_execute_sql(execution_arguments))

    def bundle(self, output_dir: Optional[Path]):
        #0 Create a directory for the entity
        if not output_dir:
            output_dir = self.root / "output" / self.model.stage
        output_dir.mkdir(parents=True, exist_ok=True)

        output_files = []

        #1 Check if requirements exits
        if (self.root / "requirements.txt").exists():

        #2 If so- parse the requirements file and prepare

        #3 get the artifacts list
        artifacts = self.model.artifacts

        for artifact in artifacts:
            output_file = output_dir / artifact.name

            if artifact.is_file():
                SecurePath(artifact).copy(output_file)
            elif artifact.is_dir():
                output_file.mkdir(parents=True, exist_ok=True)

            output_files.append(output_file)

        return output_files


        pass
    def get_deploy_sql(self):
        pass

    def get_describe_sql(self):
        return f"DESCRIBE {self.identifier}"

    def get_drop_sql(self):
        return f"DROP {self.identifier}"

    def get_execute_sql(self):
        raise NotImplementedError

    def get_usage_grant_sql(self):
        pass

    def _process_requirements(self, requirements_file: Optional[Path]):
        with SecurePath.temporary_directory as tmp_dir:
            requirements = package_utils.parse_requirements(requirements_file)
            anaconda_packages


class FunctionEntity(SnowparkEntity[FunctionEntityModel]):
    """
    A single UDF
    """

    def get_execute_sql(self, execution_arguments: List[str] = None, *args, **kwargs):
        if not execution_arguments:
            execution_arguments = []
        return f"SELECT {self.fqn}({', '.join([str(arg) for arg in execution_arguments])})"


class ProcedureEntity(SnowparkEntity[ProcedureEntityModel]):
    """
    A stored procedure
    """

    def get_execute_sql(self, execution_arguments: List[str] = None, ):
        if not execution_arguments:
            execution_arguments = []
        return f"CALL {self.fqn}({', '.join([str(arg) for arg in execution_arguments])})"
