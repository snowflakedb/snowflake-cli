import functools
from pathlib import Path
from typing import Generic, List, Optional, TypeVar

from click import ClickException


from snowflake.cli._plugins.snowpark import package_utils
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AnacondaPackagesManager,
)
from snowflake.cli._plugins.snowpark.package_utils import (
    DownloadUnavailablePackagesResult,
)
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli._plugins.snowpark.zipper import zip_dir
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
    def _sql_executor(
        self,
    ):  # maybe this could be moved to parent class, as it is used in streamlit entity as well
        return get_sql_executor()

    @functools.cached_property
    def _conn(self):
        return self._sql_executor._conn  # noqa

    @property
    def model(self):
        return self._entity_model  # noqa

    def action_bundle(self, action_ctx: ActionContext, output_dir: Path | None, ignore_anaconda: bool, skip_version_check: bool, index_url: str | None = None, allow_shared_libraries: bool = False,  *args, **kwargs):
        return self.bundle(output_dir, ignore_anaconda, skip_version_check, index_url, allow_shared_libraries)

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        pass

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._sql_executor.execute_query(self.get_drop_sql())

    def action_describe(self, action_ctx: ActionContext, *args, **kwargs):
        return self._sql_executor.execute_query(self.get_describe_sql())

    def action_execute(
        self,
        action_ctx: ActionContext,
        execution_arguments: List[str] = None,
        *args,
        **kwargs,
    ):
        return self._sql_executor.execute_query(
            self.get_execute_sql(execution_arguments)
        )

    def bundle(self, output_dir: Path | None, ignore_anaconda: bool, skip_version_check: bool, index_url: str | None = None, allow_shared_libraries: bool = False) -> List[Path]:
        '''
        Bundles the entity artifacts and dependencies into a directory.
        Parameters:
            output_dir: The directory to output the bundled artifacts to. Defaults to otput dir in project root
            ignore_anaconda: If True, ignores anaconda chceck and tries to download all packages using pip
            skip_version_check: If True, skips version check when downloading packages
            index_url: The index URL to use when downloading packages, if none set - default pip index is used (in most cases- Pypi)
            allow_shared_libraries: If not set to True, using dependency with .so/.dll files will raise an exception
        Returns:
        '''
        # 0 Create a directory for the entity
        if not output_dir:
            output_dir = self.root / "output" / self.model.stage
        output_dir.mkdir(parents=True, exist_ok=True)

        output_files = []

        # 1 Check if requirements exits
        if (self.root / "requirements.txt").exists():
            download_results = self._process_requirements(
                bundle_dir= output_dir,
                archive_name= "dependencies.zip",
                requirements_file=SecurePath(self.root / "requirements.txt"),
                ignore_anaconda=ignore_anaconda,
                skip_version_check=skip_version_check,
                index_url=index_url,
                allow_shared_libraries=allow_shared_libraries)


        # 3 get the artifacts list
        artifacts = self.model.artifacts

        for artifact in artifacts:
            output_file = output_dir / artifact.dest / artifact.src.name

            if artifact.src.is_file():
                output_file.touch(exist_ok=True)
                SecurePath(artifact.src).copy(output_file)
            elif artifact.is_dir():
                output_file.mkdir(parents=True, exist_ok=True)

            output_files.append(output_file)

        return output_files

        pass

    def get_deploy_sql(self):
        raise NotImplementedError

    def get_describe_sql(self):
        raise NotImplementedError

    def get_drop_sql(self):
        raise NotImplementedError

    def get_execute_sql(self):
        raise NotImplementedError

    def get_usage_grant_sql(self):
        pass

    def _process_requirements( #TODO: maybe leave all the logic with requirements here - so download, write requirements file etc.
        self,
        bundle_dir: Path,
        archive_name: str,  # TODO: not the best name, think of something else
        requirements_file: Optional[SecurePath],
        ignore_anaconda: bool,
        skip_version_check: bool = False,
        index_url: Optional[str] = None,
        allow_shared_libraries: bool = False,
    ) -> DownloadUnavailablePackagesResult:
        '''
        Processes the requirements file and downloads the dependencies
        Parameters:

        '''
        anaconda_packages_manager = AnacondaPackagesManager()
        with SecurePath.temporary_directory() as tmp_dir:
            requirements = package_utils.parse_requirements(requirements_file)
            anaconda_packages = (
                AnacondaPackages.empty()
                if ignore_anaconda
                else anaconda_packages_manager.find_packages_available_in_snowflake_anaconda()
            )
            download_result = package_utils.download_unavailable_packages(
                requirements=requirements,
                target_dir=tmp_dir,
                anaconda_packages=anaconda_packages,
                skip_version_check=skip_version_check,
                pip_index_url=index_url,
            )

            if download_result.anaconda_packages:
                anaconda_packages.write_requirements_file_in_snowflake_format(
                    file_path=SecurePath(bundle_dir / "requirements.txt"),
                    requirements=download_result.anaconda_packages,
                )

            if download_result.downloaded_packages_details:
                if (
                    package_utils.detect_and_log_shared_libraries(
                        download_result.downloaded_packages_details
                    )
                    and not allow_shared_libraries
                ):
                    raise ClickException(
                        "Some packages contain shared (.so/.dll) libraries. "
                        "Try again with allow_shared_libraries_flag."
                    )

                zip_dir(
                    source=tmp_dir,
                    dest_zip=bundle_dir / archive_name,
                )

        return download_result



class FunctionEntity(SnowparkEntity[FunctionEntityModel]):
    """
    A single UDF
    """
    def get_describe_sql(self):
        return f"DESCRIBE FUNCTION {self.identifier}"

    def get_drop_sql(self):
        return f"DROP FUNCTION {self.identifier}"

    def get_deploy_sql(self):
        pass

    def get_execute_sql(self, execution_arguments: List[str] = None, *args, **kwargs):
        if not execution_arguments:
            execution_arguments = []
        return (
            f"SELECT {self.fqn}({', '.join([str(arg) for arg in execution_arguments])})"
        )



class ProcedureEntity(SnowparkEntity[ProcedureEntityModel]):
    """
    A stored procedure
    """
    def get_describe_sql(self):
        return f"DESCRIBE PROCEDURE {self.identifier}"

    def get_drop_sql(self):
        return f"DROP PROCEDURE {self.identifier}"

    def get_deploy_sql(self):
        pass

    def get_execute_sql(
        self,
        execution_arguments: List[str] = None,
    ):
        if not execution_arguments:
            execution_arguments = []
        return (
            f"CALL {self.fqn}({', '.join([str(arg) for arg in execution_arguments])})"
        )
