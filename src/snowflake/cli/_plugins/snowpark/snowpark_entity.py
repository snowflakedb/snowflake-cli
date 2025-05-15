from enum import Enum
from pathlib import Path
from typing import Generic, List, Optional, TypeVar

from click import ClickException
from snowflake.cli._plugins.snowpark import package_utils
from snowflake.cli._plugins.snowpark.common import (
    DEFAULT_RUNTIME,
    map_path_mapping_to_artifact,
    zip_and_copy_artifacts_to_deploy,
)
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
from snowflake.cli._plugins.snowpark.snowpark_project_paths import SnowparkProjectPaths
from snowflake.cli._plugins.snowpark.zipper import zip_dir
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import ProgrammingError

T = TypeVar("T")


class CreateMode(
    str, Enum
):  # This should probably be moved to some common place, think where
    create = "CREATE"
    create_or_replace = "CREATE OR REPLACE"
    create_if_not_exists = "CREATE IF NOT EXISTS"


class SnowparkEntity(EntityBase[Generic[T]]):
    def __init__(self, *args, **kwargs):

        if not FeatureFlag.ENABLE_NATIVE_APP_CHILDREN.is_enabled():
            raise NotImplementedError("Snowpark entity is not implemented yet")
        super().__init__(*args, **kwargs)

    def action_bundle(
        self,
        action_ctx: ActionContext,
        ignore_anaconda: bool,
        skip_version_check: bool,
        output_dir: Path | None = None,
        index_url: str | None = None,
        allow_shared_libraries: bool = False,
        *args,
        **kwargs,
    ) -> List[Path]:
        return self.bundle(
            ignore_anaconda,
            skip_version_check,
            output_dir,
            index_url,
            allow_shared_libraries,
        )

    def action_deploy(
        self,
        action_ctx: ActionContext,
        mode: CreateMode = CreateMode.create,
        *args,
        **kwargs,
    ):
        # TODO: After introducing bundle map, we should introduce file copying part here
        return self.deploy(mode, *args, **kwargs)

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._execute_query(self.get_drop_sql())

    def action_describe(self, action_ctx: ActionContext, *args, **kwargs):
        return self._execute_query(self.get_describe_sql())

    def action_execute(
        self,
        action_ctx: ActionContext,
        execution_arguments: List[str] | None = None,
        *args,
        **kwargs,
    ):
        return self._execute_query(self.get_execute_sql(execution_arguments))

    def bundle(
        self,
        ignore_anaconda: bool,
        skip_version_check: bool,
        output_dir: Path | None = None,
        index_url: str | None = None,
        allow_shared_libraries: bool = False,
    ) -> List[Path]:
        """
        Bundles the entity artifacts and dependencies into a directory.
        Parameters:
            output_dir: The directory to output the bundled artifacts to. Defaults to output dir in project root
            ignore_anaconda: If True, ignores anaconda check and tries to download all packages using pip
            skip_version_check: If True, skips version check when downloading packages
            index_url: The index URL to use when downloading packages, if none set - default pip index is used (in most cases- Pypi)
            allow_shared_libraries: If not set to True, using dependency with .so/.dll files will raise an exception
        Returns:
        """
        # 0 Create a directory for the entity
        project_paths = SnowparkProjectPaths(
            project_root=self.root.absolute(),
        )
        if not output_dir:
            output_dir = project_paths.bundle_root
        if not output_dir.exists():  # type: ignore[union-attr]
            SecurePath(output_dir).mkdir(parents=True)

        # 1 Check if requirements exits
        if (self.root / "requirements.txt").exists():
            download_results = self._process_requirements(
                bundle_dir=output_dir,  # type: ignore
                archive_name="dependencies.zip",
                requirements_file=SecurePath(self.root / "requirements.txt"),
                ignore_anaconda=ignore_anaconda,
                skip_version_check=skip_version_check,
                index_url=index_url,
                allow_shared_libraries=allow_shared_libraries,
            )

        # 2 get the artifacts list
        artifacts = map_path_mapping_to_artifact(project_paths, self.model.artifacts)
        from snowflake.cli.api.feature_flags import FeatureFlag

        if FeatureFlag.ENABLE_SNOWPARK_GLOB_SUPPORT.is_enabled():
            return zip_and_copy_artifacts_to_deploy(
                artifacts, project_paths.bundle_root
            )
        else:
            copied_files = []
            for artifact in artifacts:
                artifact.build()
                copied_files.append(artifact.post_build_path)
            return copied_files

    def check_if_exists(
        self, action_ctx: ActionContext
    ) -> bool:  # TODO it should return current state, so we know if update is necessary
        try:
            current_state = self.action_describe(action_ctx)
            return True
        except ProgrammingError:
            return False

    def deploy(self, mode: CreateMode = CreateMode.create, *args, **kwargs):
        return self._execute_query(self.get_deploy_sql(mode))

    def get_deploy_sql(self, mode: CreateMode):
        query = [
            f"{mode.value} {self.model.type.upper()} {self.identifier}",
            "COPY GRANTS",
            f"RETURNS {self.model.returns}",
            f"LANGUAGE PYTHON",
            f"RUNTIME_VERSION '{self.model.runtime or DEFAULT_RUNTIME}'",
            f"IMPORTS={','.join(self.model.imports)}",  # TODO: Add source files here after introducing bundlemap
            f"HANDLER='{self.model.handler}'",
        ]

        if self.model.external_access_integrations:
            query.append(self.model.get_external_access_integrations_sql())

        if self.model.secrets:
            query.append(self.model.get_secrets_sql())

        if self.model.type == "procedure" and self.model.execute_as_caller:
            query.append("EXECUTE AS CALLER")

        if self.model.artifact_repository and self.model.artifact_repository_packages:
            packages = [f"'{item}'" for item in self.model.artifact_repository_packages]
            query.extend(
                [
                    f"ARTIFACT_REPOSITORY= {self.model.artifact_repository} ",
                    f"ARTIFACT_REPOSITORY_PACKAGES=({','.join(packages)})",
                ]
            )
        if self.model.resource_constraint:
            query.append(self._get_resource_constraints_sql())

        return "\n".join(query)

    def get_execute_sql(self, execution_arguments: List[str] | None = None):
        raise NotImplementedError

    def _process_requirements(  # TODO: maybe leave all the logic with requirements here - so download, write requirements file etc.
        self,
        bundle_dir: Path,
        archive_name: str,  # TODO: not the best name, think of something else
        requirements_file: Optional[SecurePath],
        ignore_anaconda: bool,
        skip_version_check: bool = False,
        index_url: Optional[str] = None,
        allow_shared_libraries: bool = False,
    ) -> DownloadUnavailablePackagesResult:
        """
        Processes the requirements file and downloads the dependencies
        Parameters:

        """
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

    def _get_resource_constraints_sql(self) -> str:
        if self.model.resource_constraint:
            constraints = ",".join(
                f"{key}='{value}'"
                for key, value in self.model.resource_constraint.items()
            )
            return f"RESOURCE_CONSTRAINT=({constraints})"
        return ""


class FunctionEntity(SnowparkEntity[FunctionEntityModel]):
    """
    A single UDF
    """

    # TO THINK OF
    # Where will we get imports? Should we rely on bundle map? Or should it be self-sufficient in this matter?

    def get_execute_sql(
        self, execution_arguments: List[str] | None = None, *args, **kwargs
    ):
        if not execution_arguments:
            execution_arguments = []
        return (
            f"SELECT {self.fqn}({', '.join([str(arg) for arg in execution_arguments])})"
        )


class ProcedureEntity(SnowparkEntity[ProcedureEntityModel]):
    """
    A stored procedure
    """

    def get_execute_sql(
        self,
        execution_arguments: List[str] | None = None,
    ):
        if not execution_arguments:
            execution_arguments = []
        return (
            f"CALL {self.fqn}({', '.join([str(arg) for arg in execution_arguments])})"
        )
