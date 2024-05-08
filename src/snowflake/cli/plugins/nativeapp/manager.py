from __future__ import annotations

import os
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import jinja2
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.definition import (
    default_app_package,
    default_application,
    default_role,
)
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.util import (
    extract_schema,
    to_identifier,
    unquote_identifier,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import make_snowsight_url
from snowflake.cli.plugins.nativeapp.artifacts import (
    ArtifactDeploymentMap,
    ArtifactMapping,
    build_bundle,
    resolve_without_follow,
    source_path_to_deploy_path,
    translate_artifact,
)
from snowflake.cli.plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    ERROR_MESSAGE_606,
    ERROR_MESSAGE_2043,
    INTERNAL_DISTRIBUTION,
    NAME_COL,
    OWNER_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    InvalidPackageScriptError,
    MissingPackageScriptError,
    UnexpectedOwnerError,
)
from snowflake.cli.plugins.nativeapp.utils import verify_exists, verify_no_directories
from snowflake.cli.plugins.stage.diff import (
    DiffResult,
    StagePath,
    compute_stage_diff,
    preserve_from_diff,
    sync_local_diff_with_stage,
    to_stage_path,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor


def generic_sql_error_handler(
    err: ProgrammingError, role: Optional[str] = None, warehouse: Optional[str] = None
):
    # Potential refactor: If moving away from Python 3.8 and 3.9 to >= 3.10, use match ... case
    if err.errno == 2043 or err.msg.__contains__(ERROR_MESSAGE_2043):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                '{role}' may not have access to warehouse '{warehouse}'.
                Please grant usage privilege on warehouse to this role.
                """
            ),
            errno=err.errno,
        )
    elif err.errno == 606 or err.msg.__contains__(ERROR_MESSAGE_606):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please provide a warehouse for the active session role in your project definition file, config.toml file, or via command line.
                """
            ),
            errno=err.errno,
        )
    elif err.msg.__contains__("does not exist or not authorized"):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please check the name of the resource you are trying to query or the permissions of the role you are using to run the query.
                """
            )
        )
    raise err


def ensure_correct_owner(row: dict, role: str, obj_name: str) -> None:
    """
    Check if an object has the right owner role
    """
    actual_owner = row[
        OWNER_COL
    ].upper()  # Because unquote_identifier() always returns uppercase str
    if actual_owner != unquote_identifier(role):
        raise UnexpectedOwnerError(obj_name, role, actual_owner)


def _get_stage_paths_to_sync(
    local_paths_to_sync: List[Path], deploy_root: Path
) -> List[StagePath]:
    """
    Takes a list of paths (files and directories), returning a list of all files recursively relative to the deploy root.
    """

    stage_paths = []
    for path in local_paths_to_sync:
        if path.is_dir():
            for current_dir, _dirs, files in os.walk(path):
                for file in files:
                    deploy_path = Path(current_dir, file).relative_to(deploy_root)
                    stage_paths.append(to_stage_path(deploy_path))
        else:
            stage_paths.append(to_stage_path(path.relative_to(deploy_root)))
    return stage_paths


class NativeAppCommandProcessor(ABC):
    @abstractmethod
    def process(self, *args, **kwargs):
        pass


class NativeAppManager(SqlExecutionMixin):
    """
    Base class with frequently used functionality already implemented and ready to be used by related subclasses.
    """

    def __init__(self, project_definition: NativeApp, project_root: Path):
        super().__init__()
        self._project_root = project_root
        self._project_definition = project_definition

    @property
    def project_root(self) -> Path:
        return self._project_root

    @property
    def definition(self) -> NativeApp:
        return self._project_definition

    @cached_property
    def artifacts(self) -> List[ArtifactMapping]:
        return [translate_artifact(item) for item in self.definition.artifacts]

    @cached_property
    def deploy_root(self) -> Path:
        return Path(self.project_root, self.definition.deploy_root)

    @cached_property
    def package_scripts(self) -> List[str]:
        """
        Relative paths to package scripts from the project root.
        """
        if self.definition.package and self.definition.package.scripts:
            return self.definition.package.scripts
        else:
            return []

    @cached_property
    def stage_fqn(self) -> str:
        return f"{self.package_name}.{self.definition.source_stage}"

    @cached_property
    def stage_schema(self) -> Optional[str]:
        return extract_schema(self.stage_fqn)

    @cached_property
    def package_warehouse(self) -> Optional[str]:
        if self.definition.package and self.definition.package.warehouse:
            return self.definition.package.warehouse
        else:
            return self._conn.warehouse

    @cached_property
    def application_warehouse(self) -> Optional[str]:
        if self.definition.application and self.definition.application.warehouse:
            return self.definition.application.warehouse
        else:
            return self._conn.warehouse

    @cached_property
    def project_identifier(self) -> str:
        # name is expected to be a valid Snowflake identifier, but PyYAML
        # will sometimes strip out double quotes so we try to get them back here.
        return to_identifier(self.definition.name)

    @cached_property
    def package_name(self) -> str:
        if self.definition.package and self.definition.package.name:
            return to_identifier(self.definition.package.name)
        else:
            return to_identifier(default_app_package(self.project_identifier))

    @cached_property
    def package_role(self) -> str:
        if self.definition.package and self.definition.package.role:
            return self.definition.package.role
        else:
            return self._default_role

    @cached_property
    def package_distribution(self) -> str:
        if self.definition.package and self.definition.package.distribution:
            return self.definition.package.distribution.lower()
        else:
            return "internal"

    @cached_property
    def app_name(self) -> str:
        if self.definition.application and self.definition.application.name:
            return to_identifier(self.definition.application.name)
        else:
            return to_identifier(default_application(self.project_identifier))

    @cached_property
    def app_role(self) -> str:
        if self.definition.application and self.definition.application.role:
            return self.definition.application.role
        else:
            return self._default_role

    @cached_property
    def _default_role(self) -> str:
        role = default_role()
        if role is None:
            role = self._get_current_role()
        return role

    def _get_current_role(self) -> str:
        role_result = self._execute_query(
            "select current_role()", cursor_class=DictCursor
        ).fetchone()
        return role_result["CURRENT_ROLE()"]

    @cached_property
    def debug_mode(self) -> bool:
        if self.definition.application:
            return self.definition.application.debug
        else:
            return True

    @cached_property
    def get_app_pkg_distribution_in_snowflake(self) -> str:
        """
        Returns the 'distribution' attribute of a 'describe application package' SQL query, in lowercase.
        """
        with self.use_role(self.package_role):
            try:
                desc_cursor = self._execute_query(
                    f"describe application package {self.package_name}"
                )
            except ProgrammingError as err:
                generic_sql_error_handler(err)

            if desc_cursor.rowcount is None or desc_cursor.rowcount == 0:
                raise SnowflakeSQLExecutionError()
            else:
                for row in desc_cursor:
                    if row[0].lower() == "distribution":
                        return row[1].lower()
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Could not find the 'distribution' attribute for application package {self.package_name} in the output of SQL query:
                'describe application package {self.package_name}'
                """
            )
        )

    def verify_project_distribution(
        self, expected_distribution: Optional[str] = None
    ) -> bool:
        """
        Returns true if the 'distribution' attribute of an existing application package in snowflake
        is the same as the the attribute specified in project definition file.
        """
        actual_distribution = (
            expected_distribution
            if expected_distribution
            else self.get_app_pkg_distribution_in_snowflake
        )
        project_def_distribution = self.package_distribution.lower()
        if actual_distribution != project_def_distribution:
            cc.warning(
                dedent(
                    f"""\
                    Application package {self.package_name} in your Snowflake account has distribution property {actual_distribution},
                    which does not match the value specified in project definition file: {project_def_distribution}.
                    """
                )
            )
            return False
        return True

    def build_bundle(self) -> ArtifactDeploymentMap:
        """
        Populates the local deploy root from artifact sources.
        """
        return build_bundle(self.project_root, self.deploy_root, self.artifacts)

    def sync_deploy_root_with_stage(
        self,
        role: str,
        prune: bool,
        recursive: bool,
        local_paths_to_sync: List[Path] | None = None,
        mapped_files: Optional[ArtifactDeploymentMap] = None,
    ) -> DiffResult:
        """
        Ensures that the files on our remote stage match the artifacts we have in
        the local filesystem.

        Args:
            role (str): The name of the role to use for queries and commands.
            prune (bool): Whether to prune artifacts from the stage that don't exist locally.
            recursive (bool): Whether to traverse directories recursively.
            local_paths_to_sync (List[Path], optional): List of local paths to sync. Defaults to None to sync all
             local paths. Note that providing an empty list here is equivalent to None.
            mapped_files: the file mapping computed during the `bundle` step. Required when local_paths_to_sync is
             provided.

        Returns:
            A `DiffResult` instance describing the changes that were performed.
        """

        # Does a stage already exist within the application package, or we need to create one?
        # Using "if not exists" should take care of either case.
        cc.step("Checking if stage exists, or creating a new one if none exists.")
        with self.use_role(role):
            self._execute_query(
                f"create schema if not exists {self.package_name}.{self.stage_schema}"
            )
            self._execute_query(
                f"""
                    create stage if not exists {self.stage_fqn}
                    encryption = (TYPE = 'SNOWFLAKE_SSE')
                    DIRECTORY = (ENABLE = TRUE)"""
            )

        # Perform a diff operation and display results to the user for informational purposes
        cc.step(
            "Performing a diff between the Snowflake stage and your local deploy_root ('%s') directory."
            % self.deploy_root
        )
        diff: DiffResult = compute_stage_diff(self.deploy_root, self.stage_fqn)

        files_not_removed = []
        if local_paths_to_sync:
            assert mapped_files is not None

            # Deploying specific files/directories
            resolved_paths_to_sync = [
                resolve_without_follow(p) for p in local_paths_to_sync
            ]
            if not recursive:
                verify_no_directories(resolved_paths_to_sync)
            deploy_paths_to_sync = [
                source_path_to_deploy_path(p, mapped_files)
                for p in resolved_paths_to_sync
            ]
            verify_exists(deploy_paths_to_sync)
            stage_paths_to_sync = _get_stage_paths_to_sync(
                deploy_paths_to_sync, self.deploy_root.resolve()
            )
            diff = preserve_from_diff(diff, stage_paths_to_sync)
        else:
            # Full deploy
            if not recursive:
                deploy_files = [p for p in self.deploy_root.resolve().iterdir()]
                verify_no_directories(deploy_files)

        if not prune:
            files_not_removed = [str(path) for path in diff.only_on_stage]
            diff.only_on_stage = []

            if len(files_not_removed) > 0:
                files_not_removed_str = "\n".join(files_not_removed)
                cc.warning(
                    f"The following files exist only on the stage:\n{files_not_removed_str}\n\nUse the --prune flag to delete them from the stage."
                )

        cc.message(str(diff))

        # Upload diff-ed files to application package stage
        if diff.has_changes():
            cc.step(
                "Uploading diff-ed files from your local %s directory to the Snowflake stage."
                % self.deploy_root,
            )
            sync_local_diff_with_stage(
                role=role,
                deploy_root_path=self.deploy_root,
                diff_result=diff,
                stage_fqn=self.stage_fqn,
            )
        return diff

    def get_existing_app_info(self) -> Optional[dict]:
        """
        Check for an existing application object by the same name as in project definition, in account.
        It executes a 'show applications like' query and returns the result as single row, if one exists.
        """
        with self.use_role(self.app_role):
            return self.show_specific_object(
                "applications", self.app_name, name_col=NAME_COL
            )

    def get_existing_app_pkg_info(self) -> Optional[dict]:
        """
        Check for an existing application package by the same name as in project definition, in account.
        It executes a 'show application packages like' query and returns the result as single row, if one exists.
        """

        with self.use_role(self.package_role):
            return self.show_specific_object(
                "application packages", self.package_name, name_col=NAME_COL
            )

    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        name = unquote_identifier(self.app_name)
        return make_snowsight_url(self._conn, f"/#/apps/application/{name}")

    def create_app_package(self) -> None:
        """
        Creates the application package with our up-to-date stage if none exists.
        """

        # 1. Check for existing existing application package
        show_obj_row = self.get_existing_app_pkg_info()

        if show_obj_row:
            # 1. Check for the right owner role
            ensure_correct_owner(
                row=show_obj_row, role=self.package_role, obj_name=self.package_name
            )

            # 2. Check distribution of the existing application package
            actual_distribution = self.get_app_pkg_distribution_in_snowflake
            if not self.verify_project_distribution(actual_distribution):
                cc.warning(
                    f"Continuing to execute `snow app run` on application package {self.package_name} with distribution '{actual_distribution}'."
                )

            # 3. If actual_distribution is external, skip comment check
            if actual_distribution == INTERNAL_DISTRIBUTION:
                row_comment = show_obj_row[COMMENT_COL]

                if row_comment not in ALLOWED_SPECIAL_COMMENTS:
                    raise ApplicationPackageAlreadyExistsError(self.package_name)

            return

        # If no application package pre-exists, create an application package, with the specified distribution in the project definition file.
        with self.use_role(self.package_role):
            cc.step(f"Creating new application package {self.package_name} in account.")
            self._execute_query(
                dedent(
                    f"""\
                    create application package {self.package_name}
                        comment = {SPECIAL_COMMENT}
                        distribution = {self.package_distribution}
                """
                )
            )

    def _apply_package_scripts(self) -> None:
        """
        Assuming the application package exists and we are using the correct role,
        applies all package scripts in-order to the application package.
        """
        env = jinja2.Environment(
            loader=jinja2.loaders.FileSystemLoader(self.project_root),
            keep_trailing_newline=True,
            undefined=jinja2.StrictUndefined,
        )

        queued_queries = []
        for relpath in self.package_scripts:
            try:
                template = env.get_template(relpath)
                result = template.render(dict(package_name=self.package_name))
                queued_queries.append(result)

            except jinja2.TemplateNotFound as e:
                raise MissingPackageScriptError(e.name)

            except jinja2.TemplateSyntaxError as e:
                raise InvalidPackageScriptError(e.name, e)

            except jinja2.UndefinedError as e:
                raise InvalidPackageScriptError(relpath, e)

        # once we're sure all the templates expanded correctly, execute all of them
        try:
            if self.package_warehouse:
                self._execute_query(f"use warehouse {self.package_warehouse}")

            for i, queries in enumerate(queued_queries):
                cc.step(f"Applying package script: {self.package_scripts[i]}")
                self._execute_queries(queries)
        except ProgrammingError as err:
            generic_sql_error_handler(
                err, role=self.package_role, warehouse=self.package_warehouse
            )

    def deploy(
        self,
        prune: bool,
        recursive: bool,
        local_paths_to_sync: List[Path] | None = None,
        mapped_files: Optional[ArtifactDeploymentMap] = None,
    ) -> DiffResult:
        """app deploy process"""

        # 1. Create an empty application package, if none exists
        self.create_app_package()

        with self.use_role(self.package_role):
            # 2. now that the application package exists, create shared data
            self._apply_package_scripts()

            # 3. Upload files from deploy root local folder to the above stage
            diff = self.sync_deploy_root_with_stage(
                self.package_role, prune, recursive, local_paths_to_sync, mapped_files
            )

        return diff
