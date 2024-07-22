# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import Any, List, NoReturn, Optional, TypedDict

import jinja2
from click import ClickException
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.native_app.application import (
    ApplicationPostDeployHook,
)
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.util import (
    identifier_for_url,
    unquote_identifier,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import make_snowsight_url
from snowflake.cli.plugins.nativeapp.artifacts import (
    BundleMap,
    build_bundle,
    resolve_without_follow,
)
from snowflake.cli.plugins.nativeapp.codegen.compiler import (
    NativeAppCompiler,
)
from snowflake.cli.plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    INTERNAL_DISTRIBUTION,
    NAME_COL,
    OWNER_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
    InvalidScriptError,
    MissingScriptError,
    NoEventTableForAccount,
    SetupScriptFailedValidation,
    UnexpectedOwnerError,
)
from snowflake.cli.plugins.nativeapp.project_model import (
    NativeAppProjectModel,
)
from snowflake.cli.plugins.nativeapp.utils import verify_exists, verify_no_directories
from snowflake.cli.plugins.stage.diff import (
    DiffResult,
    StagePath,
    compute_stage_diff,
    preserve_from_diff,
    print_diff_to_console,
    sync_local_diff_with_stage,
    to_stage_path,
)
from snowflake.cli.plugins.stage.manager import StageManager
from snowflake.connector import DictCursor, ProgrammingError

ApplicationOwnedObject = TypedDict("ApplicationOwnedObject", {"name": str, "type": str})


def generic_sql_error_handler(
    err: ProgrammingError, role: Optional[str] = None, warehouse: Optional[str] = None
) -> NoReturn:
    # Potential refactor: If moving away from Python 3.8 and 3.9 to >= 3.10, use match ... case
    if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
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
    elif err.errno == NO_WAREHOUSE_SELECTED_IN_SESSION:
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please provide a warehouse for the active session role in your project definition file, config.toml file, or via command line.
                """
            ),
            errno=err.errno,
        )
    elif "does not exist or not authorized" in err.msg:
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
        self._na_project = NativeAppProjectModel(
            project_definition=project_definition,
            project_root=project_root,
        )

    @property
    def na_project(self) -> NativeAppProjectModel:
        return self._na_project

    @property
    def project_root(self) -> Path:
        return self.na_project.project_root

    @property
    def definition(self) -> NativeApp:
        return self.na_project.definition

    @property
    def artifacts(self) -> List[PathMapping]:
        return self.na_project.artifacts

    @property
    def bundle_root(self) -> Path:
        return self.na_project.bundle_root

    @property
    def deploy_root(self) -> Path:
        return self.na_project.deploy_root

    @property
    def generated_root(self) -> Path:
        return self.na_project.generated_root

    @property
    def package_scripts(self) -> List[str]:
        return self.na_project.package_scripts

    @property
    def stage_fqn(self) -> str:
        return self.na_project.stage_fqn

    @property
    def scratch_stage_fqn(self) -> str:
        return self.na_project.scratch_stage_fqn

    @property
    def stage_schema(self) -> Optional[str]:
        return self.na_project.stage_schema

    @property
    def package_warehouse(self) -> Optional[str]:
        return self.na_project.package_warehouse

    @contextmanager
    def use_package_warehouse(self):
        if self.package_warehouse:
            with self.use_warehouse(self.package_warehouse):
                yield
        else:
            raise ClickException(
                dedent(
                    f"""\
                Application package warehouse cannot be empty.
                Please provide a value for it in your connection information or your project definition file.
                """
                )
            )

    @property
    def application_warehouse(self) -> Optional[str]:
        return self.na_project.application_warehouse

    @contextmanager
    def use_application_warehouse(self):
        if self.application_warehouse:
            with self.use_warehouse(self.application_warehouse):
                yield
        else:
            raise ClickException(
                dedent(
                    f"""\
                Application warehouse cannot be empty.
                Please provide a value for it in your connection information or your project definition file.
                """
                )
            )

    @property
    def project_identifier(self) -> str:
        return self.na_project.project_identifier

    @property
    def package_name(self) -> str:
        return self.na_project.package_name

    @property
    def package_role(self) -> str:
        return self.na_project.package_role

    @property
    def package_distribution(self) -> str:
        return self.na_project.package_distribution

    @property
    def app_name(self) -> str:
        return self.na_project.app_name

    @property
    def app_role(self) -> str:
        return self.na_project.app_role

    @property
    def app_post_deploy_hooks(self) -> Optional[List[ApplicationPostDeployHook]]:
        return self.na_project.app_post_deploy_hooks

    @property
    def debug_mode(self) -> bool:
        return self.na_project.debug_mode

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

    @cached_property
    def account_event_table(self) -> str:
        query = "show parameters like 'event_table' in account"
        results = self._execute_query(query, cursor_class=DictCursor)
        return next((r["value"] for r in results if r["key"] == "EVENT_TABLE"), "")

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

    def build_bundle(self) -> BundleMap:
        """
        Populates the local deploy root from artifact sources.
        """
        bundle_map = build_bundle(self.project_root, self.deploy_root, self.artifacts)
        compiler = NativeAppCompiler(
            na_project=self.na_project,
        )
        compiler.compile_artifacts()
        return bundle_map

    def sync_deploy_root_with_stage(
        self,
        bundle_map: BundleMap,
        role: str,
        prune: bool,
        recursive: bool,
        stage_fqn: str,
        local_paths_to_sync: List[Path] | None = None,
        print_diff: bool = True,
    ) -> DiffResult:
        """
        Ensures that the files on our remote stage match the artifacts we have in
        the local filesystem.

        Args:
            bundle_map (BundleMap): The artifact mapping computed by the `build_bundle` function.
            role (str): The name of the role to use for queries and commands.
            prune (bool): Whether to prune artifacts from the stage that don't exist locally.
            recursive (bool): Whether to traverse directories recursively.
            stage_fqn (str): The name of the stage to diff against and upload to.
            local_paths_to_sync (List[Path], optional): List of local paths to sync. Defaults to None to sync all
             local paths. Note that providing an empty list here is equivalent to None.
            print_diff (bool): Whether to print the diff between the local files and the remote stage. Defaults to True

        Returns:
            A `DiffResult` instance describing the changes that were performed.
        """

        # Does a stage already exist within the application package, or we need to create one?
        # Using "if not exists" should take care of either case.
        cc.step(
            f"Checking if stage {stage_fqn} exists, or creating a new one if none exists."
        )
        with self.use_role(role):
            self._execute_query(
                f"create schema if not exists {self.package_name}.{self.stage_schema}"
            )
            self._execute_query(
                f"""
                    create stage if not exists {stage_fqn}
                    encryption = (TYPE = 'SNOWFLAKE_SSE')
                    DIRECTORY = (ENABLE = TRUE)"""
            )

        # Perform a diff operation and display results to the user for informational purposes
        if print_diff:
            cc.step(
                "Performing a diff between the Snowflake stage and your local deploy_root ('%s') directory."
                % self.deploy_root.resolve()
            )
        diff: DiffResult = compute_stage_diff(self.deploy_root, stage_fqn)

        if local_paths_to_sync:
            # Deploying specific files/directories
            resolved_paths_to_sync = [
                resolve_without_follow(p) for p in local_paths_to_sync
            ]
            if not recursive:
                verify_no_directories(resolved_paths_to_sync)

            deploy_paths_to_sync = []
            for resolved_path in resolved_paths_to_sync:
                verify_exists(resolved_path)
                deploy_paths = bundle_map.to_deploy_paths(resolved_path)
                if not deploy_paths:
                    if resolved_path.is_dir() and recursive:
                        # No direct artifact mapping found for this path. Check to see
                        # if there are subpaths of this directory that are matches. We
                        # loop over sources because it's likely a much smaller list
                        # than the project directory.
                        for src in bundle_map.all_sources(absolute=True):
                            if resolved_path in src.parents:
                                # There is a source that contains this path, get its dest path(s)
                                deploy_paths.extend(bundle_map.to_deploy_paths(src))

                if not deploy_paths:
                    raise ClickException(f"No artifact found for {resolved_path}")
                deploy_paths_to_sync.extend(deploy_paths)

            stage_paths_to_sync = _get_stage_paths_to_sync(
                deploy_paths_to_sync, resolve_without_follow(self.deploy_root)
            )
            diff = preserve_from_diff(diff, stage_paths_to_sync)
        else:
            # Full deploy
            if not recursive:
                verify_no_directories(self.deploy_root.resolve().iterdir())

        if not prune:
            files_not_removed = [str(path) for path in diff.only_on_stage]
            diff.only_on_stage = []

            if len(files_not_removed) > 0:
                files_not_removed_str = "\n".join(files_not_removed)
                cc.warning(
                    f"The following files exist only on the stage:\n{files_not_removed_str}\n\nUse the --prune flag to delete them from the stage."
                )

        if print_diff:
            print_diff_to_console(diff, bundle_map)

        # Upload diff-ed files to application package stage
        if diff.has_changes():
            cc.step(
                "Updating the Snowflake stage from your local %s directory."
                % self.deploy_root.resolve(),
            )
            sync_local_diff_with_stage(
                role=role,
                deploy_root_path=self.deploy_root,
                diff_result=diff,
                stage_fqn=stage_fqn,
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

    def get_objects_owned_by_application(self) -> List[ApplicationOwnedObject]:
        """
        Returns all application objects owned by this application.
        """
        with self.use_role(self.app_role):
            results = self._execute_query(
                f"show objects owned by application {self.app_name}"
            ).fetchall()
            return [{"name": row[1], "type": row[2]} for row in results]

    def _application_objects_to_str(
        self, application_objects: list[ApplicationOwnedObject]
    ) -> str:
        """
        Returns a list in an "(Object Type) Object Name" format. Database-level and schema-level object names are fully qualified:
        (COMPUTE_POOL) POOL_NAME
        (DATABASE) DB_NAME
        (SCHEMA) DB_NAME.PUBLIC
        ...
        """
        return "\n".join(
            [self._application_object_to_str(obj) for obj in application_objects]
        )

    def _application_object_to_str(self, obj: ApplicationOwnedObject) -> str:
        return f"({obj['type']}) {obj['name']}"

    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        name = identifier_for_url(self.app_name)
        with self.use_application_warehouse():
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

    def _expand_script_templates(
        self, env: jinja2.Environment, jinja_context: dict[str, Any], scripts: List[str]
    ) -> List[str]:
        """
        Input:
        - env: Jinja2 environment
        - jinja_context: a dictionary with the jinja context
        - scripts: list of scripts that need to be expanded with Jinja
        Returns:
        - List of expanded scripts content.
        Size of the return list is the same as the size of the input scripts list.
        """
        scripts_contents = []
        for relpath in scripts:
            try:
                template = env.get_template(relpath)
                result = template.render(**jinja_context)
                scripts_contents.append(result)

            except jinja2.TemplateNotFound as e:
                raise MissingScriptError(e.name) from e

            except jinja2.TemplateSyntaxError as e:
                raise InvalidScriptError(e.name, e, e.lineno) from e

            except jinja2.UndefinedError as e:
                raise InvalidScriptError(relpath, e) from e

        return scripts_contents

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

        queued_queries = self._expand_script_templates(
            env, dict(package_name=self.package_name), self.package_scripts
        )

        # once we're sure all the templates expanded correctly, execute all of them
        with self.use_package_warehouse():
            try:
                for i, queries in enumerate(queued_queries):
                    cc.step(f"Applying package script: {self.package_scripts[i]}")
                    self._execute_queries(queries)
            except ProgrammingError as err:
                generic_sql_error_handler(
                    err, role=self.package_role, warehouse=self.package_warehouse
                )

    def deploy(
        self,
        bundle_map: BundleMap,
        prune: bool,
        recursive: bool,
        stage_fqn: Optional[str] = None,
        local_paths_to_sync: List[Path] | None = None,
        validate: bool = True,
        print_diff: bool = True,
    ) -> DiffResult:
        """app deploy process"""

        # 1. Create an empty application package, if none exists
        self.create_app_package()

        with self.use_role(self.package_role):
            # 2. now that the application package exists, create shared data
            self._apply_package_scripts()

            # 3. Upload files from deploy root local folder to the above stage
            stage_fqn = stage_fqn or self.stage_fqn
            diff = self.sync_deploy_root_with_stage(
                bundle_map=bundle_map,
                role=self.package_role,
                prune=prune,
                recursive=recursive,
                stage_fqn=stage_fqn,
                local_paths_to_sync=local_paths_to_sync,
                print_diff=print_diff,
            )

        if validate:
            self.validate(use_scratch_stage=False)

        return diff

    def validate(self, use_scratch_stage: bool = False):
        """Validates Native App setup script SQL."""
        with cc.phase(f"Validating Snowflake Native App setup script."):
            validation_result = self.get_validation_result(use_scratch_stage)

            # First print warnings, regardless of the outcome of validation
            for warning in validation_result.get("warnings", []):
                cc.warning(_validation_item_to_str(warning))

            # Then print errors
            for error in validation_result.get("errors", []):
                # Print them as warnings for now since we're going to be
                # revamping CLI output soon
                cc.warning(_validation_item_to_str(error))

            # Then raise an exception if validation failed
            if validation_result["status"] == "FAIL":
                raise SetupScriptFailedValidation()

    def get_validation_result(self, use_scratch_stage: bool):
        """Call system$validate_native_app_setup() to validate deployed Native App setup script."""
        stage_fqn = self.stage_fqn
        if use_scratch_stage:
            stage_fqn = self.scratch_stage_fqn
            bundle_map = self.build_bundle()
            self.deploy(
                bundle_map=bundle_map,
                prune=True,
                recursive=True,
                stage_fqn=stage_fqn,
                validate=False,
                print_diff=False,
            )
        prefixed_stage_fqn = StageManager.get_standard_stage_prefix(stage_fqn)
        try:
            cursor = self._execute_query(
                f"call system$validate_native_app_setup('{prefixed_stage_fqn}')"
            )
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                raise ApplicationPackageDoesNotExistError(self.package_name)
            generic_sql_error_handler(err)
        else:
            if not cursor.rowcount:
                raise SnowflakeSQLExecutionError()
            return json.loads(cursor.fetchone()[0])
        finally:
            if use_scratch_stage:
                cc.step(f"Dropping stage {self.scratch_stage_fqn}.")
                with self.use_role(self.package_role):
                    self._execute_query(
                        f"drop stage if exists {self.scratch_stage_fqn}"
                    )

    def get_events(self) -> list[dict]:
        if not self.account_event_table:
            raise NoEventTableForAccount()

        # resource_attributes:"snow.database.name" uses the unquoted/uppercase app name
        app_name = unquote_identifier(self.app_name)
        query = dedent(
            f"""\
            select timestamp, value::varchar value
            from {self.account_event_table}
            where resource_attributes:"snow.database.name" = '{app_name}'
            order by timestamp asc;"""
        )
        try:
            return self._execute_query(query, cursor_class=DictCursor).fetchall()
        except ProgrammingError as err:
            generic_sql_error_handler(err)


def _validation_item_to_str(item: dict[str, str | int]):
    s = item["message"]
    if item["errorCode"]:
        s = f"{s} (error code {item['errorCode']})"
    return s
