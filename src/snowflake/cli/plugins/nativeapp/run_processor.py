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

from pathlib import Path
from textwrap import dedent
from typing import Optional

import jinja2
import typer
from click import UsageError
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.errno import (
    APPLICATION_NO_LONGER_AVAILABLE,
    APPLICATION_OWNS_EXTERNAL_OBJECTS,
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    CANNOT_UPGRADE_FROM_VERSION_TO_LOOSE_FILES,
    NOT_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    ONLY_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.util import (
    identifier_to_show_like_pattern,
    unquote_identifier,
)
from snowflake.cli.api.rendering.sql_templates import (
    get_sql_cli_jinja_env,
)
from snowflake.cli.api.utils.cursor import find_all_rows
from snowflake.cli.plugins.nativeapp.artifacts import BundleMap
from snowflake.cli.plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    PATCH_COL,
    SPECIAL_COMMENT,
    VERSION_COL,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationCreatedExternallyError,
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
    ensure_correct_owner,
    generic_sql_error_handler,
)
from snowflake.cli.plugins.nativeapp.policy import PolicyBase
from snowflake.cli.plugins.nativeapp.project_model import (
    NativeAppProjectModel,
)
from snowflake.cli.plugins.stage.manager import StageManager
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

# Reasons why an `alter application ... upgrade` might fail
UPGRADE_RESTRICTION_CODES = {
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    CANNOT_UPGRADE_FROM_VERSION_TO_LOOSE_FILES,
    ONLY_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    NOT_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    APPLICATION_NO_LONGER_AVAILABLE,
}


class SameAccountInstallMethod:
    _requires_created_by_cli: bool
    _from_release_directive: bool
    version: Optional[str]
    patch: Optional[int]

    def __init__(
        self,
        requires_created_by_cli: bool,
        version: Optional[str] = None,
        patch: Optional[int] = None,
        from_release_directive: bool = False,
    ):
        self._requires_created_by_cli = requires_created_by_cli
        self.version = version
        self.patch = patch
        self._from_release_directive = from_release_directive

    @classmethod
    def unversioned_dev(cls):
        """aka. stage dev aka loose files"""
        return cls(True)

    @classmethod
    def versioned_dev(cls, version: str, patch: Optional[int] = None):
        return cls(False, version, patch)

    @classmethod
    def release_directive(cls):
        return cls(False, from_release_directive=True)

    @property
    def is_dev_mode(self) -> bool:
        return not self._from_release_directive

    def using_clause(self, app: NativeAppProjectModel) -> str:
        if self._from_release_directive:
            return ""

        if self.version:
            patch_clause = f"patch {self.patch}" if self.patch else ""
            return f"using version {self.version} {patch_clause}"

        stage_name = StageManager.quote_stage_name(app.stage_fqn)
        return f"using {stage_name}"

    def ensure_app_usable(self, app: NativeAppProjectModel, show_app_row: dict):
        """Raise an exception if we cannot proceed with install given the pre-existing application object"""

        if self._requires_created_by_cli:
            if show_app_row[COMMENT_COL] not in ALLOWED_SPECIAL_COMMENTS:
                # this application object was not created by this tooling
                raise ApplicationCreatedExternallyError(app.app_name)

        # expected owner
        ensure_correct_owner(row=show_app_row, role=app.app_role, obj_name=app.app_name)


class NativeAppRunProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: NativeApp, project_root: Path):
        super().__init__(project_definition, project_root)

    def _execute_sql_script(
        self, script_content: str, database_name: Optional[str] = None
    ):
        """
        Executing the provided SQL script content.
        This assumes that a relevant warehouse is already active.
        If database_name is passed in, it will be used first.
        """
        try:
            if database_name is not None:
                self._execute_query(f"use database {database_name}")

            self._execute_queries(script_content)
        except ProgrammingError as err:
            generic_sql_error_handler(err)

    def _execute_post_deploy_hooks(self):
        post_deploy_script_hooks = self.app_post_deploy_hooks
        if post_deploy_script_hooks:
            with cc.phase("Executing application post-deploy actions"):
                sql_scripts_paths = []
                for hook in post_deploy_script_hooks:
                    if hook.sql_script:
                        sql_scripts_paths.append(hook.sql_script)
                    else:
                        raise ValueError(
                            f"Unsupported application post-deploy hook type: {hook}"
                        )

                env = get_sql_cli_jinja_env(
                    loader=jinja2.loaders.FileSystemLoader(self.project_root)
                )
                scripts_content_list = self._expand_script_templates(
                    env, cli_context.template_context, sql_scripts_paths
                )

                for index, sql_script_path in enumerate(sql_scripts_paths):
                    cc.step(f"Executing SQL script: {sql_script_path}")
                    self._execute_sql_script(scripts_content_list[index], self.app_name)

    def get_all_existing_versions(self) -> SnowflakeCursor:
        """
        Get all existing versions, if defined, for an application package.
        It executes a 'show versions in application package' query and returns all the results.
        """
        with self.use_role(self.package_role):
            show_obj_query = f"show versions in application package {self.package_name}"
            show_obj_cursor = self._execute_query(show_obj_query)

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            return show_obj_cursor

    def get_existing_version_info(self, version: str) -> Optional[dict]:
        """
        Get the latest patch on an existing version by name in the application package.
        Executes 'show versions like ... in application package' query and returns
        the latest patch in the version as a single row, if one exists. Otherwise,
        returns None.
        """
        with self.use_role(self.package_role):
            try:
                query = f"show versions like {identifier_to_show_like_pattern(version)} in application package {self.package_name}"
                cursor = self._execute_query(query, cursor_class=DictCursor)

                if cursor.rowcount is None:
                    raise SnowflakeSQLExecutionError(query)

                matching_rows = find_all_rows(
                    cursor, lambda row: row[VERSION_COL] == unquote_identifier(version)
                )

                if not matching_rows:
                    return None

                return max(matching_rows, key=lambda row: row[PATCH_COL])

            except ProgrammingError as err:
                if err.msg.__contains__("does not exist or not authorized"):
                    raise ApplicationPackageDoesNotExistError(self.package_name)
                else:
                    generic_sql_error_handler(err=err, role=self.package_role)
                    return None

    def drop_application_before_upgrade(
        self, policy: PolicyBase, is_interactive: bool, cascade: bool = False
    ):
        """
        This method will attempt to drop an application object if a previous upgrade fails.
        """
        if cascade:
            try:
                if application_objects := self.get_objects_owned_by_application():
                    application_objects_str = self._application_objects_to_str(
                        application_objects
                    )
                    cc.message(
                        f"The following objects are owned by application {self.app_name} and need to be dropped:\n{application_objects_str}"
                    )
            except ProgrammingError as err:
                if err.errno != APPLICATION_NO_LONGER_AVAILABLE:
                    generic_sql_error_handler(err)
                cc.warning(
                    "The application owns other objects but they could not be determined."
                )
            user_prompt = "Do you want the Snowflake CLI to drop these objects, then drop the existing application object and recreate it?"
        else:
            user_prompt = "Do you want the Snowflake CLI to drop the existing application object and recreate it?"

        if not policy.should_proceed(user_prompt):
            if is_interactive:
                cc.message("Not upgrading the application object.")
                raise typer.Exit(0)
            else:
                cc.message(
                    "Cannot upgrade the application object non-interactively without --force."
                )
                raise typer.Exit(1)
        try:
            cascade_msg = " (cascade)" if cascade else ""
            cc.step(f"Dropping application object {self.app_name}{cascade_msg}.")
            cascade_sql = " cascade" if cascade else ""
            self._execute_query(f"drop application {self.app_name}{cascade_sql}")
        except ProgrammingError as err:
            if err.errno == APPLICATION_OWNS_EXTERNAL_OBJECTS and not cascade:
                # We need to cascade the deletion, let's try again (only if we didn't try with cascade already)
                return self.drop_application_before_upgrade(
                    policy, is_interactive, cascade=True
                )
            else:
                generic_sql_error_handler(err)

    def create_or_upgrade_app(
        self,
        policy: PolicyBase,
        install_method: SameAccountInstallMethod,
        is_interactive: bool = False,
    ):
        with self.use_role(self.app_role):

            # 1. Need to use a warehouse to create an application object
            with self.use_warehouse(self.application_warehouse):

                # 2. Check for an existing application by the same name
                show_app_row = self.get_existing_app_info()

                # 3. If existing application is found, perform a few validations and upgrade the application object.
                if show_app_row:

                    install_method.ensure_app_usable(self._na_project, show_app_row)

                    # If all the above checks are in order, proceed to upgrade
                    try:
                        cc.step(
                            f"Upgrading existing application object {self.app_name}."
                        )
                        using_clause = install_method.using_clause(self._na_project)
                        self._execute_query(
                            f"alter application {self.app_name} upgrade {using_clause}"
                        )

                        if install_method.is_dev_mode:
                            # if debug_mode is present (controlled), ensure it is up-to-date
                            if self.debug_mode is not None:
                                self._execute_query(
                                    f"alter application {self.app_name} set debug_mode = {self.debug_mode}"
                                )

                        # hooks always executed after a create or upgrade
                        self._execute_post_deploy_hooks()
                        return

                    except ProgrammingError as err:
                        if err.errno not in UPGRADE_RESTRICTION_CODES:
                            generic_sql_error_handler(err=err)
                        else:  # The existing application object was created from a different process.
                            cc.warning(err.msg)
                            self.drop_application_before_upgrade(policy, is_interactive)

                # 4. With no (more) existing application objects, create an application object using the release directives
                cc.step(f"Creating new application object {self.app_name} in account.")

                if self.app_role != self.package_role:
                    with self.use_role(self.package_role):
                        self._execute_query(
                            f"grant install, develop on application package {self.package_name} to role {self.app_role}"
                        )
                        self._execute_query(
                            f"grant usage on schema {self.package_name}.{self.stage_schema} to role {self.app_role}"
                        )
                        self._execute_query(
                            f"grant read on stage {self.stage_fqn} to role {self.app_role}"
                        )

                try:
                    # by default, applications are created in debug mode when possible;
                    # this can be overridden in the project definition
                    debug_mode_clause = ""
                    if install_method.is_dev_mode:
                        initial_debug_mode = (
                            self.debug_mode if self.debug_mode is not None else True
                        )
                        debug_mode_clause = f"debug_mode = {initial_debug_mode}"

                    using_clause = install_method.using_clause(self._na_project)
                    self._execute_query(
                        dedent(
                            f"""\
                        create application {self.app_name}
                            from application package {self.package_name} {using_clause} {debug_mode_clause}
                            comment = {SPECIAL_COMMENT}
                        """
                        )
                    )

                    # hooks always executed after a create or upgrade
                    self._execute_post_deploy_hooks()

                except ProgrammingError as err:
                    generic_sql_error_handler(err)

    def process(
        self,
        bundle_map: BundleMap,
        policy: PolicyBase,
        version: Optional[str] = None,
        patch: Optional[int] = None,
        from_release_directive: bool = False,
        is_interactive: bool = False,
        validate: bool = True,
        *args,
        **kwargs,
    ):
        """
        Create or upgrade the application object using the given strategy
        (unversioned dev, versioned dev, or same-account release directive).
        """

        # same-account release directive
        if from_release_directive:
            self.create_or_upgrade_app(
                policy=policy,
                is_interactive=is_interactive,
                install_method=SameAccountInstallMethod.release_directive(),
            )
            return

        # versioned dev
        if version:
            try:
                version_exists = self.get_existing_version_info(version)
                if not version_exists:
                    raise UsageError(
                        f"Application package {self.package_name} does not have any version {version} defined. Use 'snow app version create' to define a version in the application package first."
                    )
            except ApplicationPackageDoesNotExistError as app_err:
                raise UsageError(
                    f"Application package {self.package_name} does not exist. Use 'snow app version create' to first create an application package and then define a version in it."
                )

            self.create_or_upgrade_app(
                policy=policy,
                install_method=SameAccountInstallMethod.versioned_dev(version, patch),
                is_interactive=is_interactive,
            )
            return

        # unversioned dev
        self.deploy(
            bundle_map=bundle_map, prune=True, recursive=True, validate=validate
        )
        self.create_or_upgrade_app(
            policy=policy,
            is_interactive=is_interactive,
            install_method=SameAccountInstallMethod.unversioned_dev(),
        )
