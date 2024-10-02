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
from typing import Optional

import typer
from snowflake.cli._plugins.nativeapp.artifacts import BundleMap
from snowflake.cli._plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
)
from snowflake.cli._plugins.nativeapp.policy import PolicyBase
from snowflake.cli._plugins.nativeapp.same_account_install_method import (
    SameAccountInstallMethod,
)
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.application_entity import (
    ApplicationEntity,
)
from snowflake.cli.api.entities.utils import (
    generic_sql_error_handler,
)
from snowflake.cli.api.errno import (
    APPLICATION_NO_LONGER_AVAILABLE,
    APPLICATION_OWNS_EXTERNAL_OBJECTS,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor


class NativeAppRunProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: NativeApp, project_root: Path):
        super().__init__(project_definition, project_root)

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
        return ApplicationEntity.get_existing_version_info(
            version=version,
            package_name=self.package_name,
            package_role=self.package_role,
        )

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
        def drop_app():
            self.drop_application_before_upgrade(policy, is_interactive)

        return ApplicationEntity.create_or_upgrade_app(
            console=cc,
            project_root=self.project_root,
            package_name=self.package_name,
            package_role=self.package_role,
            app_name=self.app_name,
            app_role=self.app_role,
            app_warehouse=self.application_warehouse,
            stage_schema=self.stage_schema,
            stage_fqn=self.stage_fqn,
            debug_mode=self.debug_mode,
            policy=policy,
            install_method=install_method,
            is_interactive=is_interactive,
            post_deploy_hooks=self.app_post_deploy_hooks,
            drop_application_before_upgrade=drop_app,
        )

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
        def deploy_package():
            self.deploy(
                bundle_map=bundle_map, prune=True, recursive=True, validate=validate
            )

        def drop_app():
            self.drop_application_before_upgrade(policy, is_interactive)

        ApplicationEntity.deploy(
            console=cc,
            project_root=self.project_root,
            app_name=self.app_name,
            app_role=self.app_role,
            app_warehouse=self.application_warehouse,
            package_name=self.package_name,
            package_role=self.package_role,
            stage_schema=self.stage_schema,
            stage_fqn=self.stage_fqn,
            debug_mode=self.debug_mode,
            validate=validate,
            from_release_directive=from_release_directive,
            is_interactive=is_interactive,
            policy=policy,
            version=version,
            patch=patch,
            post_deploy_hooks=self.app_post_deploy_hooks,
            deploy_package=deploy_package,
            drop_application_before_upgrade=drop_app,
        )
