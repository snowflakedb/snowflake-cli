from pathlib import Path
from textwrap import dedent
from typing import Optional

import typer
from click import UsageError
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
    VERSION_COL,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
    ensure_correct_owner,
    generic_sql_error_handler,
)
from snowflake.cli.plugins.nativeapp.policy import PolicyBase
from snowflake.cli.plugins.stage.diff import DiffResult
from snowflake.cli.plugins.stage.manager import StageManager
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

UPGRADE_RESTRICTION_CODES = {93044, 93055, 93045, 93046}


class NativeAppRunProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: NativeApp, project_root: Path):
        super().__init__(project_definition, project_root)

    def _create_dev_app(self, diff: DiffResult) -> None:
        """
        (Re-)creates the application object with our up-to-date stage.
        """
        with self.use_role(self.app_role):

            # 1. Need to use a warehouse to create an application object
            try:
                if self.application_warehouse:
                    self._execute_query(f"use warehouse {self.application_warehouse}")
            except ProgrammingError as err:
                generic_sql_error_handler(
                    err=err, role=self.app_role, warehouse=self.application_warehouse
                )

            # 2. Check for an existing application object by the same name
            show_app_row = self.get_existing_app_info()

            # 3. If existing application object is found, perform a few validations and upgrade the application object.
            if show_app_row:

                # Check if not created by Snowflake CLI or not created using "files on a named stage" / stage dev mode.
                if show_app_row[COMMENT_COL] not in ALLOWED_SPECIAL_COMMENTS or (
                    show_app_row[VERSION_COL] != LOOSE_FILES_MAGIC_VERSION
                ):
                    raise ApplicationAlreadyExistsError(self.app_name)

                # Check for the right owner
                ensure_correct_owner(
                    row=show_app_row, role=self.app_role, obj_name=self.app_name
                )

                # If all the above checks are in order, proceed to upgrade
                try:
                    cc.step(f"Upgrading existing application object {self.app_name}.")
                    self._execute_query(
                        f"alter application {self.app_name} upgrade using @{self.stage_fqn}"
                    )

                    # ensure debug_mode is up-to-date
                    self._execute_query(
                        f"alter application {self.app_name} set debug_mode = {self.debug_mode}"
                    )

                    return

                except ProgrammingError as err:
                    generic_sql_error_handler(err)

            # 4. If no existing application object is found, create an application object using "files on a named stage" / stage dev mode.
            cc.step(f"Creating new application {self.app_name} in account.")

            if self.app_role != self.package_role:
                with self.use_role(new_role=self.package_role):
                    self._execute_queries(
                        dedent(
                            f"""\
                        grant install, develop on application package {self.package_name} to role {self.app_role};
                        grant usage on schema {self.package_name}.{self.stage_schema} to role {self.app_role};
                        grant read on stage {self.stage_fqn} to role {self.app_role};
                        """
                        )
                    )

            stage_name = StageManager.quote_stage_name(self.stage_fqn)

            try:
                self._execute_query(
                    dedent(
                        f"""\
                    create application {self.app_name}
                        from application package {self.package_name}
                        using {stage_name}
                        debug_mode = {self.debug_mode}
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                )
            except ProgrammingError as err:
                generic_sql_error_handler(err)

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
        Get an existing version, if defined, by the same name in an application package.
        It executes a 'show versions like ... in application package' query and returns the result as single row, if one exists.
        """
        with self.use_role(self.package_role):
            try:
                version_obj = self.show_specific_object(
                    "versions",
                    version,
                    name_col=VERSION_COL,
                    in_clause=f"in application package {self.package_name}",
                )
            except ProgrammingError as err:
                if err.msg.__contains__("does not exist or not authorized"):
                    raise ApplicationPackageDoesNotExistError(self.package_name)
                else:
                    generic_sql_error_handler(err=err, role=self.package_role)

            return version_obj

    def drop_application_before_upgrade(self, policy: PolicyBase, is_interactive: bool):
        """
        This method will attempt to drop an application object if a previous upgrade fails.
        """
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
            self._execute_query(f"drop application {self.app_name}")
        except ProgrammingError as err:
            generic_sql_error_handler(err)

    def upgrade_app(
        self,
        policy: PolicyBase,
        is_interactive: bool,
        version: Optional[str] = None,
        patch: Optional[str] = None,
    ):

        patch_clause = f"patch {patch}" if patch else ""
        using_clause = f"using version {version} {patch_clause}" if version else ""

        with self.use_role(self.app_role):

            # 1. Need to use a warehouse to create an application object
            try:
                if self.application_warehouse:
                    self._execute_query(f"use warehouse {self.application_warehouse}")
            except ProgrammingError as err:
                generic_sql_error_handler(
                    err=err, role=self.app_role, warehouse=self.application_warehouse
                )

            # 2. Check for an existing application by the same name
            show_app_row = self.get_existing_app_info()

            # 3. If existing application is found, perform a few validations and upgrade the application object.
            if show_app_row:

                # We skip comment check here, because prod/pre-existing application objects may not be created by the Snowflake CLI.
                # Check for the right owner
                ensure_correct_owner(
                    row=show_app_row, role=self.app_role, obj_name=self.app_name
                )

                # If all the above checks are in order, proceed to upgrade
                try:
                    self._execute_query(
                        f"alter application {self.app_name} upgrade {using_clause}"
                    )

                    # ensure debug_mode is up-to-date
                    if using_clause:
                        self._execute_query(
                            f"alter application {self.app_name} set debug_mode = {self.debug_mode}"
                        )
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
                with self.use_role(new_role=self.package_role):
                    self._execute_query(
                        f"grant install on application package {self.package_name} to role {self.app_role}"
                    )
                    if version:
                        self._execute_query(
                            f"grant develop on application package {self.package_name} to role {self.app_role}"
                        )

            try:
                self._execute_query(
                    dedent(
                        f"""\
                    create application {self.app_name}
                        from application package {self.package_name} {using_clause}
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                )

                # ensure debug_mode is up-to-date
                if using_clause:
                    self._execute_query(
                        f"alter application {self.app_name} set debug_mode = {self.debug_mode}"
                    )
            except ProgrammingError as err:
                generic_sql_error_handler(err)

    def process(
        self,
        policy: PolicyBase,
        version: Optional[str] = None,
        patch: Optional[str] = None,
        from_release_directive: bool = False,
        is_interactive: bool = False,
        *args,
        **kwargs,
    ):
        """app run process"""

        if from_release_directive:
            self.upgrade_app(policy=policy, is_interactive=is_interactive)
            return

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

            self.upgrade_app(
                policy=policy,
                version=version,
                patch=patch,
                is_interactive=is_interactive,
            )
            return

        diff = self.deploy(prune=True, recursive=True)
        self._create_dev_app(diff)
