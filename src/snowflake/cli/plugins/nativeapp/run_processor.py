from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional

import jinja2
import typer
from click import UsageError
from rich import print
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.plugins.nativeapp.constants import (
    COMMENT_COL,
    INTERNAL_DISTRIBUTION,
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
    VERSION_COL,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationAlreadyExistsError,
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
    InvalidPackageScriptError,
    MissingPackageScriptError,
)
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
    ensure_correct_owner,
    generic_sql_error_handler,
)
from snowflake.cli.plugins.nativeapp.policy import PolicyBase
from snowflake.cli.plugins.nativeapp.utils import find_first_row
from snowflake.cli.plugins.object.stage.diff import DiffResult
from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

UPGRADE_RESTRICTION_CODES = {93044, 93055, 93045, 93046}


class NativeAppRunProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__(project_definition, project_root)

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

            # 2. Check distribution of the existing app package
            actual_distribution = self.get_app_pkg_distribution_in_snowflake
            if not self.verify_project_distribution(actual_distribution):
                print(
                    f"Continuing to execute `snow app run` on app pkg {self.package_name} with distribution '{actual_distribution}'."
                )

            # 3. If actual_distribution is external, skip comment check
            if actual_distribution == INTERNAL_DISTRIBUTION:
                row_comment = show_obj_row[COMMENT_COL]

                if row_comment != SPECIAL_COMMENT:
                    raise ApplicationPackageAlreadyExistsError(self.package_name)

            return

        # If no app pkg pre-exists, create an app pkg, with the specified distribution in the project definition file.
        with self.use_role(self.package_role):
            print(f"Creating new application package {self.package_name} in account.")
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
                print(f"Applying package script: {self.package_scripts[i]}")
                self._execute_queries(queries)
        except ProgrammingError as err:
            generic_sql_error_handler(
                err, role=self.package_role, warehouse=self.package_warehouse
            )

    def _create_dev_app(self, diff: DiffResult) -> None:
        """
        (Re-)creates the application with our up-to-date stage.
        """
        with self.use_role(self.app_role):

            # 1. Need to use a warehouse to create an application instance
            try:
                if self.application_warehouse:
                    self._execute_query(f"use warehouse {self.application_warehouse}")
            except ProgrammingError as err:
                generic_sql_error_handler(
                    err=err, role=self.app_role, warehouse=self.application_warehouse
                )

            # 2. Check for an existing application by the same name
            show_app_row = self.get_existing_app_info()

            # 3. If existing application is found, perform a few validations and upgrade the instance.
            if show_app_row:

                # Check if not created by snowCLI or not created using "loose files" / stage dev mode.
                if show_app_row[COMMENT_COL] != SPECIAL_COMMENT or (
                    show_app_row[VERSION_COL] != LOOSE_FILES_MAGIC_VERSION
                ):
                    raise ApplicationAlreadyExistsError(self.app_name)

                # Check for the right owner
                ensure_correct_owner(
                    row=show_app_row, role=self.app_role, obj_name=self.app_name
                )

                # If all the above checks are in order, proceed to upgrade
                try:
                    if diff.has_changes():
                        print(f"Upgrading existing application {self.app_name}.")
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

            # 4. If no existing application is found, create an app using "loose files" / stage dev mode.
            print(f"Creating new application {self.app_name} in account.")

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
        Get all existing versions, if present, for an application package.
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
        Get an existing version, if present, by the same name for an application package.
        It executes a 'show versions like ... in application package' query and returns the result as single row, if one exists.
        """
        with self.use_role(self.package_role):
            show_obj_query = f"show versions like '{unquote_identifier(version)}' in application package {self.package_name}"

            try:
                show_obj_cursor = self._execute_query(
                    show_obj_query, cursor_class=DictCursor
                )
            except ProgrammingError as err:
                if err.msg.__contains__("does not exist or not authorized"):
                    raise ApplicationPackageDoesNotExistError(self.package_name)
                else:
                    generic_sql_error_handler(err=err, role=self.package_role)

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            show_obj_row = find_first_row(
                show_obj_cursor,
                lambda row: row[VERSION_COL] == unquote_identifier(version),
            )

            return show_obj_row

    def drop_application_before_upgrade(self, policy: PolicyBase, is_interactive: bool):
        """
        This method will attempt to drop an application if a previous upgrade fails.
        """
        user_prompt = (
            "Do you want the CLI to drop the existing application and recreate it?"
        )
        if not policy.should_proceed(user_prompt):
            if is_interactive:
                print("Not upgrading the application.")
                raise typer.Exit(0)
            else:
                print(
                    "Cannot upgrade the application non-interactively without --force."
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

            # 1. Need to use a warehouse to create an application instance
            try:
                if self.application_warehouse:
                    self._execute_query(f"use warehouse {self.application_warehouse}")
            except ProgrammingError as err:
                generic_sql_error_handler(
                    err=err, role=self.app_role, warehouse=self.application_warehouse
                )

            # 2. Check for an existing application by the same name
            show_app_row = self.get_existing_app_info()

            # 3. If existing application is found, perform a few validations and upgrade the instance.
            if show_app_row:

                # We skip comment check here, because prod apps/pre-existing apps may not be created by the CLI.
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
                    else:  # The existing app was created from a different process.
                        print(err.msg)
                        self.drop_application_before_upgrade(policy, is_interactive)

            # 4. With no (more) existing applications, create an app using the release directives
            print(f"Creating new application {self.app_name} in account.")

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
                        f"Application package {self.package_name} does not contain any version {version}. Use 'snow app version create' to create a version on the application package first."
                    )
            except ApplicationPackageDoesNotExistError as app_err:
                raise UsageError(
                    f"Application package {self.package_name} does not exist. Use 'snow app version create' to create an application package and/or version first."
                )

            self.upgrade_app(
                policy=policy,
                version=version,
                patch=patch,
                is_interactive=is_interactive,
            )
            return

        # 1. Create an empty application package, if none exists
        self.create_app_package()

        with self.use_role(self.package_role):
            # 2. now that the application package exists, create shared data
            self._apply_package_scripts()

            # 3. Upload files from deploy root local folder to the above stage
            diff = self.sync_deploy_root_with_stage(self.package_role)

        # 4. Create an application if none exists, else upgrade the application
        self._create_dev_app(diff)
