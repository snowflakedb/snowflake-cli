from pathlib import Path
from textwrap import dedent
from typing import Dict

import typer
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    EXTERNAL_DISTRIBUTION,
    INTERNAL_DISTRIBUTION,
    OWNER_COL,
)
from snowflake.cli.plugins.nativeapp.exceptions import (
    CouldNotDropApplicationPackageWithVersions,
)
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
    ensure_correct_owner,
)
from snowflake.cli.plugins.nativeapp.utils import needs_confirmation
from snowflake.connector.cursor import DictCursor


class NativeAppTeardownProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__(project_definition, project_root)

    def drop_generic_object(self, object_type: str, object_name: str, role: str):
        """
        Drop object using the given role.
        """
        with self.use_role(role):
            cc.step(f"Dropping {object_type} {object_name} now.")
            drop_query = f"drop {object_type} {object_name}"
            try:
                self._execute_query(drop_query)
            except:
                raise SnowflakeSQLExecutionError(drop_query)

            cc.message(f"Dropped {object_type} {object_name} successfully.")

    def drop_application(self, auto_yes: bool):
        """
        Attempts to drop the application object if all validations and user prompts allow so.
        """

        needs_confirm = True

        # 1. If existing application package is not found, exit gracefully
        show_obj_row = self.get_existing_app_info()
        if show_obj_row is None:
            cc.warning(
                f"Role {self.app_role} does not own any application object with the name {self.app_name}, or the application object does not exist."
            )
            return

        # 2. Check for the right owner
        ensure_correct_owner(
            row=show_obj_row, role=self.app_role, obj_name=self.app_name
        )

        # 3. Check if created by the Snowflake CLI
        row_comment = show_obj_row[COMMENT_COL]
        if row_comment in ALLOWED_SPECIAL_COMMENTS:
            # No confirmation needed before dropping
            needs_confirm = False
        else:
            if needs_confirmation(needs_confirm, auto_yes):
                should_drop_object = typer.confirm(
                    dedent(
                        f"""\
                            Application object {self.app_name} was not created by Snowflake CLI.
                            Application object details:
                            Name: {self.app_name}
                            Created on: {show_obj_row["created_on"]}
                            Source: {show_obj_row["source"]}
                            Owner: {show_obj_row[OWNER_COL]}
                            Comment: {show_obj_row[COMMENT_COL]}
                            Version: {show_obj_row["version"]}
                            Patch: {show_obj_row["patch"]}
                            Are you sure you want to drop it?
                        """
                    )
                )
                if not should_drop_object:
                    cc.message(f"Did not drop application object {self.app_name}.")
                    return  # The user desires to keep the app, therefore exit gracefully

        # 4. All validations have passed, drop object
        self.drop_generic_object(
            object_type="application", object_name=self.app_name, role=self.app_role
        )
        return  # The application object was successfully dropped, therefore exit gracefully

    def drop_package(self, auto_yes: bool):
        """
        Attempts to drop application package unless user specifies otherwise.
        """
        needs_confirm = True

        # 1. If existing application package is not found, exit gracefully
        show_obj_row = self.get_existing_app_pkg_info()
        if show_obj_row is None:
            cc.warning(
                f"Role {self.package_role} does not own any application package with the name {self.package_name}, or the application package does not exist."
            )
            return

        # 2. Check for the right owner
        ensure_correct_owner(
            row=show_obj_row, role=self.package_role, obj_name=self.package_name
        )

        with self.use_role(self.package_role):
            # 3. Check for versions in the application package
            show_versions_query = (
                f"show versions in application package {self.package_name}"
            )
            show_versions_cursor = self._execute_query(
                show_versions_query, cursor_class=DictCursor
            )
            if show_versions_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_versions_query)
            if show_versions_cursor.rowcount > 0:
                raise CouldNotDropApplicationPackageWithVersions()

        # 4. Check distribution of the existing application package
        actual_distribution = self.get_app_pkg_distribution_in_snowflake
        if not self.verify_project_distribution(actual_distribution):
            cc.warning(
                f"Continuing to execute `snow app teardown` on application package {self.package_name} with distribution '{actual_distribution}'."
            )

        # 5. If distribution is internal, check if created by the Snowflake CLI
        row_comment = show_obj_row[COMMENT_COL]
        if actual_distribution == INTERNAL_DISTRIBUTION:
            if row_comment in ALLOWED_SPECIAL_COMMENTS:
                needs_confirm = False
            else:
                if needs_confirmation(needs_confirm, auto_yes):
                    cc.warning(
                        f"Application package {self.package_name} was not created by Snowflake CLI."
                    )
        else:
            if needs_confirmation(needs_confirm, auto_yes):
                cc.warning(
                    f"Application package {self.package_name} in your Snowflake account has distribution property '{EXTERNAL_DISTRIBUTION}' and could be associated with one or more of your listings on Snowflake Marketplace."
                )

        if needs_confirmation(needs_confirm, auto_yes):
            should_drop_object = typer.confirm(
                dedent(
                    f"""\
                        Application package details:
                        Name: {self.app_name}
                        Created on: {show_obj_row["created_on"]}
                        Distribution: {actual_distribution}
                        Owner: {show_obj_row[OWNER_COL]}
                        Comment: {show_obj_row[COMMENT_COL]}
                        Are you sure you want to drop it?
                    """
                )
            )
            if not should_drop_object:
                cc.message(f"Did not drop application package {self.package_name}.")
                return  # The user desires to keep the application package, therefore exit gracefully

        # All validations have passed, drop object
        self.drop_generic_object(
            object_type="application package",
            object_name=self.package_name,
            role=self.package_role,
        )
        return  # The application package was successfully dropped, therefore exit gracefully

    def process(self, force_drop: bool = False, *args, **kwargs):

        # Drop the application object
        self.drop_application(auto_yes=force_drop)

        # Drop the application package
        self.drop_package(auto_yes=force_drop)
