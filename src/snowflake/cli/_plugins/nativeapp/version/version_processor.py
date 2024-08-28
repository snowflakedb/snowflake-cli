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
from typing import Dict, List, Optional

import typer
from click import BadOptionUsage, ClickException
from snowflake.cli._plugins.nativeapp.artifacts import (
    BundleMap,
    find_version_info_in_manifest_file,
)
from snowflake.cli._plugins.nativeapp.constants import VERSION_COL
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli._plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
)
from snowflake.cli._plugins.nativeapp.policy import PolicyBase
from snowflake.cli._plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import ensure_correct_owner
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.util import to_identifier, unquote_identifier
from snowflake.cli.api.utils.cursor import (
    find_all_rows,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor


def check_index_changes_in_git_repo(
    project_root: Path, policy: PolicyBase, is_interactive: bool
) -> None:
    """
    Checks if the project root, i.e. the native apps project is a git repository. If it is a git repository,
    it also checks if there any local changes to the directory that may not be on the application package stage.
    """
    from git import Repo
    from git.exc import InvalidGitRepositoryError

    try:
        repo = Repo(project_root, search_parent_directories=True)
        assert repo.git_dir is not None

        # Check if the repo has any changes, including untracked files
        if repo.is_dirty(untracked_files=True):
            cc.warning(
                "Changes detected in the git repository. "
                "(Rerun your command with --skip-git-check flag to ignore this check)"
            )
            repo.git.execute(["git", "status"])

            user_prompt = (
                "You have local changes in this repository that are not part of a previous commit. "
                "Do you still want to continue?"
            )
            if not policy.should_proceed(user_prompt):
                if is_interactive:
                    cc.message("Not creating a new version.")
                    raise typer.Exit(0)
                else:
                    cc.message(
                        "Cannot create a new version non-interactively without --force."
                    )
                    raise typer.Exit(1)

    except InvalidGitRepositoryError:
        pass  # not a git repository, which is acceptable


class NativeAppVersionCreateProcessor(NativeAppRunProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__(project_definition, project_root)

    def get_existing_release_directive_info_for_version(
        self, version: str
    ) -> List[dict]:
        """
        Get all existing release directives, if present, set on the version defined in an application package.
        It executes a 'show release directives in application package' query and returns the filtered results, if they exist.
        """
        with self.use_role(self.package_role):
            show_obj_query = (
                f"show release directives in application package {self.package_name}"
            )
            show_obj_cursor = self._execute_query(
                show_obj_query, cursor_class=DictCursor
            )

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            show_obj_rows = find_all_rows(
                show_obj_cursor,
                lambda row: row[VERSION_COL] == unquote_identifier(version),
            )

            return show_obj_rows

    def add_new_version(self, version: str) -> None:
        """
        Defines a new version in an existing application package.
        """
        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)
        with self.use_role(self.package_role):
            cc.step(
                f"Defining a new version {version} in application package {self.package_name}"
            )
            add_version_query = dedent(
                f"""\
                    alter application package {self.package_name}
                        add version {version}
                        using @{self.stage_fqn}
                """
            )
            self._execute_query(add_version_query, cursor_class=DictCursor)
            cc.message(
                f"Version {version} created for application package {self.package_name}."
            )

    def add_new_patch_to_version(self, version: str, patch: Optional[int] = None):
        """
        Add a new patch, optionally a custom one, to an existing version in an application package.
        """
        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)
        with self.use_role(self.package_role):
            cc.step(
                f"Adding new patch to version {version} defined in application package {self.package_name}"
            )
            add_version_query = dedent(
                f"""\
                    alter application package {self.package_name}
                        add patch {patch if patch else ""} for version {version}
                        using @{self.stage_fqn}
                """
            )
            result_cursor = self._execute_query(
                add_version_query, cursor_class=DictCursor
            )

            show_row = result_cursor.fetchall()[0]
            new_patch = show_row["patch"]
            cc.message(
                f"Patch {new_patch} created for version {version} defined in application package {self.package_name}."
            )

    def process(
        self,
        bundle_map: BundleMap,
        version: Optional[str],
        patch: Optional[int],
        policy: PolicyBase,
        git_policy: PolicyBase,
        is_interactive: bool,
        *args,
        **kwargs,
    ):
        """
        Perform bundle, application package creation, stage upload, version and/or patch to an application package.
        """

        # Make sure version is not None before proceeding any further.
        # This will raise an exception if version information is not found. Patch can be None.
        if not version:
            cc.message(
                "Version was not provided through the Snowflake CLI. Checking version in the manifest.yml instead."
            )

            version, patch = find_version_info_in_manifest_file(self.deploy_root)
            if not version:
                raise ClickException(
                    "Manifest.yml file does not contain a value for the version field."
                )

        # Check if --patch needs to throw a bad option error, either if application package does not exist or if version does not exist
        if patch:
            try:
                if not self.get_existing_version_info(version):
                    raise BadOptionUsage(
                        option_name="patch",
                        message=f"Cannot create a custom patch when version {version} is not defined in the application package {self.package_name}. Try again without using --patch.",
                    )
            except ApplicationPackageDoesNotExistError as app_err:
                raise BadOptionUsage(
                    option_name="patch",
                    message=f"Cannot create a custom patch when application package {self.package_name} does not exist. Try again without using --patch.",
                )

        if git_policy.should_proceed():
            check_index_changes_in_git_repo(
                project_root=self.project_root,
                policy=policy,
                is_interactive=is_interactive,
            )

        # TODO: consider using self.deploy() instead

        try:
            self.create_app_package()
        except ApplicationPackageAlreadyExistsError as e:
            cc.warning(e.message)
            if not policy.should_proceed("Proceed with using this package?"):
                raise typer.Abort() from e

        with self.use_role(self.package_role):
            # Now that the application package exists, create shared data
            self._apply_package_scripts()

            # Upload files from deploy root local folder to the above stage
            self.sync_deploy_root_with_stage(
                bundle_map=bundle_map,
                role=self.package_role,
                prune=True,
                recursive=True,
                stage_fqn=self.stage_fqn,
            )
            with self.use_package_warehouse():
                self.execute_package_post_deploy_hooks()

        # Warn if the version exists in a release directive(s)
        existing_release_directives = (
            self.get_existing_release_directive_info_for_version(version)
        )
        if existing_release_directives:
            release_directive_names = ", ".join(
                row["name"] for row in existing_release_directives
            )
            cc.warning(
                dedent(
                    f"""\
                    Version {version} already defined in application package {self.package_name} and in release directive(s): {release_directive_names}.
                    """
                )
            )

            user_prompt = (
                f"Are you sure you want to create a new patch for version {version} in application "
                f"package {self.package_name}? Once added, this operation cannot be undone."
            )
            if not policy.should_proceed(user_prompt):
                if is_interactive:
                    cc.message("Not creating a new patch.")
                    raise typer.Exit(0)
                else:
                    cc.message(
                        "Cannot create a new patch non-interactively without --force."
                    )
                    raise typer.Exit(1)

        # Define a new version in the application package
        if not self.get_existing_version_info(version):
            self.add_new_version(version=version)
            return  # A new version created automatically has patch 0, we do not need to further increment the patch.

        # Add a new patch to an existing (old) version
        self.add_new_patch_to_version(version=version, patch=patch)


class NativeAppVersionDropProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: NativeApp, project_root: Path):
        super().__init__(project_definition, project_root)

    def process(
        self,
        version: Optional[str],
        policy: PolicyBase,
        is_interactive: bool,
        *args,
        **kwargs,
    ):
        """
        Drops a version defined in an application package. If --force is provided, then no user prompts will be executed.
        """

        # 1. Check for existing an existing application package
        show_obj_row = self.get_existing_app_pkg_info()
        if show_obj_row:
            # Check for the right owner role
            ensure_correct_owner(
                row=show_obj_row, role=self.package_role, obj_name=self.package_name
            )
        else:
            raise ApplicationPackageDoesNotExistError(self.package_name)

        # 2. Check distribution of the existing application package
        actual_distribution = self.get_app_pkg_distribution_in_snowflake
        if not self.verify_project_distribution(actual_distribution):
            cc.warning(
                f"Continuing to execute `snow app version drop` on application package {self.package_name} with distribution '{actual_distribution}'."
            )

        # 3. If the user did not pass in a version string, determine from manifest.yml
        if not version:
            cc.message(
                dedent(
                    f"""\
                        Version was not provided through the Snowflake CLI. Checking version in the manifest.yml instead.
                        This step will bundle your app artifacts to determine the location of the manifest.yml file.
                    """
                )
            )
            self.build_bundle()
            version, _ = find_version_info_in_manifest_file(self.deploy_root)
            if not version:
                raise ClickException(
                    "Manifest.yml file does not contain a value for the version field."
                )

        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)

        cc.step(
            dedent(
                f"""\
                    About to drop version {version} in application package {self.package_name}.
                """
            )
        )

        # If user did not provide --force, ask for confirmation
        user_prompt = (
            f"Are you sure you want to drop version {version} in application package {self.package_name}? Once dropped, this operation cannot be undone.",
        )
        if not policy.should_proceed(user_prompt):
            if is_interactive:
                cc.message("Not dropping version.")
                raise typer.Exit(0)
            else:
                cc.message("Cannot drop version non-interactively without --force.")
                raise typer.Exit(1)

        # Drop the version
        with self.use_role(self.package_role):
            try:
                self._execute_query(
                    f"alter application package {self.package_name} drop version {version}"
                )
            except ProgrammingError as err:
                raise err  # e.g. version is referenced in a release directive(s)

        cc.message(
            f"Version {version} in application package {self.package_name} dropped successfully."
        )
