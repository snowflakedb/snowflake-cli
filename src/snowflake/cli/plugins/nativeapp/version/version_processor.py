import logging
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional

import typer
from click import BadOptionUsage, ClickException
from rich import print
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.plugins.nativeapp.artifacts import find_version_info_in_manifest_file
from snowflake.cli.plugins.nativeapp.constants import VERSION_COL
from snowflake.cli.plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli.plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
    ensure_correct_owner,
)
from snowflake.cli.plugins.nativeapp.policy import PolicyBase
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli.plugins.nativeapp.utils import (
    find_all_rows,
    find_first_row,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)


def check_index_changes_in_git_repo(
    project_root: Path, policy: PolicyBase, is_interactive: bool
) -> None:
    """
    Checks if the project root, i.e. the native apps project is a git repository. If it is a git repository,
    it also checks if there any local changes to the directory that may not be on the app package stage.
    """
    from git import Repo
    from git.exc import InvalidGitRepositoryError

    try:
        repo = Repo(project_root, search_parent_directories=True)
        assert repo.git_dir is not None

        # Check if the repo has any changes, including untracked files
        if repo.is_dirty(untracked_files=True):
            print("Changes detected in your git repository!")
            repo.git.execute(["git", "status"])

            user_prompt = (
                "You have local changes in this repository that are not part of a previous commit. Do you still want to continue?",
            )
            if not policy.should_proceed(user_prompt):
                if is_interactive:
                    print("Not creating a new version.")
                    raise typer.Exit(0)
                else:
                    print(
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
        Get all existing release directives, if present, set on the version for an application package.
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
        Add a new version to an existing application package.
        """
        with self.use_role(self.package_role):
            add_version_query = dedent(
                f"""\
                    alter application package {self.package_name}
                        add version {version}
                        using @{self.stage_fqn}
                """
            )
            self._execute_query(add_version_query, cursor_class=DictCursor)
            print(
                f"Version {version} created for application package {self.package_name}."
            )

    def add_new_patch_to_version(self, version: str, patch: Optional[str] = None):
        """
        Add a new patch, optionally a custom one, to an existing version of an application package.
        """
        with self.use_role(self.package_role):
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

            show_row = find_first_row(
                result_cursor,
                lambda row: row[VERSION_COL] == unquote_identifier(version),
            )

            new_patch = show_row["patch"]
            print(
                f"Patch {new_patch} created for version {version} for application package {self.package_name}."
            )

    def process(
        self,
        version: Optional[str],
        patch: Optional[str],
        policy: PolicyBase,
        git_policy: PolicyBase,
        is_interactive: bool,
        *args,
        **kwargs,
    ):
        """
        Perform bundle, app package creation, stage upload, version and/or patch to an application package.
        """

        # Make sure version is not None before proceeding any further.
        # This will raise an exception if version information is not found. Patch can be None.
        if not version:
            log.info(
                dedent(
                    f"""\
                        Version was not provided through the CLI. Checking version in the manifest.yml instead.
                    """
                )
            )
            version, patch = find_version_info_in_manifest_file(self.deploy_root)
            if not version:
                raise ClickException(
                    "Manifest.yml file does not contain a value for the version field."
                )

        # Check if --patch needs to throw a bad option error, either if app package does not exist or if version does not exist
        if patch:
            try:
                if not self.get_existing_version_info(version):
                    raise BadOptionUsage(
                        option_name="patch",
                        message=f"Cannot create a custom patch when version {version} does not exist for application package {self.package_name}. Try again without using --patch.",
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

        self.create_app_package()

        with self.use_role(self.package_role):
            # Now that the application package exists, create shared data
            self._apply_package_scripts()

            # Upload files from deploy root local folder to the above stage
            self.sync_deploy_root_with_stage(self.package_role)

        # Warn if the version exists in a release directive(s)
        existing_release_directives = (
            self.get_existing_release_directive_info_for_version(version)
        )
        if existing_release_directives:
            release_directive_names = ", ".join(
                row["name"] for row in existing_release_directives
            )
            print(
                dedent(
                    f"""\
                    Version {version} already exists for application package {self.package_name} and in release directive(s): {release_directive_names}.
                """
                )
            )

            user_prompt = (
                f"Are you sure you want to create a new patch for version {version} of application "
                f"package {self.package_name}? Once added, this operation cannot be undone."
            )
            if not policy.should_proceed(user_prompt):
                if is_interactive:
                    print("Not creating a new patch.")
                    raise typer.Exit(0)
                else:
                    print(
                        "Cannot create a new patch non-interactively without --force."
                    )
                    raise typer.Exit(1)

        # Add a new version to the app package
        if not self.get_existing_version_info(version):
            self.add_new_version(version=version)
            return  # A new version created automatically has patch 0, we do not need to further increment the patch.

        # Add a new patch to an existing (old) version
        self.add_new_patch_to_version(version=version, patch=patch)


class NativeAppVersionDropProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
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
        Drops a version associated with an application package. If --force is provided, then no user prompts will be executed.
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

        # 2. Check distribution of the existing app package
        actual_distribution = self.get_app_pkg_distribution_in_snowflake
        if not self.verify_project_distribution(actual_distribution):
            print(
                f"Continuing to execute `snow app version drop` on app pkg {self.package_name} with distribution '{actual_distribution}'."
            )

        # 3. If the user did not pass in a version string, determine from manifest.yml
        if not version:
            log.info(
                dedent(
                    f"""\
                        Version was not provided through the CLI. Checking version in the manifest.yml instead.
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

        print(
            dedent(
                f"""\
                    About to drop version {version} of application package {self.package_name}.
                """
            )
        )

        # If user did not provide --force, ask for confirmation
        user_prompt = (
            f"Are you sure you want to drop version {version} of application package {self.package_name}? Once dropped, this operation cannot be undone.",
        )
        if not policy.should_proceed(user_prompt):
            if is_interactive:
                print("Not dropping version.")
                raise typer.Exit(0)
            else:
                print("Cannot drop version non-interactively without --force.")
                raise typer.Exit(1)

        # Drop the version
        with self.use_role(self.package_role):
            try:
                self._execute_query(
                    f"alter application package {self.package_name} drop version {version}"
                )
            except ProgrammingError as err:
                raise err  # e.g. version is referenced in a release directive(s)

        print(
            f"Version {version} of application package {self.package_name} dropped successfully."
        )
