import logging
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional

import typer
from click import ClickException
from git import Repo
from git.exc import InvalidGitRepositoryError
from rich import print
from snowcli.cli.nativeapp.artifacts import find_version_info_in_manifest_file
from snowcli.cli.nativeapp.constants import VERSION_COL
from snowcli.cli.nativeapp.exceptions import ApplicationPackageDoesNotExistError
from snowcli.cli.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
    ensure_correct_owner,
)
from snowcli.cli.nativeapp.policy import PolicyBase
from snowcli.cli.nativeapp.run_processor import NativeAppRunProcessor
from snowcli.cli.nativeapp.utils import (
    find_all_rows,
    find_first_row,
)
from snowcli.cli.project.util import unquote_identifier
from snowcli.exception import SnowflakeSQLExecutionError
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)


def check_index_changes_in_git_repo(
    project_root: Path,
    policy: PolicyBase,
) -> None:
    """
    Checks if the project root, i.e. the native apps project is a git repository. If it is a git repository,
    it also checks if there any local changes to the directory that may not be on the app package stage.
    """
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
                if policy.exit_code:
                    print(
                        "Cannot create a new version non-interactively without --force."
                    )
                else:
                    print("Not creating a new version.")
                raise typer.Exit(policy.exit_code)

    except InvalidGitRepositoryError:
        pass  # not a git repository, which is acceptable


class NativeAppVersionCreateProcessor(NativeAppRunProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__(project_definition, project_root)

    def get_existing_version_info(self, version: str) -> Optional[dict]:
        """
        Get an existing version, if present, by the same name for an application package.
        It executes a 'show versions like ... in application package' query and returns the result as single row, if one exists.
        """
        with self.use_role(self.package_role):
            show_obj_query = f"show versions like '{unquote_identifier(version)}' in application package {self.package_name}"
            show_obj_cursor = self._execute_query(
                show_obj_query, cursor_class=DictCursor
            )

            if not show_obj_cursor.rowcount:
                raise SnowflakeSQLExecutionError(show_obj_query)

            show_obj_row = find_first_row(
                show_obj_cursor,
                lambda row: row[VERSION_COL] == unquote_identifier(version),
            )

            return show_obj_row

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

            if not show_obj_cursor.rowcount:
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
        skip_git_check: bool = False,
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

        if not skip_git_check:
            check_index_changes_in_git_repo(
                project_root=self.project_root,
                policy=policy,
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
                f"Are you sure you want to create a new patch for version {version} of application package {self.package_name}? Once added, this operation cannot be undone.",
            )
            if not policy.should_proceed(user_prompt):
                if policy.exit_code:  # is 1
                    print(
                        "Cannot create a new patch non-interactively without --force."
                    )
                else:
                    print("Not creating a new patch.")
                raise typer.Exit(policy.exit_code)

        # Add a new version to the app package
        if not self.get_existing_version_info(version):
            self.add_new_version(version=version)
            return  # A new version created automatically has patch 0, we do not need to further increment the patch.

        # Add a new patch to an existing (old) version
        self.add_new_patch_to_version(version=version, patch=patch)


class NativeAppVersionDropProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__(project_definition, project_root)

    def process(self, version: Optional[str], policy: PolicyBase, *args, **kwargs):
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

        # 2. If the user did not pass in a version string, determine from manifest.yml
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
            if policy.exit_code:  # is 1
                print("Cannot drop version non-interactively without --force.")
            else:  # User did not want to drop
                print("Not dropping version.")
            raise typer.Exit(policy.exit_code)

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
