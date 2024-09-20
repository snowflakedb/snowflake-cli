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
from typing import Dict, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.nativeapp.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli._plugins.nativeapp.artifacts import (
    find_version_info_in_manifest_file,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
)
from snowflake.cli._plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
)
from snowflake.cli._plugins.nativeapp.policy import PolicyBase
from snowflake.cli._plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.v1.native_app.native_app import NativeApp
from snowflake.cli.api.project.util import to_identifier
from snowflake.connector import ProgrammingError


class NativeAppVersionCreateProcessor(NativeAppRunProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__(project_definition, project_root)

    def process(
        self,
        version: Optional[str],
        patch: Optional[int],
        force: bool,
        interactive: bool,
        skip_git_check: bool,
        *args,
        **kwargs,
    ):
        return ApplicationPackageEntity.version_create(
            console=cc,
            project_root=self.project_root,
            deploy_root=self.deploy_root,
            bundle_root=self.bundle_root,
            generated_root=self.generated_root,
            artifacts=self.artifacts,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
            prune=True,
            recursive=True,
            paths=None,
            print_diff=True,
            validate=True,
            stage_fqn=self.stage_fqn,
            package_warehouse=self.package_warehouse,
            post_deploy_hooks=self.package_post_deploy_hooks,
            package_scripts=self.package_scripts,
            version=version,
            patch=patch,
            force=force,
            interactive=interactive,
            skip_git_check=skip_git_check,
        )


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
        if not show_obj_row:
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
