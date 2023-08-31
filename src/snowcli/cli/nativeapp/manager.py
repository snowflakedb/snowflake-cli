from __future__ import annotations

import logging
import sys
import jinja2
from pathlib import Path
import subprocess

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.utils import get_client_git_version

log = logging.getLogger(__name__)


class NativeAppManager(SqlExecutionMixin):
    def nativeapp_init(self, name: str, template: str | None = None):
        """
        Initialize a Native Apps project in the user's local directory, with or without the use of a template.
        """

        current_working_directory = Path.cwd()

        # If current directory is already contains a file named snowflake.yml, i.e. is a native apps project, fail init command.
        # We do not validate the yml here though.
        path_to_snowflake_yml = current_working_directory.joinpath("snowflake.yml")
        if Path.is_file(path_to_snowflake_yml):
            sys.exit(
                "Cannot initialize a new project within an existing Native Application project!"
            )

        # If a subdirectory with the same name as name exists in the current directory, fail init command
        path_to_project = current_working_directory.joinpath(name)
        if Path.exists(path_to_project):
            sys.exit(
                f"This directory already contains a sub-directory called {name}. Please try a different name."
            )

        if template:  # If user provided a template, use the template
            # Implementation to be added as part of https://snowflakecomputing.atlassian.net/browse/SNOW-896905
            pass
        else:  # No template provided, use Native Apps Basic Template
            try:
                # The logic makes use of git sparse checkout, which was introduced in git 2.25.0. Check client's installed git version.
                if get_client_git_version() >= float(2.25):
                    self.init_without_project_template(
                        current_working_directory=current_working_directory,
                        project_name=name,
                    )
                else:
                    sys.exit(
                        "Init requires git version to be at least 2.25.0. Please update git and try again."
                    )

            except subprocess.CalledProcessError as err:
                log.error(err.stderr)
                sys.exit(err.returncode)

        # Successful Exit
        sys.exit(0)

    def init_without_project_template(
        self, current_working_directory: Path, project_name: str
    ):
        """
        Initialize a Native Apps project without any template specified by the user.
        """

        snowflakelabs_org_github_url = "https://github.com/Snowflake-Labs/"
        nativeapps_repo = "native-apps-templates"
        basic_template = "native-app-basic"

        try:
            # Checkout the basic template, which will now reside at ./native-apps-templates/native-app-basic
            self.sparse_checkout(
                git_url=f"{snowflakelabs_org_github_url}{nativeapps_repo}",
                git_repo=nativeapps_repo,
                directory_name=basic_template,
            )

            # Move native-app-basic to current_working_directory and rename to name
            self.move_and_rename_project(
                common_path=current_working_directory,
                parent_directory=nativeapps_repo,
                directory_name=basic_template,
                new_name=project_name,
            )

            # Delete old git repo
            subprocess.run(f"rm -rf {nativeapps_repo}", shell=True)

            # Render snowflake.yml file from its jinja template
            parent_to_snowflake_yml = current_working_directory.joinpath(project_name)
            snowflake_yml_jinja = "snowflake.yml.jinja"
            env = jinja2.Environment(
                loader=jinja2.loaders.FileSystemLoader(parent_to_snowflake_yml),
                keep_trailing_newline=True,
            )
            snowflake_yml_template = env.get_template(snowflake_yml_jinja)
            output_yml_file = parent_to_snowflake_yml.joinpath("snowflake.yml")
            output_yml_file.write_text(
                snowflake_yml_template.render(project_name=project_name)
            )
            subprocess.run(
                f"rm {snowflake_yml_jinja}",
                shell=True,
                cwd=str(parent_to_snowflake_yml),
            )
        except subprocess.CalledProcessError as err:
            log.error(err.stderr)
            sys.exit(err.returncode)

    def sparse_checkout(self, git_url: str, git_repo: str, directory_name: str):
        """
        Clone the request directory from the provided git url.
        """

        clone_command = f"git clone -n --depth=1 --filter=tree:0 {git_url}"
        sparse_checkout_command = f"""
            cd {git_repo} &&
                git sparse-checkout set --no-cone {directory_name} &&
                    git checkout
            """
        try:
            subprocess.run(
                clone_command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            subprocess.run(
                sparse_checkout_command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as err:
            log.error(err.stderr)

    def move_and_rename_project(
        self,
        common_path: Path,
        parent_directory: str,
        directory_name: str,
        new_name: str,
    ):
        """
        Move the newly cloned repository's directory to its parent's parent directory, and rename to new_name.
        """

        # Move to grand-parent directory
        source_path = common_path.joinpath(parent_directory, directory_name)
        destination_path = common_path
        source_path.rename(destination_path / source_path.name)

        # Rename directory
        old_name = common_path.joinpath(directory_name)
        old_name.rename(old_name.parent / new_name)
