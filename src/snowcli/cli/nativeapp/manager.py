from __future__ import annotations
from collections import OrderedDict

import logging
from pathlib import Path
from functools import cached_property
from typing import List, Optional
from click.exceptions import ClickException
from snowcli.exception import SnowflakeSQLExecutionError

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.project.util import clean_identifier
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.project.definition import (
    default_app_package,
    default_role,
    default_application,
)
from snowcli.output.printing import print_result
from snowcli.output.types import ObjectResult
from snowcli.cli.stage.diff import (
    DiffResult,
    stage_diff,
    sync_local_diff_with_stage,
)
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.nativeapp.artifacts import (
    build_bundle,
    translate_artifact,
    ArtifactMapping,
)

from snowflake.connector.cursor import DictCursor

SPECIAL_COMMENT = "GENERATED_BY_SNOWCLI"
COMMENT_COL = "comment"

log = logging.getLogger(__name__)


class ApplicationPackageAlreadyExistsError(ClickException):
    def __init__(self, name: str):
        super().__init__(
            f"An Application Package {name} already exists in account that may have been created without snowCLI. "
        )


class CouldNotDropObjectError(ClickException):
    """
    Could not successfully drop the required Snowflake object.
    """

    def __init__(self, message: str):
        super().__init__(message=message)


class NativeAppManager(SqlExecutionMixin):
    definition_manager: DefinitionManager

    def __init__(self, search_path: Optional[str] = None):
        super().__init__()
        self.definition_manager = DefinitionManager(search_path or "")

    @property
    def project_root(self) -> Path:
        return self.definition_manager.project_root

    @property
    def definition(self) -> dict:
        return self.definition_manager.project_definition["native_app"]

    @cached_property
    def artifacts(self) -> List[ArtifactMapping]:
        return [translate_artifact(item) for item in self.definition["artifacts"]]

    @cached_property
    def deploy_root(self) -> Path:
        return Path(self.project_root, self.definition["deploy_root"])

    def build_bundle(self) -> None:
        build_bundle(self.project_root, self.deploy_root, self.artifacts)

    def sync_deploy_root_with_stage(self, role: str, app_pkg: str):
        schema_and_stage: str = self.definition["source_stage"]

        # Does a stage already exist within the app pkg, or we need to create one?
        # Using "if not exists" should take care of either case.
        log.info("Checking if stage exists, or creating a new one if none exists.")
        with self.use_role(role):
            self._execute_query(
                f"create schema if not exists {app_pkg}.{schema_and_stage.split('.')[0]}"
            )
            self._execute_query(
                f"""
                    create stage if not exists {app_pkg}.{schema_and_stage}
                    encryption = (TYPE = 'SNOWFLAKE_SSE')"""
            )

        # Perform a diff operation and display results to the user for informational purposes
        log.info(
            f"Performing a diff between the Snowflake stage and your local deploy_root {self.deploy_root} directory."
        )
        diff: DiffResult = stage_diff(self.deploy_root, f"{app_pkg}.{schema_and_stage}")
        log.info("Listing results of diff:")
        log.info(f"New files only on your local: {','.join(diff.only_local)}")
        log.info(f"New files only on the stage: {','.join(diff.only_on_stage)}")
        log.info(
            f"Existing files modified or status unknown: {','.join(diff.different)}"
        )
        log.info(f"Existing files identical to the stage: {','.join(diff.identical)}")

        # Upload diff-ed files to app pkg stage
        sync_local_diff_with_stage(
            role=role,
            deploy_root_path=self.deploy_root,
            diff_result=diff,
            stage_path=f"{app_pkg}.{schema_and_stage}",
        )

    def app_run(self) -> SnowflakeCursor:

        project_name = clean_identifier(self.definition["name"])
        app_pkg = self.definition.get("package", {}).get(
            "name", default_app_package(project_name)
        )
        app_pkg_role = self.definition.get("package", {}).get("role", default_role())

        with self.use_role(app_pkg_role):
            show_cursor = self._execute_query(
                f"show application packages like '{app_pkg}'",
                cursor_class=DictCursor,
            )

            if show_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError()
            elif show_cursor.rowcount == 0:
                # Create an app pkg, with distribution = internal to avoid triggering security scan
                log.info(f"Creating new application package {app_pkg} in account.")
                self._execute_query(
                    f"""
                    create application package {app_pkg}
                        comment = {SPECIAL_COMMENT}
                        distribution = internal
                """
                )
            else:
                # There can only be one possible pre-existing app pkg with the same name
                show_app_row = show_cursor.fetchone()
                row_comment = show_app_row[
                    COMMENT_COL
                ]  # Because we use a DictCursor, we are guaranteed a dictionary instead of indexing through a list.

                if row_comment != SPECIAL_COMMENT:
                    raise ApplicationPackageAlreadyExistsError(app_pkg)

            # Upload files from deploy root local folder to the above stage
            self.sync_deploy_root_with_stage(app_pkg_role, app_pkg)

    def drop_object(
        self,
        object_name: str,
        object_role: str,
        object_type: str,
        query_dict: dict,
    ) -> None:
        log_object_type = (
            "Application Package" if object_type == "package" else object_type
        )

        with self.use_role(object_role):
            show_obj_cursor = self._execute_query(
                f"{query_dict['show']} '{object_name}'", cursor_class=DictCursor
            )

            if show_obj_cursor.rowcount == 0:
                raise CouldNotDropObjectError(
                    f"Role {object_role} does not own any {log_object_type.lower()} with name {object_name}!"
                )
            elif show_obj_cursor.rowcount > 0:
                # There can only be one possible pre-existing app pkg with the same name
                show_obj_row = show_obj_cursor.fetchone()
                row_comment = show_obj_row[
                    COMMENT_COL
                ]  # Because we use a DictCursor, we are guaranteed a dictionary instead of indexing through a list.

                if row_comment != SPECIAL_COMMENT:
                    raise CouldNotDropObjectError(
                        f"{log_object_type} {object_name} was not created by SnowCLI. Cannot drop the {log_object_type.lower()}."
                    )
            else:  # rowcount is None
                raise SnowflakeSQLExecutionError()

            log.info(f"Dropping {log_object_type.lower()} {object_name} now.")
            self._execute_query(f"{query_dict['drop']} {object_name}")
            log.info(f"Dropped {log_object_type.lower()} {object_name} successfully.")

    def teardown(self) -> None:
        project_name = clean_identifier(self.definition["name"])

        # Drop the application first
        self.drop_object(
            object_name=self.definition.get("application", {}).get(
                "name", default_application(project_name)
            ),
            object_role=self.definition.get("application", {}).get(
                "role", default_role(project_name)
            ),
            object_type="application",
            query_dict={"show": "show applications like", "drop": "drop application"},
        )

        # Drop the application package next
        self.drop_object(
            object_name=self.definition.get("package", {}).get(
                "name", default_app_package(project_name)
            ),
            object_role=self.definition.get("package", {}).get(
                "role", default_role(project_name)
            ),
            object_type="package",
            query_dict={
                "show": "show application packages like",
                "drop": "drop application package",
            },
        )
