from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.connection.util import make_snowsight_url
from snowcli.cli.nativeapp.artifacts import (
    ArtifactMapping,
    build_bundle,
    translate_artifact,
)
from snowcli.cli.nativeapp.constants import (
    ERROR_MESSAGE_606,
    ERROR_MESSAGE_2043,
    NAME_COL,
    OWNER_COL,
)
from snowcli.cli.nativeapp.exceptions import UnexpectedOwnerError
from snowcli.cli.nativeapp.utils import find_row
from snowcli.cli.object.stage.diff import (
    DiffResult,
    stage_diff,
    sync_local_diff_with_stage,
)
from snowcli.cli.project.definition import (
    default_app_package,
    default_application,
    default_role,
)
from snowcli.cli.project.util import (
    extract_schema,
    to_identifier,
    unquote_identifier,
)
from snowcli.exception import SnowflakeSQLExecutionError
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)


def _generic_sql_error_handler(
    err: ProgrammingError, role: Optional[str] = None, warehouse: Optional[str] = None
):
    # Potential refactor: If moving away from python 3.8 and 3.9 to >= 3.10, use match ... case
    if err.errno == 2043 or err.msg.__contains__(ERROR_MESSAGE_2043):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                '{role}' may not have access to warehouse '{warehouse}'.
                Please grant usage privilege on warehouse to this role.
                """
            ),
            errno=err.errno,
        )
    elif err.errno == 606 or err.msg.__contains__(ERROR_MESSAGE_606):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please provide a warehouse for the active session role in your project definition file, config.toml file, or via command line.
                """
            ),
            errno=err.errno,
        )
    elif err.msg.__contains__("does not exist or not authorized"):
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Received error message '{err.msg}' while executing SQL statement.
                Please check the name of the resource you are trying to query or the permissions of the role you are using to run the query.
                """
            )
        )
    raise err


def is_correct_owner(row: dict, role: str, obj_name: str) -> bool:
    """
    Check if an object has the right owner role
    """
    actual_owner = row[
        OWNER_COL
    ].upper()  # Because unquote_identifier() always returns uppercase str
    if actual_owner != unquote_identifier(role):
        raise UnexpectedOwnerError(obj_name, role, actual_owner)
    return True


class NativeAppCommandProcessor(ABC):
    @abstractmethod
    def process(self, *args, **kwargs):
        pass


class NativeAppManager(SqlExecutionMixin):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__()
        self._project_root = project_root
        self._project_definition = project_definition

    @property
    def project_root(self) -> Path:
        return self._project_root

    @property
    def definition(self) -> Dict:
        return self._project_definition

    @cached_property
    def artifacts(self) -> List[ArtifactMapping]:
        return [translate_artifact(item) for item in self.definition["artifacts"]]

    @cached_property
    def deploy_root(self) -> Path:
        return Path(self.project_root, self.definition["deploy_root"])

    @cached_property
    def package_scripts(self) -> List[str]:
        """
        Relative paths to package scripts from the project root.
        """
        return self.definition.get("package", {}).get("scripts", [])

    @cached_property
    def stage_fqn(self) -> str:
        return f'{self.package_name}.{self.definition["source_stage"]}'

    @cached_property
    def stage_schema(self) -> Optional[str]:
        return extract_schema(self.stage_fqn)

    @cached_property
    def package_warehouse(self) -> Optional[str]:
        return self.definition.get("package", {}).get("warehouse", self._conn.warehouse)

    @cached_property
    def application_warehouse(self) -> Optional[str]:
        return self.definition.get("application", {}).get(
            "warehouse", self._conn.warehouse
        )

    @cached_property
    def project_identifier(self) -> str:
        # name is expected to be a valid Snowflake identifier, but PyYAML
        # will sometimes strip out double quotes so we try to get them back here.
        return to_identifier(self.definition["name"])

    @cached_property
    def package_name(self) -> str:
        return to_identifier(
            self.definition.get("package", {}).get(
                "name", default_app_package(self.project_identifier)
            )
        )

    @cached_property
    def package_role(self) -> str:
        return self.definition.get("package", {}).get("role", None) or default_role()

    @cached_property
    def package_distribution(self) -> str:
        return (
            self.definition.get("package", {}).get("distribution", "internal").lower()
        )

    @cached_property
    def app_name(self) -> str:
        return to_identifier(
            self.definition.get("application", {}).get(
                "name", default_application(self.project_identifier)
            )
        )

    @cached_property
    def app_role(self) -> str:
        return (
            self.definition.get("application", {}).get("role", None) or default_role()
        )

    @cached_property
    def debug_mode(self) -> bool:
        return self.definition.get("application", {}).get("debug", True)

    @cached_property
    def get_app_pkg_distribution_in_snowflake(self) -> str:
        """
        Returns the 'distribution' attribute of a 'describe application package' SQL query, in lowercase.
        """
        with self.use_role(self.package_role):
            try:
                desc_cursor = self._execute_query(
                    f"describe application package {self.package_name}"
                )
            except ProgrammingError as err:
                _generic_sql_error_handler(err)

            if desc_cursor.rowcount is None or desc_cursor.rowcount == 0:
                raise SnowflakeSQLExecutionError()
            else:
                for row in desc_cursor:
                    if row[0].lower() == "distribution":
                        return row[1].lower()
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Could not find the 'distribution' attribute for app package {self.package_name} in the output of SQL query:
                'describe application package {self.package_name}'
                """
            )
        )

    def is_app_pkg_distribution_same_in_sf(self) -> bool:
        """
        Returns true if the 'distribution' attribute of an existing application package in snowflake
        is the same as the the attribute specified in project definition file.
        """
        actual_distribution = self.get_app_pkg_distribution_in_snowflake
        project_def_distribution = self.package_distribution.lower()
        if actual_distribution != project_def_distribution:
            log.warning(
                dedent(
                    f"""\
                    App pkg {self.package_name} in your Snowflake account has distribution property {actual_distribution},
                    which does not match the value specified in project definition file: {project_def_distribution}.
                    """
                )
            )
            return False
        return True

    def build_bundle(self) -> None:
        """
        Populates the local deploy root from artifact sources.
        """
        build_bundle(self.project_root, self.deploy_root, self.artifacts)

    def sync_deploy_root_with_stage(self, role: str) -> DiffResult:
        """
        Ensures that the files on our remote stage match the artifacts we have in
        the local filesystem. Returns the DiffResult used to make changes.
        """

        # Does a stage already exist within the app pkg, or we need to create one?
        # Using "if not exists" should take care of either case.
        log.info("Checking if stage exists, or creating a new one if none exists.")
        with self.use_role(role):
            self._execute_query(
                f"create schema if not exists {self.package_name}.{self.stage_schema}"
            )
            self._execute_query(
                f"""
                    create stage if not exists {self.stage_fqn}
                    encryption = (TYPE = 'SNOWFLAKE_SSE')"""
            )

        # Perform a diff operation and display results to the user for informational purposes
        log.info(
            f'Performing a diff between the Snowflake stage and your local deploy_root ("{self.deploy_root}") directory.'
        )
        diff: DiffResult = stage_diff(self.deploy_root, self.stage_fqn)
        log.info("Listing results of diff:")
        log.info(f"New files only on your local: {','.join(diff.only_local)}")
        log.info(f"New files only on the stage: {','.join(diff.only_on_stage)}")
        log.info(
            f"Existing files modified or status unknown: {','.join(diff.different)}"
        )
        log.info(f"Existing files identical to the stage: {','.join(diff.identical)}")

        # Upload diff-ed files to app pkg stage
        if diff.has_changes():
            log.info(
                f"Uploading diff-ed files from your local {self.deploy_root} directory to the Snowflake stage."
            )
            sync_local_diff_with_stage(
                role=role,
                deploy_root_path=self.deploy_root,
                diff_result=diff,
                stage_path=self.stage_fqn,
            )
        return diff

    def get_existing_app_info(self) -> dict:
        """
        Check for an existing application by the same name as in project definition, in account
        """
        with self.use_role(self.app_role):
            show_obj_query = (
                f"show applications like '{unquote_identifier(self.app_name)}'"
            )
            show_obj_cursor = self._execute_query(
                show_obj_query,
                cursor_class=DictCursor,
            )

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            show_obj_row = find_row(
                show_obj_cursor,
                lambda row: row[NAME_COL] == unquote_identifier(self.app_name),
            )

            return show_obj_row

    def get_existing_app_pkg_info(self) -> Optional[dict]:
        """
        Check for an existing application package by the same name as in project definition, in account
        """

        with self.use_role(self.package_role):
            show_obj_query = f"show application packages like '{unquote_identifier(self.package_name)}'"
            show_obj_cursor = self._execute_query(
                show_obj_query, cursor_class=DictCursor
            )

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            show_obj_row = find_row(
                show_obj_cursor,
                lambda row: row[NAME_COL] == unquote_identifier(self.package_name),
            )

            return show_obj_row  # Can be None or a dict

    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        name = unquote_identifier(self.app_name)
        return make_snowsight_url(self._conn, f"/#/apps/application/{name}")
