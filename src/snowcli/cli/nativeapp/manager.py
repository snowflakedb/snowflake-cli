from __future__ import annotations

import logging
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import Callable, List, Literal, Optional

import jinja2
from click.exceptions import ClickException
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.connection.util import make_snowsight_url
from snowcli.cli.nativeapp.artifacts import (
    ArtifactMapping,
    build_bundle,
    translate_artifact,
)
from snowcli.cli.object.stage.diff import (
    DiffResult,
    stage_diff,
    sync_local_diff_with_stage,
)
from snowcli.cli.object.stage.manager import StageManager
from snowcli.cli.project.definition import (
    default_app_package,
    default_application,
    default_role,
)
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.project.util import (
    extract_schema,
    to_identifier,
    unquote_identifier,
)
from snowcli.exception import SnowflakeSQLExecutionError
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

SPECIAL_COMMENT = "GENERATED_BY_SNOWCLI"
LOOSE_FILES_MAGIC_VERSIONS = ["dev_stage", "UNVERSIONED"]

NAME_COL = "name"
COMMENT_COL = "comment"
OWNER_COL = "owner"
VERSION_COL = "version"

ERROR_MESSAGE_2043 = "Object does not exist, or operation cannot be performed."
ERROR_MESSAGE_606 = "No active warehouse selected in the current session."

log = logging.getLogger(__name__)


class ApplicationPackageAlreadyExistsError(ClickException):
    """An application package not created by SnowCLI exists with the same name."""

    def __init__(self, name: str):
        super().__init__(
            f"An Application Package {name} already exists in account that may have been created without snowCLI."
        )


class ApplicationAlreadyExistsError(ClickException):
    """An application not created by SnowCLI exists with the same name."""

    def __init__(self, name: str):
        super().__init__(
            f'A non-dev application "{name}" already exists in the account.'
        )


class CouldNotDropObjectError(ClickException):
    """
    Could not successfully drop the required Snowflake object.
    """

    def __init__(self, message: str):
        super().__init__(message=message)


class UnexpectedOwnerError(ClickException):
    """An operation is blocked becuase an object is owned by an unexpected role."""

    def __init__(self, item: str, expected_owner: str, actual_owner: str):
        super().__init__(
            f"Cannot operate on {item}: owned by {actual_owner} (expected {expected_owner})"
        )


class MissingPackageScriptError(ClickException):
    """A referenced package script was not found."""

    def __init__(self, relpath: str):
        super().__init__(f'Package script "{relpath}" does not exist')


class InvalidPackageScriptError(ClickException):
    """A referenced package script had syntax error(s)."""

    def __init__(self, relpath: str, err: jinja2.TemplateError):
        super().__init__(f'Package script "{relpath}" is not a valid jinja2 template')
        self.err = err


class MissingSchemaError(ClickException):
    """An identifier is missing a schema qualifier."""

    def __init__(self, identifier: str):
        super().__init__(f'Identifier missing a schema qualifier: "{identifier}"')


def find_row(cursor: DictCursor, predicate: Callable[[dict], bool]) -> Optional[dict]:
    """Returns the first row that matches the predicate, or None."""
    return next(
        (row for row in cursor.fetchall() if predicate(row)),
        None,
    )


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
    raise err


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
                log.info(f"Applying package script: {self.package_scripts[i]}")
                self._execute_queries(queries)
        except ProgrammingError as err:
            _generic_sql_error_handler(
                err, role=self.package_role, warehouse=self.package_warehouse
            )

    def _create_dev_app(self, diff: DiffResult) -> None:
        """
        (Re-)creates the application with our up-to-date stage.
        """
        with self.use_role(self.app_role):
            try:
                if self.application_warehouse:
                    self._execute_query(f"use warehouse {self.application_warehouse}")
            except ProgrammingError as err:
                _generic_sql_error_handler(
                    err=err, role=self.app_role, warehouse=self.application_warehouse
                )

            show_app_cursor = self._execute_query(
                f"show applications like '{unquote_identifier(self.app_name)}'",
                cursor_class=DictCursor,
            )

            # There can only be one possible pre-existing app with the same name
            show_app_row = find_row(
                show_app_cursor,
                lambda row: row[NAME_COL] == unquote_identifier(self.app_name),
            )

            if show_app_row is not None:
                if show_app_row[COMMENT_COL] != SPECIAL_COMMENT or (
                    show_app_row[VERSION_COL] not in LOOSE_FILES_MAGIC_VERSIONS
                ):
                    raise ApplicationAlreadyExistsError(self.app_name)

                actual_owner = show_app_row[OWNER_COL]
                if actual_owner != unquote_identifier(self.app_role):
                    raise UnexpectedOwnerError(
                        self.app_name, self.app_role, actual_owner
                    )

                try:
                    if diff.has_changes():
                        # the app needs to be upgraded
                        log.info(f"Upgrading existing application {self.app_name}.")
                        self._execute_query(
                            f"alter application {self.app_name} upgrade using @{self.stage_fqn}"
                        )

                    # ensure debug_mode is up-to-date
                    self._execute_query(
                        f"alter application {self.app_name} set debug_mode = {self.debug_mode}"
                    )
                    return
                except ProgrammingError as err:
                    _generic_sql_error_handler(err)

            # Create an app using "loose files" / stage dev mode.
            log.info(f"Creating new application {self.app_name} in account.")

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
                    f"""
                    create application {self.app_name}
                        from application package {self.package_name}
                        using {stage_name}
                        debug_mode = {self.debug_mode}
                        comment = {SPECIAL_COMMENT}
                    """,
                )
            except ProgrammingError as err:
                _generic_sql_error_handler(err)

    def app_exists(self) -> bool:
        """Returns True iff the application exists on Snowflake."""
        with self.use_role(self.app_role):
            show_app_cursor = self._execute_query(
                f"show applications like '{unquote_identifier(self.app_name)}'",
                cursor_class=DictCursor,
            )
            return (
                find_row(
                    show_app_cursor,
                    lambda row: row[NAME_COL] == unquote_identifier(self.app_name),
                )
                is not None
            )

    def app_run(self) -> None:
        """
        Implementation of the "snow app run" dev command.
        """
        with self.use_role(self.package_role):
            show_cursor = self._execute_query(
                f"show application packages like '{unquote_identifier(self.package_name)}'",
                cursor_class=DictCursor,
            )

            if show_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError()

            # There can be maximum one possible pre-existing app pkg with the same name
            show_app_row = find_row(
                show_cursor,
                lambda row: row[NAME_COL] == unquote_identifier(self.package_name),
            )
            if show_app_row is None:
                # Create an app pkg, with distribution = internal to avoid triggering security scan
                log.info(
                    f"Creating new application package {self.package_name} in account."
                )
                self._execute_query(
                    f"""
                    create application package {self.package_name}
                        comment = {SPECIAL_COMMENT}
                        distribution = internal
                    """
                )
            else:
                row_comment = show_app_row[
                    COMMENT_COL
                ]  # Because we use a DictCursor, we are guaranteed a dictionary instead of indexing through a list.

                if row_comment != SPECIAL_COMMENT:
                    raise ApplicationPackageAlreadyExistsError(self.package_name)

                actual_owner = show_app_row[OWNER_COL]
                if actual_owner != unquote_identifier(self.package_role):
                    raise UnexpectedOwnerError(
                        self.app_name, self.app_role, actual_owner
                    )

            # now that the application package exists, create shared data
            self._apply_package_scripts()

            # Upload files from deploy root local folder to the above stage
            diff = self.sync_deploy_root_with_stage(self.package_role)

        self._create_dev_app(diff)

    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        name = unquote_identifier(self.app_name)
        return make_snowsight_url(self._conn, f"/#/apps/application/{name}")

    def drop_object(
        self,
        object_name: str,
        object_role: str,
        object_type: Literal["application", "package"],
        query_dict: dict,
    ) -> None:
        """
        N.B. query_dict['show'] must be a like % clause
        """
        log_object_type = (
            "Application Package" if object_type == "package" else object_type
        )

        with self.use_role(object_role):
            show_obj_query = f"{query_dict['show']} '{unquote_identifier(object_name)}'"
            show_obj_cursor = self._execute_query(
                show_obj_query, cursor_class=DictCursor
            )

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            show_obj_row = find_row(
                show_obj_cursor,
                lambda row: row[NAME_COL] == unquote_identifier(object_name),
            )
            if show_obj_row is None:
                raise CouldNotDropObjectError(
                    f"Role {object_role} does not own any {log_object_type.lower()} with the name {object_name}!"
                )

            # There can only be one possible pre-existing object with the same name
            row_comment = show_obj_row[
                COMMENT_COL
            ]  # Because we use a DictCursor, we are guaranteed a dictionary instead of indexing through a list.

            if row_comment != SPECIAL_COMMENT:
                raise CouldNotDropObjectError(
                    f"{log_object_type} {object_name} was not created by SnowCLI. Cannot drop the {log_object_type.lower()}."
                )

            log.info(f"Dropping {log_object_type.lower()} {object_name} now.")
            drop_query = f"{query_dict['drop']} {object_name}"
            try:
                self._execute_query(drop_query)
            except:
                # Case if an object exists but owned by a different role.
                raise SnowflakeSQLExecutionError(drop_query)
            log.info(f"Dropped {log_object_type.lower()} {object_name} successfully.")

    def teardown(self) -> None:
        # Drop the application first
        self.drop_object(
            object_name=self.app_name,
            object_role=self.app_role,
            object_type="application",
            query_dict={"show": "show applications like", "drop": "drop application"},
        )

        # Drop the application package next
        self.drop_object(
            object_name=self.package_name,
            object_role=self.package_role,
            object_type="package",
            query_dict={
                "show": "show application packages like",
                "drop": "drop application package",
            },
        )
