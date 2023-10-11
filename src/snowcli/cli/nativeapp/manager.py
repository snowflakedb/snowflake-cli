from __future__ import annotations

import logging
from pathlib import Path
from functools import cached_property
from typing import List, Optional, Literal
from click.exceptions import ClickException
from snowcli.exception import SnowflakeSQLExecutionError

from snowflake.connector.cursor import DictCursor

import jinja2

from snowcli.cli.project.util import (
    clean_identifier,
    identifier_as_part,
    extract_schema,
)
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.project.definition import (
    default_app_package,
    default_application,
    default_role,
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
from snowcli.cli.connection.util import make_snowsight_url

from snowflake.connector.cursor import DictCursor

SPECIAL_COMMENT = "GENERATED_BY_SNOWCLI"
LOOSE_FILES_MAGIC_VERSION = "dev_stage"

COMMENT_COL = "comment"
OWNER_COL = "owner"
VERSION_COL = "version"

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
    def warehouse(self) -> Optional[str]:
        return self.definition.get("application", {}).get("warehouse", None)

    @cached_property
    def project_identifier(self) -> str:
        return clean_identifier(self.definition["name"])

    @cached_property
    def package_name(self) -> str:
        return self.definition.get("package", {}).get(
            "name", default_app_package(self.project_identifier)
        )

    @cached_property
    def package_role(self) -> str:
        return self.definition.get("package", {}).get("role", None) or default_role()

    @cached_property
    def app_name(self) -> str:
        return self.definition.get("application", {}).get(
            "name", default_application(self.project_identifier)
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
        if self.warehouse:
            self._execute_query(f"use warehouse {self.warehouse}")

        for i, queries in enumerate(queued_queries):
            log.info(f"Applying package script: {self.package_scripts[i]}")
            self._execute_queries(queries)

    def _create_dev_app(self, diff: DiffResult) -> None:
        """
        (Re-)creates the application with our up-to-date stage.
        """
        with self.use_role(self.app_role):
            if self.warehouse:
                self._execute_query(f"use warehouse {self.warehouse}")

            show_app_cursor = self._execute_query(
                f"show applications like '{identifier_as_part(self.app_name)}'",
                cursor_class=DictCursor,
            )

            if show_app_cursor.rowcount != 0:
                # There can only be one possible pre-existing app with the same name
                show_app_row: dict = show_app_cursor.fetchone()
                if (
                    show_app_row[COMMENT_COL] != SPECIAL_COMMENT
                    or show_app_row[VERSION_COL] != LOOSE_FILES_MAGIC_VERSION
                ):
                    raise ApplicationAlreadyExistsError(self.app_name)

                actual_owner = show_app_row[OWNER_COL]
                if actual_owner != self.app_role:
                    raise UnexpectedOwnerError(
                        self.app_name, self.app_role, actual_owner
                    )

                if not diff.has_changes():
                    # the app already exists and is up-to-date
                    # ensure debug_mode is up-to-date
                    self._execute_query(
                        f"alter application {self.app_name} set debug_mode = {self.debug_mode}"
                    )
                    return
                else:
                    # the app needs to be re-created; drop it
                    self._execute_query(f"drop application {self.app_name}")

            # Create an app using "loose files" / stage dev mode.
            log.info(f"Creating new application {self.app_name} in account.")
            self._execute_query(
                f"""
                create application {self.app_name}
                    from application package {self.package_name}
                    using @{self.stage_fqn}
                    debug_mode = {self.debug_mode}
                    comment = {SPECIAL_COMMENT}
                """,
            )

    def app_exists(self) -> bool:
        """Returns True iff the application exists on Snowflake."""
        with self.use_role(self.app_role):
            show_app_cursor = self._execute_query(
                f"show applications like '{identifier_as_part(self.app_name)}'",
                cursor_class=DictCursor,
            )
            return show_app_cursor.rowcount != 0

    def app_run(self) -> None:
        """
        Implementation of the "snow app run" dev command.
        """
        with self.use_role(self.package_role):
            show_cursor = self._execute_query(
                f"show application packages like '{identifier_as_part(self.package_name)}'",
                cursor_class=DictCursor,
            )

            if show_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError()
            elif show_cursor.rowcount == 0:
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
                # There can only be one possible pre-existing app pkg with the same name
                show_app_row = show_cursor.fetchone()
                row_comment = show_app_row[
                    COMMENT_COL
                ]  # Because we use a DictCursor, we are guaranteed a dictionary instead of indexing through a list.

                if row_comment != SPECIAL_COMMENT:
                    raise ApplicationPackageAlreadyExistsError(self.package_name)

                actual_owner = show_app_row[OWNER_COL]
                if actual_owner != self.package_role:
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
        name = identifier_as_part(self.app_name)
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
            show_obj_query = f"{query_dict['show']} '{identifier_as_part(object_name)}'"
            show_obj_cursor = self._execute_query(
                show_obj_query, cursor_class=DictCursor
            )

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)
            elif show_obj_cursor.rowcount == 0:
                raise CouldNotDropObjectError(
                    f"Role {object_role} does not own any {log_object_type.lower()} with the name {object_name}!"
                )
            elif show_obj_cursor.rowcount > 0:
                # There can only be one possible pre-existing object with the same name
                show_obj_row = show_obj_cursor.fetchone()
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
