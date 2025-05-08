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

import json
import logging
from contextlib import contextmanager
from datetime import datetime
from functools import cache
from textwrap import dedent
from typing import Any, Dict, List, TypedDict

from snowflake.cli._plugins.connection.util import UIParameter, get_ui_parameter
from snowflake.cli._plugins.nativeapp.constants import (
    AUTHORIZE_TELEMETRY_COL,
    CHANNEL_COL,
    DEFAULT_CHANNEL,
    DEFAULT_DIRECTIVE,
    NAME_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.same_account_install_method import (
    SameAccountInstallMethod,
)
from snowflake.cli._plugins.nativeapp.sf_facade_constants import UseObjectType
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    CREATE_OR_UPGRADE_APPLICATION_EXPECTED_USER_ERROR_CODES,
    UPGRADE_RESTRICTION_CODES,
    CouldNotUseObjectError,
    InsufficientPrivilegesError,
    UnexpectedResultError,
    UpgradeApplicationRestrictionError,
    UserInputError,
    UserScriptError,
    handle_unclassified_error,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.errno import (
    ACCOUNT_DOES_NOT_EXIST,
    ACCOUNT_HAS_TOO_MANY_QUALIFIERS,
    APPLICATION_PACKAGE_MAX_VERSIONS_HIT,
    APPLICATION_PACKAGE_PATCH_ALREADY_EXISTS,
    APPLICATION_REQUIRES_TELEMETRY_SHARING,
    CANNOT_ADD_PATCH_WITH_NON_INCREASING_PATCH_NUMBER,
    CANNOT_CREATE_VERSION_WITH_NON_ZERO_PATCH,
    CANNOT_DEREGISTER_VERSION_ASSOCIATED_WITH_CHANNEL,
    CANNOT_DISABLE_MANDATORY_TELEMETRY,
    CANNOT_DISABLE_RELEASE_CHANNELS,
    CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS,
    CANNOT_SET_DEBUG_MODE_WITH_MANIFEST_VERSION,
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
    INSUFFICIENT_PRIVILEGES,
    MAX_UNBOUND_VERSIONS_REACHED,
    MAX_VERSIONS_IN_RELEASE_CHANNEL_REACHED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
    RELEASE_DIRECTIVE_DOES_NOT_EXIST,
    RELEASE_DIRECTIVE_UNAPPROVED_VERSION_OR_PATCH,
    RELEASE_DIRECTIVES_VERSION_PATCH_NOT_FOUND,
    SQL_COMPILATION_ERROR,
    TARGET_ACCOUNT_USED_BY_OTHER_RELEASE_DIRECTIVE,
    VERSION_ALREADY_ADDED_TO_RELEASE_CHANNEL,
    VERSION_DOES_NOT_EXIST,
    VERSION_NOT_ADDED_TO_RELEASE_CHANNEL,
    VERSION_NOT_IN_RELEASE_CHANNEL,
    VERSION_REFERENCED_BY_RELEASE_DIRECTIVE,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.metrics import CLICounterField
from snowflake.cli.api.project.schemas.v1.native_app.package import DistributionOptions
from snowflake.cli.api.project.util import (
    identifier_to_show_like_pattern,
    same_identifiers,
    to_identifier,
    to_string_literal,
    unquote_identifier,
)
from snowflake.cli.api.sql_execution import BaseSqlExecutor
from snowflake.cli.api.utils.cursor import find_first_row
from snowflake.connector import DictCursor, ProgrammingError

ReleaseChannel = TypedDict(
    "ReleaseChannel",
    {
        "name": str,
        "description": str,
        "created_on": datetime,
        "updated_on": datetime,
        "targets": dict[str, Any],
        "versions": list[str],
    },
)

Version = TypedDict(
    "Version",
    {
        "version": str,
        "patch": int,
        "label": str | None,
        "created_on": datetime,
        "review_status": str,
    },
)


class SnowflakeSQLFacade:
    def __init__(self, sql_executor: BaseSqlExecutor | None = None):
        self._sql_executor = (
            sql_executor if sql_executor is not None else BaseSqlExecutor()
        )
        self._log = logging.getLogger(__name__)

    def _use_object(self, object_type: UseObjectType, name: str):
        """
        Call sql to use snowflake object with error handling
        @param object_type: ObjectType, type of snowflake object to use
        @param name: object name, has to be a valid snowflake identifier.
        """
        try:
            self._sql_executor.execute_query(f"use {object_type} {name}")
        except Exception as err:
            if isinstance(err, ProgrammingError):
                if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                    raise CouldNotUseObjectError(object_type, name) from err
            handle_unclassified_error(err, f"Failed to use {object_type} {name}.")

    @contextmanager
    def _use_object_optional(self, object_type: UseObjectType, name: str | None):
        """
        Call sql to use snowflake object with error handling
        @param object_type: ObjectType, type of snowflake object to use
        @param name: object name, will be cast to a valid snowflake identifier.
        """
        if name is None:
            yield
            return

        name = to_identifier(name)
        try:
            current_obj_result_row = self._sql_executor.execute_query(
                f"select current_{object_type}()"
            ).fetchone()
        except Exception as err:
            return handle_unclassified_error(
                err, f"Failed to select current {object_type}."
            )

        try:
            prev_obj = to_identifier(current_obj_result_row[0])
        except IndexError:
            prev_obj = None

        if prev_obj is not None and same_identifiers(prev_obj, name):
            yield
            return

        self._log.debug(f"Switching to {object_type}: {name}")
        self._use_object(object_type, to_identifier(name))
        try:
            yield
        finally:
            if prev_obj is not None:
                self._log.debug(f"Switching back to {object_type}: {prev_obj}")
                self._use_object(object_type, prev_obj)

    def _use_warehouse_optional(self, new_wh: str | None):
        """
        Switches to a different warehouse for a while, then switches back.
        This is a no-op if the requested warehouse is already active or if no warehouse is passed in.
        @param new_wh: Name of the warehouse to use. If not a valid Snowflake identifier, will be converted before use.
        """
        return self._use_object_optional(UseObjectType.WAREHOUSE, new_wh)

    def _use_role_optional(self, new_role: str | None):
        """
        Switches to a different role for a while, then switches back.
        This is a no-op if the requested role is already active or if no role is passed in.
        @param new_role: Name of the role to use. If not a valid Snowflake identifier, will be converted before use.
        """
        return self._use_object_optional(UseObjectType.ROLE, new_role)

    def _use_database_optional(self, database_name: str | None):
        """
        Switch to database `database_name`, then switches back.
        This is a no-op if the requested database is already selected or if no database_name is passed in.
        @param database_name: Name of the database to use. If not a valid Snowflake identifier, will be converted before use.
        """
        return self._use_object_optional(UseObjectType.DATABASE, database_name)

    def _use_schema_optional(self, schema_name: str | None):
        """
        Switch to schema `schema_name`, then switches back.
        This is a no-op if the requested schema is already selected or if no schema_name is passed in.
        @param schema_name: Name of the schema to use. If not a valid Snowflake identifier, will be converted before use.
        """
        return self._use_object_optional(UseObjectType.SCHEMA, schema_name)

    def grant_privileges_to_role(
        self,
        privileges: list[str],
        object_type: ObjectType,
        object_identifier: str,
        role_to_grant: str,
        role_to_use: str | None = None,
    ) -> None:
        """
        Grants one or more access privileges on a securable object to a role

        @param privileges: List of privileges to grant to a role
        @param object_type: Type of snowflake object to grant to a role
        @param object_identifier: Valid identifier of the snowflake object to grant to a role
        @param role_to_grant: Name of the role to grant privileges to
        @param [Optional] role_to_use: Name of the role to use to grant privileges
        """
        comma_separated_privileges = ", ".join(privileges)
        object_type_and_name = f"{object_type.value.sf_name} {object_identifier}"

        with self._use_role_optional(role_to_use):
            try:
                self._sql_executor.execute_query(
                    f"grant {comma_separated_privileges} on {object_type_and_name} to role {to_identifier(role_to_grant)}"
                )
            except Exception as err:
                handle_unclassified_error(
                    err,
                    f"Failed to grant {comma_separated_privileges} on {object_type_and_name}"
                    f" to role {to_identifier(role_to_grant)}.",
                )

    def execute_user_script(
        self,
        queries: str,
        script_name: str,
        role: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
    ):
        """
        Runs the user-provided sql script.
        @param queries: Queries to run in this script
        @param script_name: Name of the file containing the script. Used to show logs to the user.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        @param [Optional] warehouse: Warehouse to use while running this script.
        @param [Optional] database: Database to use while running this script.
        """
        with (
            self._use_role_optional(role),
            self._use_warehouse_optional(warehouse),
            self._use_database_optional(database),
        ):
            try:
                self._sql_executor.execute_queries(queries)
            except ProgrammingError as err:
                if err.errno == NO_WAREHOUSE_SELECTED_IN_SESSION:
                    raise UserScriptError(
                        script_name,
                        f"{err.msg}. Please provide a warehouse in your project definition file, config.toml file, or via command line",
                    ) from err
                else:
                    raise UserScriptError(script_name, err.msg) from err
            except Exception as err:
                handle_unclassified_error(err, f"Failed to run script {script_name}.")

    def get_account_event_table(self, role: str | None = None) -> str | None:
        """
        Returns the name of the event table for the account.
        If the account has no event table set up or the event table is set to NONE, returns None.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """
        query = "show parameters like 'event_table' in account"
        with self._use_role_optional(role):
            try:
                results = self._sql_executor.execute_query(
                    query, cursor_class=DictCursor
                )
            except Exception as err:
                handle_unclassified_error(err, f"Failed to get event table.")
        table = next((r["value"] for r in results if r["key"] == "EVENT_TABLE"), None)
        if table is None or table == "NONE":
            return None
        return table

    def create_version_in_package(
        self,
        package_name: str,
        path_to_version_directory: str,
        version: str,
        label: str | None = None,
        role: str | None = None,
    ):
        """
        Creates a new version in an existing application package.
        @param package_name: Name of the application package to alter.
        @param path_to_version_directory: Path to artifacts on the stage to create a version from.
        @param version: Version name to create.
        @param [Optional] role: Switch to this role while executing create version.
        @param [Optional] label: Label for this version, visible to consumers.
        """

        version = to_identifier(version)
        package_name = to_identifier(package_name)

        available_release_channels = self.show_release_channels(package_name, role)

        # Label must be a string literal
        with_label_clause = (
            f"label={to_string_literal(label)}" if label is not None else ""
        )
        using_clause = (
            f"using {StageManager.quote_stage_name(path_to_version_directory)}"
        )

        action = "register" if available_release_channels else "add"

        query = dedent(
            _strip_empty_lines(
                f"""\
                    alter application package {package_name}
                        {action} version {version}
                        {using_clause}
                        {with_label_clause}
                """
            )
        )

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(query)
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == MAX_UNBOUND_VERSIONS_REACHED:
                        raise UserInputError(
                            f"Maximum unbound versions reached for application package {package_name}. "
                            "Please drop other unbound versions first, or add them to a release channel. "
                            "Use `snow app version list` to view all versions.",
                        ) from err
                    if err.errno == APPLICATION_PACKAGE_MAX_VERSIONS_HIT:
                        raise UserInputError(
                            f"Maximum versions reached for application package {package_name}. "
                            "Please drop the other versions first."
                        ) from err
                    if err.errno == CANNOT_CREATE_VERSION_WITH_NON_ZERO_PATCH:
                        raise UserInputError(
                            "Cannot create a new version with a non-zero patch in the manifest file."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to {action} version {version} to application package {package_name}.",
                )

    def drop_version_from_package(
        self, package_name: str, version: str, role: str | None = None
    ):
        """
        Drops a version from an existing application package.
        @param package_name: Name of the application package to alter.
        @param version: Version name to drop.
        @param [Optional] role: Switch to this role while executing drop version.
        """

        version = to_identifier(version)
        package_name = to_identifier(package_name)

        release_channels = self.show_release_channels(package_name, role)
        action = "deregister" if release_channels else "drop"

        query = f"alter application package {package_name} {action} version {version}"
        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(query)
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == VERSION_REFERENCED_BY_RELEASE_DIRECTIVE:
                        raise UserInputError(
                            f"Cannot drop version {version} from application package {package_name} because it is in use by one or more release directives."
                        ) from err
                    if err.errno == CANNOT_DEREGISTER_VERSION_ASSOCIATED_WITH_CHANNEL:
                        raise UserInputError(
                            f"Cannot drop version {version} from application package {package_name} because it is associated with a release channel."
                        ) from err
                    if err.errno == VERSION_DOES_NOT_EXIST:
                        raise UserInputError(
                            f"Version {version} does not exist in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to {action} version {version} from application package {package_name}.",
                )

    def add_patch_to_package_version(
        self,
        package_name: str,
        path_to_version_directory: str,
        version: str,
        patch: int | None = None,
        label: str | None = None,
        role: str | None = None,
    ) -> int:
        """
        Add a new patch, optionally a custom one, to an existing version in an application package.
        @param package_name: Name of the application package to alter.
        @param path_to_version_directory: Path to artifacts on the stage to create a version from.
        @param version: Version name to create.
        @param [Optional] patch: Patch number to create.
        @param [Optional] label: Label for this patch, visible to consumers.
        @param [Optional] role: Switch to this role while executing create version.

        @return patch number created for the version.
        """

        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)

        # Label must be a string literal
        with_label_clause = (
            f"\nlabel={to_string_literal(label)}" if label is not None else ""
        )

        patch_query = f" {patch}" if patch is not None else ""
        using_clause = StageManager.quote_stage_name(path_to_version_directory)
        # No space between patch and patch{patch_query} to avoid extra space when patch is None
        add_patch_query = dedent(
            f"""\
                 alter application package {package_name}
                     add patch{patch_query} for version {version}
                     using {using_clause}{with_label_clause}
             """
        )
        with self._use_role_optional(role):
            try:
                result_cursor = self._sql_executor.execute_query(
                    add_patch_query, cursor_class=DictCursor
                ).fetchall()
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == APPLICATION_PACKAGE_PATCH_ALREADY_EXISTS:
                        extra_message = (
                            " Check the manifest file for any hard-coded patch value."
                            if patch is None
                            else ""
                        )
                        raise UserInputError(
                            f"Patch{patch_query} already exists for version {version} in application package {package_name}.{extra_message}"
                        ) from err
                    if err.errno == CANNOT_ADD_PATCH_WITH_NON_INCREASING_PATCH_NUMBER:
                        raise UserInputError(
                            f"Cannot add a patch with a non-increasing patch number to version {version} in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to create patch{patch_query} for version {version} in application package {package_name}.",
                )
            try:
                show_row = result_cursor[0]
            except IndexError as err:
                raise UnexpectedResultError(
                    f"Expected to receive the new patch but the result is empty"
                ) from err
            new_patch = show_row["patch"]

        return new_patch

    def get_event_definitions(
        self, app_name: str, role: str | None = None
    ) -> list[dict]:
        """
        Retrieves event definitions for the specified application.
        @param app_name: Name of the application to get event definitions for.
        @return: A list of dictionaries containing event definitions.
        """
        query = (
            f"show telemetry event definitions in application {to_identifier(app_name)}"
        )
        with self._use_role_optional(role):
            try:
                results = self._sql_executor.execute_query(
                    query, cursor_class=DictCursor
                ).fetchall()
            except Exception as err:
                handle_unclassified_error(
                    err,
                    f"Failed to get event definitions for application {to_identifier(app_name)}.",
                )
        return [dict(row) for row in results]

    def get_app_properties(
        self, app_name: str, role: str | None = None
    ) -> Dict[str, str]:
        """
        Retrieve the properties of the specified application.
        @param app_name: Name of the application.
        @return: A dictionary containing the properties of the application.
        """

        query = f"desc application {to_identifier(app_name)}"
        with self._use_role_optional(role):
            try:
                results = self._sql_executor.execute_query(
                    query, cursor_class=DictCursor
                ).fetchall()
            except Exception as err:
                handle_unclassified_error(
                    err, f"Failed to describe application {to_identifier(app_name)}."
                )
        return {row["property"]: row["value"] for row in results}

    def share_telemetry_events(
        self, app_name: str, event_names: List[str], role: str | None = None
    ):
        """
        Shares the specified events from the specified application to the application package provider.
        @param app_name: Name of the application to share events from.
        @param events: List of event names to share.
        """

        self._log.info("sharing events %s", event_names)
        query = f"alter application {to_identifier(app_name)} set shared telemetry events ({', '.join([to_string_literal(x) for x in event_names])})"

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(query)
            except Exception as err:
                handle_unclassified_error(
                    err,
                    f"Failed to share telemetry events for application {to_identifier(app_name)}.",
                )

    def create_schema(
        self, name: str, role: str | None = None, database: str | None = None
    ):
        """
        Creates a schema.
        @param name: Name of the schema to create. Can be a database-qualified name or just the schema name, in which case the current database or the database passed in will be used.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        @param [Optional] database: Database to use while running this query, unless the schema name is database-qualified.
        """
        fqn = FQN.from_string(name)
        identifier = to_identifier(fqn.name)
        database = fqn.prefix or database
        with (
            self._use_role_optional(role),
            self._use_database_optional(database),
        ):
            try:
                self._sql_executor.execute_query(
                    f"create schema if not exists {identifier}"
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == INSUFFICIENT_PRIVILEGES:
                        raise InsufficientPrivilegesError(
                            f"Insufficient privileges to create schema {name}",
                            role=role,
                            database=database,
                        ) from err
                handle_unclassified_error(err, f"Failed to create schema {name}.")

    def stage_exists(
        self,
        name: str,
        role: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> bool:
        """
        Checks if a stage exists.
        @param name: Name of the stage to check for. Can be a fully qualified name or just the stage name, in which case the current database and schema or the database and schema passed in will be used.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        @param [Optional] database: Database to use while running this script, unless the stage name is database-qualified.
        @param [Optional] schema: Schema to use while running this script, unless the stage name is schema-qualified.
        """
        fqn = FQN.from_string(name)
        identifier = to_identifier(fqn.name)
        database = fqn.database or database
        schema = fqn.schema or schema

        pattern = identifier_to_show_like_pattern(identifier)
        if schema and database:
            in_schema_clause = f" in schema {database}.{schema}"
        elif schema:
            in_schema_clause = f" in schema {schema}"
        elif database:
            in_schema_clause = f" in database {database}"
        else:
            in_schema_clause = ""

        try:
            with self._use_role_optional(role):
                try:
                    results = self._sql_executor.execute_query(
                        f"show stages like {pattern}{in_schema_clause}",
                    )
                except Exception as err:
                    if isinstance(err, ProgrammingError):
                        if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                            return False
                        if err.errno == INSUFFICIENT_PRIVILEGES:
                            raise InsufficientPrivilegesError(
                                f"Insufficient privileges to check if stage {name} exists",
                                role=role,
                                database=database,
                                schema=schema,
                            ) from err
                    handle_unclassified_error(
                        err, f"Failed to check if stage {name} exists."
                    )
            return results.rowcount > 0
        except CouldNotUseObjectError:
            return False

    def create_stage(
        self,
        name: str,
        encryption_type: str = "SNOWFLAKE_SSE",
        enable_directory: bool = True,
        role: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ):
        """
        Creates a stage.
        @param name: Name of the stage to create. Can be a fully qualified name or just the stage name, in which case the current database and schema or the database and schema passed in will be used.
        @param [Optional] encryption_type: Encryption type for the stage. Default is Snowflake SSE. Pass an empty string to disable encryption.
        @param [Optional] enable_directory: Directory settings for the stage. Default is enabled.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        @param [Optional] database: Database to use while running this script, unless the stage name is database-qualified.
        @param [Optional] schema: Schema to use while running this script, unless the stage name is schema-qualified.
        """
        fqn = FQN.from_string(name)
        identifier = to_identifier(fqn.name)
        database = fqn.database or database
        schema = fqn.schema or schema

        query = f"create stage if not exists {identifier}"
        if encryption_type:
            query += f" encryption = (type = '{encryption_type}')"
        if enable_directory:
            query += f" directory = (enable = {str(enable_directory)})"
        with (
            self._use_role_optional(role),
            self._use_database_optional(database),
            self._use_schema_optional(schema),
        ):
            try:
                self._sql_executor.execute_query(query)
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == INSUFFICIENT_PRIVILEGES:
                        raise InsufficientPrivilegesError(
                            f"Insufficient privileges to create stage {name}",
                            role=role,
                            database=database,
                            schema=schema,
                        ) from err
                handle_unclassified_error(err, f"Failed to create stage {name}.")

    def show_release_directives(
        self,
        package_name: str,
        release_channel: str | None = None,
        role: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Show release directives for a package
        @param package_name: Name of the package
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """
        package_identifier = to_identifier(package_name)

        query = f"show release directives in application package {package_identifier}"
        if release_channel:
            query += f" for release channel {to_identifier(release_channel)}"

        with self._use_role_optional(role):
            try:
                cursor = self._sql_executor.execute_query(
                    query,
                    cursor_class=DictCursor,
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == INSUFFICIENT_PRIVILEGES:
                        raise InsufficientPrivilegesError(
                            f"Insufficient privileges to show release directives for application package {package_name}",
                            role=role,
                        ) from err
                    if err.errno == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                        raise UserInputError(
                            f"Application package {package_name} does not exist or you are not authorized to access it."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to show release directives for application package {package_name}.",
                )
            return cursor.fetchall()

    def get_existing_app_info(self, name: str, role: str) -> dict | None:
        """
        Check for an existing application object by the same name as in project definition, in account.
        It executes a 'show applications like' query and returns the result as single row, if one exists.
        """
        with self._use_role_optional(role):
            try:
                object_type_plural = ObjectType.APPLICATION.value.sf_plural_name
                show_obj_query = f"show {object_type_plural} like {identifier_to_show_like_pattern(name)}".strip()

                show_obj_cursor = self._sql_executor.execute_query(
                    show_obj_query, cursor_class=DictCursor
                )

                show_obj_row = find_first_row(
                    # row[NAME_COL] is not an identifier. It is the unquoted internal representation
                    show_obj_cursor,
                    lambda row: row[NAME_COL] == unquote_identifier(name),
                )
            except Exception as err:
                handle_unclassified_error(
                    err, f"Unable to fetch information on application {name}."
                )
            return show_obj_row

    def upgrade_application(
        self,
        name: str,
        install_method: SameAccountInstallMethod,
        path_to_version_directory: str,
        role: str,
        warehouse: str,
        debug_mode: bool | None,
        should_authorize_event_sharing: bool | None,
        release_channel: str | None = None,
    ) -> list[tuple[str]]:
        """
        Upgrades an application object using the provided clauses

        @param name: Name of the application object
        @param install_method: Method of installing the application
        @param path_to_version_directory: Path to directory in stage housing the application artifacts
        @param role: Role to use when creating the application and provider-side objects
        @param warehouse: Warehouse which is required to create an application object
        @param debug_mode: Whether to enable debug mode; None means not explicitly enabled or disabled
        @param should_authorize_event_sharing: Whether to enable event sharing; None means not explicitly enabled or disabled
        @param release_channel [Optional]: Release channel to use when upgrading the application
        """

        name = to_identifier(name)
        release_channel = to_identifier(release_channel or DEFAULT_CHANNEL)

        install_method.ensure_app_usable(
            app_name=name,
            app_role=role,
            show_app_row=self.get_existing_app_info(name, role),
        )

        # If all the above checks are in order, proceed to upgrade

        @cache  # only cache within the scope of this method
        def get_app_properties():
            return self.get_app_properties(name, role)

        with self._use_role_optional(role), self._use_warehouse_optional(warehouse):
            try:
                using_clause = install_method.using_clause(path_to_version_directory)

                current_release_channel = (
                    get_app_properties().get(CHANNEL_COL) or DEFAULT_CHANNEL
                )
                if unquote_identifier(release_channel) != current_release_channel:
                    raise UpgradeApplicationRestrictionError(
                        f"Application {name} is currently on release channel {current_release_channel}. Cannot upgrade to release channel {release_channel}."
                    )

                upgrade_cursor = self._sql_executor.execute_query(
                    f"alter application {name} upgrade {using_clause}",
                )

                # if debug_mode is present (controlled), ensure it is up-to-date
                if install_method.is_dev_mode:
                    if debug_mode is not None:
                        self._sql_executor.execute_query(
                            f"alter application {name} set debug_mode = {debug_mode}"
                        )

            except UpgradeApplicationRestrictionError as err:
                raise err
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno in UPGRADE_RESTRICTION_CODES:
                        raise UpgradeApplicationRestrictionError(err.msg) from err
                    if (
                        err.errno
                        in CREATE_OR_UPGRADE_APPLICATION_EXPECTED_USER_ERROR_CODES
                    ):
                        raise UserInputError(
                            f"Failed to upgrade application {name} with the following error message:\n"
                            f"{err.msg}"
                        ) from err
                handle_unclassified_error(err, f"Failed to upgrade application {name}.")

            try:
                # Only update event sharing if the current value is different as the one we want to set
                if should_authorize_event_sharing is not None:
                    current_authorize_event_sharing = (
                        get_app_properties()
                        .get(AUTHORIZE_TELEMETRY_COL, "false")
                        .lower()
                        == "true"
                    )
                    if (
                        current_authorize_event_sharing
                        != should_authorize_event_sharing
                    ):
                        self._log.info(
                            "Setting telemetry sharing authorization to %s",
                            should_authorize_event_sharing,
                        )
                        self._sql_executor.execute_query(
                            f"alter application {name} set AUTHORIZE_TELEMETRY_EVENT_SHARING = {str(should_authorize_event_sharing).upper()}"
                        )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == CANNOT_DISABLE_MANDATORY_TELEMETRY:
                        get_cli_context().metrics.set_counter(
                            CLICounterField.EVENT_SHARING_ERROR, 1
                        )
                        raise UserInputError(
                            "Could not disable telemetry event sharing for the application because it contains mandatory events. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to set AUTHORIZE_TELEMETRY_EVENT_SHARING when upgrading application {name}.",
                )

            return upgrade_cursor.fetchall()

    def create_application(
        self,
        name: str,
        package_name: str,
        install_method: SameAccountInstallMethod,
        path_to_version_directory: str,
        role: str,
        warehouse: str,
        debug_mode: bool | None,
        should_authorize_event_sharing: bool | None,
        release_channel: str | None = None,
    ) -> tuple[list[tuple[str]], list[str]]:
        """
        Creates a new application object using an application package,
        running the setup script of the application package

        @param name: Name of the application object
        @param package_name: Name of the application package to install the application from
        @param install_method: Method of installing the application
        @param path_to_version_directory: Path to directory in stage housing the application artifacts
        @param role: Role to use when creating the application and provider-side objects
        @param warehouse: Warehouse which is required to create an application object
        @param debug_mode: Whether to enable debug mode; None means not explicitly enabled or disabled
        @param should_authorize_event_sharing: Whether to enable event sharing; None means not explicitly enabled or disabled
        @param release_channel [Optional]: Release channel to use when creating the application
        @return: a tuple containing the result of the create application query and possible warning messages
        """
        package_name = to_identifier(package_name)
        name = to_identifier(name)
        release_channel = to_identifier(release_channel) if release_channel else None

        # by default, applications are created in debug mode when possible;
        # this can be overridden in the project definition
        initial_debug_mode = False
        if install_method.is_dev_mode:
            initial_debug_mode = debug_mode if debug_mode is not None else True
        authorize_telemetry_clause = ""
        if should_authorize_event_sharing is not None:
            self._log.info(
                "Setting AUTHORIZE_TELEMETRY_EVENT_SHARING to %s",
                should_authorize_event_sharing,
            )
            authorize_telemetry_clause = f"AUTHORIZE_TELEMETRY_EVENT_SHARING = {str(should_authorize_event_sharing).upper()}"

        using_clause = install_method.using_clause(path_to_version_directory)
        release_channel_clause = (
            f"using release channel {release_channel}" if release_channel else ""
        )

        with self._use_role_optional(role), self._use_warehouse_optional(warehouse):
            try:
                create_cursor = self._sql_executor.execute_query(
                    dedent(
                        _strip_empty_lines(
                            f"""\
                                create application {name}
                                    from application package {package_name}
                                    {using_clause}
                                    {release_channel_clause}
                                    {authorize_telemetry_clause}
                                    comment = {SPECIAL_COMMENT}
                            """
                        )
                    ),
                )

            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == APPLICATION_REQUIRES_TELEMETRY_SHARING:
                        get_cli_context().metrics.set_counter(
                            CLICounterField.EVENT_SHARING_ERROR, 1
                        )
                        raise UserInputError(
                            "The application package requires event sharing to be authorized. Please set 'share_mandatory_events' to true in the application telemetry section of the project definition file."
                        ) from err
                    if (
                        err.errno
                        in CREATE_OR_UPGRADE_APPLICATION_EXPECTED_USER_ERROR_CODES
                    ):
                        raise UserInputError(
                            f"Failed to create application {name} with the following error message:\n"
                            f"{err.msg}"
                        ) from err

                handle_unclassified_error(err, f"Failed to create application {name}.")

            warnings = []
            try:
                if initial_debug_mode:
                    self._sql_executor.execute_query(
                        dedent(
                            _strip_empty_lines(
                                f"""\
                                    alter application {name}
                                    set debug_mode = {initial_debug_mode}
                                """
                            )
                        )
                    )
            except Exception as err:
                if (
                    isinstance(err, ProgrammingError)
                    and err.errno == CANNOT_SET_DEBUG_MODE_WITH_MANIFEST_VERSION
                ):
                    warnings.append(
                        "Did not apply debug mode to application because the manifest version is set to 2 or higher. Please use session debugging instead."
                    )
                else:
                    warnings.append(
                        f"Failed to set debug mode for application {name}. {str(err)}"
                    )

            return create_cursor.fetchall(), warnings

    def create_application_package(
        self,
        package_name: str,
        distribution: DistributionOptions,
        enable_release_channels: bool | None = None,
        role: str | None = None,
    ) -> None:
        """
        Creates a new application package.
        @param package_name: Name of the application package to create.
        @param [Optional] enable_release_channels: Enable/Disable release channels if not None.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """
        package_name = to_identifier(package_name)

        enable_release_channels_clause = ""
        if enable_release_channels is not None:
            enable_release_channels_clause = (
                f"enable_release_channels = {str(enable_release_channels).lower()}"
            )

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    dedent(
                        _strip_empty_lines(
                            f"""\
                                create application package {package_name}
                                    comment = {SPECIAL_COMMENT}
                                    distribution = {distribution}
                                    {enable_release_channels_clause}
                            """
                        )
                    )
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == INSUFFICIENT_PRIVILEGES:
                        raise InsufficientPrivilegesError(
                            f"Insufficient privileges to create application package {package_name}",
                            role=role,
                        ) from err
                handle_unclassified_error(
                    err, f"Failed to create application package {package_name}."
                )

    def alter_application_package_properties(
        self,
        package_name: str,
        enable_release_channels: bool | None = None,
        role: str | None = None,
    ) -> None:
        """
        Alters the properties of an existing application package.
        @param package_name: Name of the application package to alter.
        @param [Optional] enable_release_channels: Enable/Disable release channels if not None.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        package_name = to_identifier(package_name)

        if enable_release_channels is not None:
            with self._use_role_optional(role):
                try:
                    self._sql_executor.execute_query(
                        dedent(
                            f"""\
                                alter application package {package_name}
                                    set enable_release_channels = {str(enable_release_channels).lower()}
                            """
                        )
                    )
                except Exception as err:
                    if isinstance(err, ProgrammingError):
                        if err.errno == INSUFFICIENT_PRIVILEGES:
                            raise InsufficientPrivilegesError(
                                f"Insufficient privileges to update enable_release_channels for application package {package_name}",
                                role=role,
                            ) from err
                        if err.errno == CANNOT_DISABLE_RELEASE_CHANNELS:
                            raise UserInputError(
                                f"Cannot disable release channels for application package {package_name} after it is enabled. Try recreating the application package."
                            ) from err
                    handle_unclassified_error(
                        err,
                        f"Failed to update enable_release_channels for application package {package_name}.",
                    )

    def get_ui_parameter(self, parameter: UIParameter, default: Any) -> Any:
        """
        Returns the value of a single UI parameter.
        If the parameter is not found, the default value is returned.

        @param parameter: UIParameter, the parameter to get the value of.
        @param default: Default value to return if the parameter is not found.
        """
        connection = self._sql_executor._conn  # noqa SLF001

        return get_ui_parameter(connection, parameter, default)

    def set_release_directive(
        self,
        package_name: str,
        release_directive: str,
        release_channel: str | None,
        target_accounts: List[str] | None,
        version: str,
        patch: int,
        role: str | None = None,
    ):
        """
        Sets a release directive for an application package.
        Default release directive does not support target accounts.
        Non-default release directives require target accounts to be specified.

        @param package_name: Name of the application package to alter.
        @param release_directive: Name of the release directive to set.
        @param release_channel: Name of the release channel to set the release directive for.
        @param target_accounts: List of target accounts for the release directive.
        @param version: Version to set the release directive for.
        @param patch: Patch number to set the release directive for.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel) if release_channel else None
        release_directive = to_identifier(release_directive)
        version = to_identifier(version)

        if same_identifiers(release_directive, DEFAULT_DIRECTIVE):
            if target_accounts:
                raise UserInputError(
                    "Default release directive does not support target accounts."
                )
            release_directive_statement = "set default release directive"
        else:
            if target_accounts:
                release_directive_statement = (
                    f"set release directive {release_directive}"
                )
            else:
                release_directive_statement = (
                    f"modify release directive {release_directive}"
                )

        release_channel_statement = (
            f"modify release channel {release_channel}" if release_channel else ""
        )

        accounts_statement = (
            f"accounts = ({','.join(target_accounts)})" if target_accounts else ""
        )

        full_query = dedent(
            _strip_empty_lines(
                f"""\
                    alter application package {package_name}
                        {release_channel_statement}
                        {release_directive_statement}
                        {accounts_statement}
                        version = {version} patch = {patch}
                """
            )
        )

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(full_query)
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if (
                        err.errno == ACCOUNT_DOES_NOT_EXIST
                        or err.errno == ACCOUNT_HAS_TOO_MANY_QUALIFIERS
                    ):
                        raise UserInputError(
                            f"Invalid account passed in.\n{str(err.msg)}"
                        ) from err
                    if err.errno == RELEASE_DIRECTIVE_DOES_NOT_EXIST:
                        raise UserInputError(
                            f"Release directive {release_directive} does not exist in application package {package_name}. Please create it first by specifying --target-accounts with the `snow app release-directive set` command."
                        ) from err
                    if err.errno == TARGET_ACCOUNT_USED_BY_OTHER_RELEASE_DIRECTIVE:
                        raise UserInputError(
                            f"Some target accounts are already referenced by other release directives in application package {package_name}.\n{str(err.msg)}"
                        ) from err
                    if err.errno == VERSION_NOT_ADDED_TO_RELEASE_CHANNEL:
                        raise UserInputError(
                            f"Version {version} is not added to release channel {release_channel}. Please add it to the release channel first."
                        ) from err
                    if err.errno == RELEASE_DIRECTIVES_VERSION_PATCH_NOT_FOUND:
                        raise UserInputError(
                            f"Patch {patch} for version {version} not found in application package {package_name}."
                        ) from err
                    if err.errno == RELEASE_DIRECTIVE_UNAPPROVED_VERSION_OR_PATCH:
                        raise UserInputError(
                            f"Version {version}, patch {patch} has not yet been approved to release to accounts outside of this organization."
                        ) from err
                    if err.errno == VERSION_DOES_NOT_EXIST:
                        raise UserInputError(
                            f"Version {version} does not exist in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to set release directive {release_directive} for application package {package_name}.",
                )

    def unset_release_directive(
        self,
        package_name: str,
        release_directive: str,
        release_channel: str | None,
        role: str | None = None,
    ):
        """
        Unsets a release directive for an application package.
        Release directive must already exist in the application package.
        Does not accept default release directive.

        @param package_name: Name of the application package to alter.
        @param release_directive: Name of the release directive to unset.
        @param release_channel: Name of the release channel to unset the release directive for.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """
        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel) if release_channel else None
        release_directive = to_identifier(release_directive)

        if same_identifiers(release_directive, DEFAULT_DIRECTIVE):
            raise UserInputError(
                "Cannot unset default release directive. Please specify a non-default release directive."
            )

        release_channel_statement = ""
        if release_channel:
            release_channel_statement = f" modify release channel {release_channel}"

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    f"alter application package {package_name}{release_channel_statement} unset release directive {release_directive}"
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == RELEASE_DIRECTIVE_DOES_NOT_EXIST:
                        raise UserInputError(
                            f"Release directive {release_directive} does not exist in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to unset release directive {release_directive} for application package {package_name}.",
                )

    def add_accounts_to_release_directive(
        self,
        package_name: str,
        release_directive: str,
        release_channel: str | None,
        target_accounts: List[str],
        role: str | None = None,
    ):
        """
        Adds target accounts to a release directive of a release channel in an application package.
        Release directive must already exist in the application package.
        Default release directive does not support target accounts.

        @param package_name: Name of the application package to alter.
        @param release_directive: Name of the release directive to add target accounts to.
        @param release_channel: Name of the release channel where the release directive belongs to.
        @param target_accounts: List of target accounts to add to the release directive.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """
        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel) if release_channel else None
        release_directive = to_identifier(release_directive)

        if same_identifiers(release_directive, DEFAULT_DIRECTIVE):
            raise UserInputError(
                "Default release directive does not support adding accounts. Please specify a non-default release directive."
            )

        release_channel_statement = ""
        if release_channel:
            release_channel_statement = f"modify release channel {release_channel}"

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    dedent(
                        _strip_empty_lines(
                            f"""\
                            alter application package {package_name}
                                {release_channel_statement}
                                modify release directive {release_directive}
                                add accounts = ({','.join(target_accounts)})
                        """
                        )
                    )
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if (
                        err.errno == ACCOUNT_DOES_NOT_EXIST
                        or err.errno == ACCOUNT_HAS_TOO_MANY_QUALIFIERS
                    ):
                        raise UserInputError(
                            f"Invalid account passed in.\n{str(err.msg)}"
                        ) from err
                    if err.errno == RELEASE_DIRECTIVE_DOES_NOT_EXIST:
                        raise UserInputError(
                            f"Release directive {release_directive} does not exist in application package {package_name}."
                        ) from err
                    if err.errno == TARGET_ACCOUNT_USED_BY_OTHER_RELEASE_DIRECTIVE:
                        raise UserInputError(
                            f"Some target accounts are already referenced by other release directives in application package {package_name}.\n{str(err.msg)}"
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to add accounts to release directive {release_directive} for application package {package_name}.",
                )

    def remove_accounts_from_release_directive(
        self,
        package_name: str,
        release_directive: str,
        release_channel: str | None,
        target_accounts: List[str],
        role: str | None = None,
    ):
        """
        Removes target accounts from a release directive of a release channel in an application package.
        Release directive must already exist in the application package.
        Default release directive does not support target accounts.

        @param package_name: Name of the application package to alter.
        @param release_directive: Name of the release directive to remove target accounts from.
        @param release_channel: Name of the release channel where the release directive belongs to.
        @param target_accounts: List of target accounts to remove from the release directive.
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """
        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel) if release_channel else None
        release_directive = to_identifier(release_directive)

        if same_identifiers(release_directive, DEFAULT_DIRECTIVE):
            raise UserInputError(
                "Default release directive does not support removing accounts. Please specify a non-default release directive."
            )

        release_channel_statement = ""
        if release_channel:
            release_channel_statement = f"modify release channel {release_channel}"

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    dedent(
                        _strip_empty_lines(
                            f"""\
                            alter application package {package_name}
                                {release_channel_statement}
                                modify release directive {release_directive}
                                remove accounts = ({','.join(target_accounts)})
                        """
                        )
                    )
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if (
                        err.errno == ACCOUNT_DOES_NOT_EXIST
                        or err.errno == ACCOUNT_HAS_TOO_MANY_QUALIFIERS
                    ):
                        raise UserInputError(
                            f"Invalid account passed in.\n{str(err.msg)}"
                        ) from err
                    if err.errno == RELEASE_DIRECTIVE_DOES_NOT_EXIST:
                        raise UserInputError(
                            f"Release directive {release_directive} does not exist in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to remove accounts from release directive {release_directive} for application package {package_name}.",
                )

    def show_release_channels(
        self, package_name: str, role: str | None = None
    ) -> list[ReleaseChannel]:
        """
        Show release channels in a package.

        @param package_name: Name of the application package
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        if (
            self.get_ui_parameter(UIParameter.NA_FEATURE_RELEASE_CHANNELS, True)
            is False
        ):
            return []

        package_identifier = to_identifier(package_name)
        results = []
        with self._use_role_optional(role):
            try:
                cursor = self._sql_executor.execute_query(
                    f"show release channels in application package {package_identifier}",
                    cursor_class=DictCursor,
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    # TODO: Temporary check for syntax until UI Parameter is available in production
                    if err.errno == SQL_COMPILATION_ERROR:
                        # Release not out yet and param not out yet
                        return []
                    if err.errno == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                        raise UserInputError(
                            f"Application package {package_name} does not exist or you are not authorized to access it."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to show release channels for application package {package_name}.",
                )

            rows = cursor.fetchall()

            for row in rows:
                targets = json.loads(row["targets"]) if row.get("targets") else {}
                versions = json.loads(row["versions"]) if row.get("versions") else []
                results.append(
                    ReleaseChannel(
                        name=row["name"],
                        description=row["description"],
                        created_on=row["created_on"],
                        updated_on=row["updated_on"],
                        targets=targets,
                        versions=versions,
                    )
                )

            return results

    def add_accounts_to_release_channel(
        self,
        package_name: str,
        release_channel: str,
        target_accounts: List[str],
        role: str | None = None,
    ):
        """
        Adds accounts to a release channel.

        @param package_name: Name of the application package
        @param release_channel: Name of the release channel
        @param target_accounts: List of target accounts to add to the release channel
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel)

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    f"alter application package {package_name} modify release channel {release_channel} add accounts = ({','.join(target_accounts)})"
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if (
                        err.errno == ACCOUNT_DOES_NOT_EXIST
                        or err.errno == ACCOUNT_HAS_TOO_MANY_QUALIFIERS
                    ):
                        raise UserInputError(
                            f"Invalid account passed in.\n{str(err.msg)}"
                        ) from err
                    if err.errno == CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS:
                        raise UserInputError(
                            f"Cannot modify accounts for release channel {release_channel} in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to add accounts to release channel {release_channel} in application package {package_name}.",
                )

    def remove_accounts_from_release_channel(
        self,
        package_name: str,
        release_channel: str,
        target_accounts: List[str],
        role: str | None = None,
    ):
        """
        Removes accounts from a release channel.

        @param package_name: Name of the application package
        @param release_channel: Name of the release channel
        @param target_accounts: List of target accounts to remove from the release channel
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel)

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    f"alter application package {package_name} modify release channel {release_channel} remove accounts = ({','.join(target_accounts)})"
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if (
                        err.errno == ACCOUNT_DOES_NOT_EXIST
                        or err.errno == ACCOUNT_HAS_TOO_MANY_QUALIFIERS
                    ):
                        raise UserInputError(
                            f"Invalid account passed in.\n{str(err.msg)}"
                        ) from err
                    if err.errno == CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS:
                        raise UserInputError(
                            f"Cannot modify accounts for release channel {release_channel} in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to remove accounts from release channel {release_channel} in application package {package_name}.",
                )

    def set_accounts_for_release_channel(
        self,
        package_name: str,
        release_channel: str,
        target_accounts: List[str],
        role: str | None = None,
    ):
        """
        Sets accounts for a release channel.

        @param package_name: Name of the application package
        @param release_channel: Name of the release channel
        @param target_accounts: List of target accounts to set for the release channel
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel)

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    f"alter application package {package_name} modify release channel {release_channel} set accounts = ({','.join(target_accounts)})"
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if (
                        err.errno == ACCOUNT_DOES_NOT_EXIST
                        or err.errno == ACCOUNT_HAS_TOO_MANY_QUALIFIERS
                    ):
                        raise UserInputError(
                            f"Invalid account passed in.\n{str(err.msg)}"
                        ) from err
                    if err.errno == CANNOT_MODIFY_RELEASE_CHANNEL_ACCOUNTS:
                        raise UserInputError(
                            f"Cannot modify accounts for release channel {release_channel} in application package {package_name}."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to set accounts for release channel {release_channel} in application package {package_name}.",
                )

    def add_version_to_release_channel(
        self,
        package_name: str,
        release_channel: str,
        version: str,
        role: str | None = None,
    ):
        """
        Adds a version to a release channel.

        @param package_name: Name of the application package
        @param release_channel: Name of the release channel
        @param version: Version to add to the release channel
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel)
        version = to_identifier(version)

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    f"alter application package {package_name} modify release channel {release_channel} add version {version}"
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == VERSION_DOES_NOT_EXIST:
                        raise UserInputError(
                            f"Version {version} does not exist in application package {package_name}."
                        ) from err
                    if err.errno == VERSION_ALREADY_ADDED_TO_RELEASE_CHANNEL:
                        raise UserInputError(
                            f"Version {version} is already added to release channel {release_channel}."
                        ) from err
                    if err.errno == MAX_VERSIONS_IN_RELEASE_CHANNEL_REACHED:
                        raise UserInputError(
                            f"Maximum number of versions allowed in release channel {release_channel} has been reached."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to add version {version} to release channel {release_channel} in application package {package_name}.",
                )

    def remove_version_from_release_channel(
        self,
        package_name: str,
        release_channel: str,
        version: str,
        role: str | None = None,
    ):
        """
        Removes a version from a release channel.

        @param package_name: Name of the application package
        @param release_channel: Name of the release channel
        @param version: Version to remove from the release channel
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """

        package_name = to_identifier(package_name)
        release_channel = to_identifier(release_channel)
        version = to_identifier(version)

        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(
                    f"alter application package {package_name} modify release channel {release_channel} drop version {version}"
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == VERSION_NOT_IN_RELEASE_CHANNEL:
                        raise UserInputError(
                            f"Version {version} is not found in release channel {release_channel}."
                        ) from err
                    if err.errno == VERSION_REFERENCED_BY_RELEASE_DIRECTIVE:
                        raise UserInputError(
                            f"Cannot remove version {version} from release channel {release_channel} as it is referenced by a release directive."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to remove version {version} from release channel {release_channel} in application package {package_name}.",
                )

    def show_versions(
        self,
        package_name: str,
        role: str | None = None,
    ) -> list[Version]:
        """
        Show all versions in an application package.

        @param package_name: Name of the application package
        @param [Optional] role: Role to switch to while running this script. Current role will be used if no role is passed in.
        """
        package_name = to_identifier(package_name)

        with self._use_role_optional(role):
            try:
                cursor = self._sql_executor.execute_query(
                    f"show versions in application package {package_name}",
                    cursor_class=DictCursor,
                )
            except Exception as err:
                if isinstance(err, ProgrammingError):
                    if err.errno == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                        raise UserInputError(
                            f"Application package {package_name} does not exist or you are not authorized to access it."
                        ) from err
                handle_unclassified_error(
                    err,
                    f"Failed to show versions for application package {package_name}.",
                )

            return cursor.fetchall()


def _strip_empty_lines(text: str) -> str:
    """
    Strips empty lines from the input string.
    Preserves the new line at the end of the string if it exists.
    """
    all_lines = text.splitlines()

    # join all non-empty lines, but preserve the new line at the end if it exists
    last_line = all_lines[-1]
    other_lines = [line for line in all_lines[:-1] if line.strip()]

    return "\n".join(other_lines) + "\n" + last_line
