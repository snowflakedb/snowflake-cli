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

import logging
from contextlib import contextmanager
from textwrap import dedent
from typing import Dict
from enum import Enum
from functools import lru_cache
from textwrap import dedent
from typing import Any, Dict, List

from snowflake.cli._plugins.nativeapp.sf_facade_constants import UseObjectType
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    CouldNotUseObjectError,
    UnexpectedResultError,
    UserScriptError,
    handle_unclassified_error,
)
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.project.util import (
    is_valid_unquoted_identifier,
    to_identifier,
    to_quoted_identifier,
    to_string_literal,
)
from snowflake.cli.api.sql_execution import BaseSqlExecutor, SqlExecutor
from snowflake.connector import DictCursor, ProgrammingError


class UIParameter(Enum):
    NA_ENABLE_REGIONLESS_REDIRECT = "UI_SNOWSIGHT_ENABLE_REGIONLESS_REDIRECT"
    NA_EVENT_SHARING_V2 = "ENABLE_EVENT_SHARING_V2_IN_THE_SAME_ACCOUNT"
    NA_ENFORCE_MANDATORY_FILTERS = (
        "ENFORCE_MANDATORY_FILTERS_FOR_SAME_ACCOUNT_INSTALLATION"
    )


class SnowflakeSQLFacade:
    def __init__(self, sql_executor: SqlExecutor | None = None):
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
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                raise CouldNotUseObjectError(object_type, name) from err
            else:
                handle_unclassified_error(err, f"Failed to use {object_type} {name}.")
        except Exception as err:
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

        try:
            current_obj_result_row = self._sql_executor.execute_query(
                f"select current_{object_type}()"
            ).fetchone()
        except Exception as err:
            return handle_unclassified_error(
                err, f"Failed to select current {object_type}."
            )

        try:
            prev_obj = current_obj_result_row[0]
        except IndexError:
            prev_obj = None

        if prev_obj is not None and _same_identifier(prev_obj, name):
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
        stage_fqn: str,
        version: str,
        label: str | None = None,
        role: str | None = None,
    ):
        """
        Creates a new version in an existing application package.
        @param package_name: Name of the application package to alter.
        @param stage_fqn: Stage fully qualified name.
        @param version: Version name to create.
        @param [Optional] role: Switch to this role while executing create version.
        @param [Optional] label: Label for this version, visible to consumers.
        """

        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)

        # Label must be a string literal
        with_label_cause = (
            f"\nlabel={to_string_literal(label)}" if label is not None else ""
        )
        add_version_query = dedent(
            f"""\
                alter application package {package_name}
                    add version {version}
                    using @{stage_fqn}{with_label_cause}
            """
        )
        with self._use_role_optional(role):
            try:
                self._sql_executor.execute_query(add_version_query)
            except Exception as err:
                handle_unclassified_error(
                    err,
                    f"Failed to add version {version} to application package {package_name}.",
                )

    def add_patch_to_package_version(
        self,
        package_name: str,
        stage_fqn: str,
        version: str,
        patch: int | None = None,
        label: str | None = None,
        role: str | None = None,
    ) -> int:
        """
        Add a new patch, optionally a custom one, to an existing version in an application package.
        @param package_name: Name of the application package to alter.
        @param stage_fqn: Stage fully qualified name.
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
        patch_query = f"{patch}" if patch else ""
        add_patch_query = dedent(
            f"""\
                 alter application package {package_name}
                     add patch {patch_query} for version {version}
                     using @{stage_fqn}{with_label_clause}
             """
        )
        with self._use_role_optional(role):
            try:
                result_cursor = self._sql_executor.execute_query(
                    add_patch_query, cursor_class=DictCursor
                ).fetchall()
            except Exception as err:
                handle_unclassified_error(
                    err,
                    f"Failed to create patch {patch_query} for version {version} in application package {package_name}.",
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

    def get_ui_parameter(self, parameter: UIParameter, default: Any) -> str:
        """
        Returns the value of a single UI parameter.
        If the parameter is not found, the default value is returned.
        """

        ui_parameters = self.get_ui_parameters()
        return ui_parameters.get(parameter, default)

    @lru_cache()
    def get_ui_parameters(self) -> Dict[UIParameter, Any]:
        """
        Returns the UI parameters from the SYSTEM$BOOTSTRAP_DATA_REQUEST function
        """

        parameters_to_fetch = sorted([param.value for param in UIParameter])

        query = dedent(
            f"""
            select value['value']::string as PARAM_VALUE, value['name']::string as PARAM_NAME from table(flatten(
                input => parse_json(SYSTEM$BOOTSTRAP_DATA_REQUEST()),
                path => 'clientParamsInfo'
            )) where value['name'] in ('{"', '".join(parameters_to_fetch)}');
            """
        )

        try:
            result = self._sql_executor.execute_query(
                query, cursor_class=DictCursor
            ).fetchall()
        except Exception as err:
            handle_unclassified_error(err, "Failed to get UI parameters.")

        return {UIParameter(row["PARAM_NAME"]): row["PARAM_VALUE"] for row in result}

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


# TODO move this to src/snowflake/cli/api/project/util.py in a separate
# PR since it's codeowned by the CLI team
def _same_identifier(id1: str, id2: str) -> bool:
    """
    Returns whether two identifiers refer to the same object.

    Two unquoted identifiers are considered the same if they are equal when both are converted to uppercase
    Two quoted identifiers are considered the same if they are exactly equal
    An unquoted identifier and a quoted identifier are considered the same
      if the quoted identifier is equal to the unquoted identifier
      when the unquoted identifier is converted to uppercase and quoted
    """
    # Canonicalize the identifiers by converting unquoted identifiers to uppercase and leaving quoted identifiers as is
    canonical_id1 = id1.upper() if is_valid_unquoted_identifier(id1) else id1
    canonical_id2 = id2.upper() if is_valid_unquoted_identifier(id2) else id2

    # The canonical identifiers are equal if they are equal when both are quoted
    # (if they are already quoted, this is a no-op)
    return to_quoted_identifier(canonical_id1) == to_quoted_identifier(canonical_id2)
