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

import logging
from contextlib import contextmanager

from snowflake.cli._plugins.nativeapp.sf_facade_constants import UseObjectType
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    CouldNotUseObjectError,
    UserScriptError,
    handle_unclassified_error,
)
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.project.util import same_identifier, to_identifier
from snowflake.cli.api.sql_execution import BaseSqlExecutor, SqlExecutor
from snowflake.connector import DictCursor, ProgrammingError


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

        if prev_obj is not None and same_identifier(prev_obj, name):
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
            results = self._sql_executor.execute_query(query, cursor_class=DictCursor)
        table = next((r["value"] for r in results if r["key"] == "EVENT_TABLE"), None)
        if table is None or table == "NONE":
            return None
        return table
