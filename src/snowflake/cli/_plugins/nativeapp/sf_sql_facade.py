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
from snowflake.cli.api.project.util import to_identifier
from snowflake.cli.api.sql_execution import BaseSqlExecutor, SqlExecutor
from snowflake.connector import ProgrammingError


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
    def _use_warehouse_optional(self, new_wh: str | None):
        """
        Switches to a different warehouse for a while, then switches back.
        This is a no-op if the requested warehouse is already active or if no warehouse is passed in.
        @param new_wh: Name of the warehouse to use. If not a valid Snowflake identifier, will be converted before use.
        """
        if new_wh is None:
            yield
            return

        valid_wh_name = to_identifier(new_wh)

        try:
            wh_result = self._sql_executor.execute_query(
                "select current_warehouse()"
            ).fetchone()
        except Exception as err:
            return handle_unclassified_error(err, "Failed to select current warehouse.")

        # If user has an assigned default warehouse, prev_wh will contain a value even if the warehouse is suspended.
        try:
            prev_wh = wh_result[0]
        except:
            prev_wh = None
        # new_wh is not None, and should already be a valid identifier, no additional check is performed here.
        is_different_wh = valid_wh_name != prev_wh
        if is_different_wh:
            self._log.debug(f"Using warehouse: {valid_wh_name}")
            self._use_object(UseObjectType.WAREHOUSE, valid_wh_name)
        try:
            yield
        finally:
            if is_different_wh and prev_wh is not None:
                self._log.debug(f"Switching back to warehouse: {prev_wh}")
                self._use_object(UseObjectType.WAREHOUSE, prev_wh)

    @contextmanager
    def _use_role_optional(self, new_role: str | None):
        """
        Switches to a different role for a while, then switches back.
        This is a no-op if the requested role is already active or if no role is passed in.
        @param new_role: Name of the role to use. If not a valid Snowflake identifier, will be converted before use.
        """
        if new_role is None:
            yield
            return

        valid_role_name = to_identifier(new_role)
        try:
            prev_role_res = self._sql_executor.execute_query(
                "select current_role()"
            ).fetchone()
        except Exception as err:
            return handle_unclassified_error(err, "Failed to select current role.")

        prev_role = prev_role_res[0]
        is_different_role = valid_role_name.lower() != prev_role.lower()
        if is_different_role:
            self._log.debug(f"Assuming different role: {valid_role_name}")
            self._use_object(UseObjectType.ROLE, valid_role_name)
        try:
            yield
        finally:
            if is_different_role:
                self._log.debug(f"Switching back to role: {prev_role}")
                self._use_object(UseObjectType.ROLE, prev_role)

    @contextmanager
    def _use_database_optional(self, database_name: str | None):
        """
        Switch to database `database_name`, then switches back.
        This is a no-op if the requested database is already selected or if no database_name is passed in.
        @param database_name: Name of the database to use. If not a valid Snowflake identifier, will be converted before use.
        """

        if database_name is None:
            yield
            return

        valid_name = to_identifier(database_name)

        try:
            db_result = self._sql_executor.execute_query(
                "select current_database()"
            ).fetchone()
        except Exception as err:
            return handle_unclassified_error(err, "Failed to select current database.")

        try:
            prev_db = db_result[0]
        except:
            prev_db = None

        is_different_db = valid_name != prev_db
        if is_different_db:
            self._log.debug(f"Using database {valid_name}")
            self._use_object(UseObjectType.DATABASE, valid_name)

        try:
            yield
        finally:
            if is_different_db and prev_db is not None:
                self._log.debug(f"Switching back to database: {prev_db}")
                self._use_object(UseObjectType.DATABASE, prev_db)

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
