import logging
from contextlib import contextmanager

from click import ClickException
from cryptography.utils import cached_property
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.entities.common import get_sql_executor
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    NO_WAREHOUSE_SELECTED_IN_SESSION,
)
from snowflake.cli.api.exceptions import CouldNotUseObjectError
from snowflake.cli.api.project.util import to_identifier
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.connector import ProgrammingError


class UnknownSQLError(Exception):
    """Exception raised when the root of the SQL error is unidentified by us."""

    # PJ-question how do we ensure exit codes remain unique
    exit_code = 3

    def __init__(self, message):
        msg = f"Unknown SQL error occurred. {message}"
        super().__init__(msg)
        self.message = msg

    def __str__(self):
        return self.message


class UserScriptError(ClickException):
    def __init__(self, script_name, msg):
        super().__init__(f"Failed to run script {script_name}. {msg}")


class SnowflakeSQLFacade:
    def __init__(self, sql_executor: SqlExecutor | None):
        self._sql_executor = (
            sql_executor if sql_executor is not None else get_sql_executor()
        )

    @cached_property
    def _log(self):
        return logging.getLogger(__name__)

    @contextmanager
    def _use_warehouse_optional(self, new_wh: str | None):
        """
        Switches to a different warehouse for a while, then switches back.
        This is a no-op if the requested warehouse is already active or if no warehouse is passed in.
        If there is no default warehouse in the account, it will throw an error.
        """
        if new_wh is None:
            yield
            return

        valid_wh_name = to_identifier(new_wh)

        wh_result = self._sql_executor.execute_query(
            f"select current_warehouse()"
        ).fetchone()

        # If user has an assigned default warehouse, prev_wh will contain a value even if the warehouse is suspended.
        try:
            prev_wh = wh_result[0]
        except:
            prev_wh = None
        # new_wh is not None, and should already be a valid identifier, no additional check is performed here.
        is_different_wh = valid_wh_name != prev_wh
        if is_different_wh:
            self._log.debug(f"Using warehouse: {valid_wh_name}")
            try:
                self._sql_executor.execute_query(f"use warehouse {valid_wh_name}")
            except ProgrammingError as err:
                # add the unauthorized case here too?
                if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                    raise CouldNotUseObjectError(
                        ObjectType.WAREHOUSE, valid_wh_name
                    ) from err

                raise ProgrammingError(
                    f"Failed to use warehouse {valid_wh_name}"
                ) from err
            except Exception as err:
                raise UnknownSQLError(
                    f"Failed to use warehouse {valid_wh_name}"
                ) from err
        try:
            yield
        finally:
            if is_different_wh and prev_wh is not None:
                self._log.debug(f"Switching back to warehouse:{prev_wh}")
                self._sql_executor.execute_query(f"use warehouse {prev_wh}")

    @contextmanager
    def _use_role_optional(self, new_role: str | None):
        """
        Switches to a different role for a while, then switches back.
        This is a no-op if the requested role is already active or if no role is passed in.
        """
        if new_role is None:
            yield
            return

        valid_role_name = to_identifier(new_role)

        prev_role = self._sql_executor.current_role()

        is_different_role = valid_role_name.lower() != prev_role.lower()
        if is_different_role:
            self._log.debug(f"Assuming different role: {valid_role_name}")
            try:
                self._sql_executor.execute_query(f"use role {valid_role_name}")
            except ProgrammingError as err:
                if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                    raise CouldNotUseObjectError(
                        ObjectType.ROLE, valid_role_name
                    ) from err

                raise ProgrammingError(f"Failed to use role {valid_role_name}") from err
            except Exception as err:
                raise UnknownSQLError(f"Failed to use role {valid_role_name}") from err
        try:
            yield
        finally:
            if is_different_role:
                self._log.debug(f"Switching back to role:{prev_role}")
                self._sql_executor.execute_query(f"use role {prev_role}")

    @contextmanager
    def _use_database_optional(self, database_name: str | None):
        """
        Switch to database `database_name`. No-op if no database is passed in.
        UPDATE DOCSTRING (identifier will be checked and converted etc)
        CONFIGURE PYCHARM TO format automatically
        """

        if database_name is None:
            yield
            return

        valid_name = to_identifier(database_name)

        db_result = self._sql_executor.execute_query(
            f"select current_database()"
        ).fetchone()
        try:
            prev_db = db_result[0]
        except:
            prev_db = None

        is_different_db = valid_name != prev_db
        if is_different_db:
            self._log.debug(f"Using database {valid_name}")
            try:
                self._sql_executor.execute_query(f"use database {valid_name}")
            except ProgrammingError as err:
                if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                    raise CouldNotUseObjectError(
                        ObjectType.DATABASE, valid_name
                    ) from err

                raise ProgrammingError(f"Failed to use database {valid_name}") from err
            except Exception as err:
                raise UnknownSQLError(f"Failed to use database {valid_name}") from err
        try:
            yield
        finally:
            if is_different_db and prev_db is not None:
                self._log.debug(f"Switching back to database:{prev_db}")
                self._sql_executor.execute_query(f"use database {prev_db}")

    def execute_user_script(
        self,
        queries: str,
        script_name: str,
        role: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
    ):
        with self._use_role_optional(role):
            with self._use_warehouse_optional(warehouse):
                with self._use_database_optional(database):
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
                        raise UnknownSQLError(
                            f"Failed to run script {script_name}"
                        ) from err
