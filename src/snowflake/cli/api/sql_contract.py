from contextlib import contextmanager

from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.entities.common import get_sql_executor
from snowflake.cli.api.entities.utils import generic_sql_error_handler
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED
from snowflake.cli.api.exceptions import CouldNotUseObjectError
from snowflake.connector import ProgrammingError


class UnknownSQLError(Exception):
    def __init__(self, msg):
        super().__init__(f"Unknown SQL error occurred. {msg}")


class CannotUseRoleError(CouldNotUseObjectError):
    def __init__(self, role):
        self.role = role
        super().__init__(ObjectType.ROLE, role)


class SQLService:
    _sql_executor = get_sql_executor()

    # TODO: Extract common to a _use_object_optional
    @contextmanager
    def _use_warehouse_optional(self, new_wh: str | None):
        """
        Switches to a different warehouse for a while, then switches back.
        This is a no-op if the requested warehouse is already active or if no warehouse is passed in.
        If there is no default warehouse in the account, it will throw an error.
        """
        if new_wh is None:
            yield
        wh_result = self._sql_executor.execute_query(
            f"select current_warehouse()"
        ).fetchone()

        # If user has an assigned default warehouse, prev_wh will contain a value even if the warehouse is suspended.
        try:
            prev_wh = wh_result[0]
        except:
            prev_wh = None
        # new_wh is not None, and should already be a valid identifier, no additional check is performed here.
        is_different_wh = new_wh != prev_wh
        if is_different_wh:
            self._sql_executor.log_debug(f"Using warehouse: {new_wh}")

            try:
                self._sql_executor.execute_query(f"use warehouse {new_wh}")
            except ProgrammingError as err:
                if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                    raise CouldNotUseObjectError(ObjectType.WAREHOUSE, new_wh) from err
                else:
                    raise UnknownSQLError(f"Failed to use warehouse {new_wh}") from err
            except:
                raise UnknownSQLError(f"Failed to use warehouse {new_wh}")

        try:
            yield

        finally:
            if is_different_wh and prev_wh is not None:
                self._sql_executor.log_debug(f"Switching back to warehouse:{prev_wh}")
                self._sql_executor.execute_query(f"use warehouse {prev_wh}")

    @contextmanager
    def _use_role_optional(self, new_role: str | None):
        """
        Switches to a different role for a while, then switches back.
        This is a no-op if the requested role is already active or if no role is passed in.
        """
        if new_role is None:
            yield

        prev_role = self._sql_executor.current_role()
        is_different_role = (
            new_role is not None and new_role.lower() != prev_role.lower()
        )
        if is_different_role:
            self._sql_executor.log_debug(f"Assuming different role: {new_role}")
            try:
                self._sql_executor.execute_query(f"use role {new_role}")
            except ProgrammingError as err:
                if err.errno == DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED:
                    raise CannotUseRoleError(new_role) from err
                else:
                    raise UnknownSQLError(f"Failed to use role {new_role}") from err
            except:
                raise UnknownSQLError(f"Failed to use role {new_role}")
        try:
            yield

        finally:
            if is_different_role:
                self._sql_executor.execute_query(f"use role {prev_role}")

    def _use_database_optional(self, database_name: str | None):
        """
        Switch to database `database_name`. No-op if no database is passed in.
        """
        if database_name is None:
            return
        self._sql_executor.log_debug(f"Using database {database_name}")
        try:
            self._sql_executor.execute_query(f"use database {database_name}")
        except ProgrammingError as err:
            # todo : what are the errors that can happen here
            pass

    def execute_user_script(
        self,
        queries: str,
        role: str | None = None,
        warehouse: str | None = None,
        database: str | None = None,
    ):
        with self._use_role_optional(role):
            with self._use_warehouse_optional(warehouse):
                self._use_database_optional(database)
                try:
                    self._sql_executor.execute_queries(queries)
                except ProgrammingError as err:
                    # TODO: Replace with granular error
                    generic_sql_error_handler(err)

                    # if err.errno == NO_WAREHOUSE_SELECTED_IN_SESSION:
                    #     raise NoWarehouseSelectedInSessionError(err.msg) from err
                    # # TODO: replace with error code! Find error code?
                    # elif "does not exist or not authorized" in err.msg:
                    #
                    # else:
                    #     # Can we include more information about the query here?
                    #     raise UnknownSQLError(f"Failed to execute user-provided queries") from err
