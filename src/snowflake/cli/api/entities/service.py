# mypy: disable-error-code=abstract
# above line skips mypy error for empty RESTService
import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import cached_property
from textwrap import dedent

from click import ClickException
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exception_handler import generic_sql_error_handler
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.connector.errors import ProgrammingError


class SFService(ABC):
    _cc = cli_console

    @cached_property
    def _log(self):
        return logging.getLogger(__name__)

    def _log_debug(self, message: str):
        self._log.debug(message)

    # Should the error handling be done here or in the implementing layer hmm
    @abstractmethod
    def use_object(self, object_type: ObjectType, name: str):
        pass

    @abstractmethod
    def get_current_role(self):
        pass

    @abstractmethod
    def get_current_warehouse(self):
        pass

    @abstractmethod
    def switch_to_role(self, role: str):
        pass

    # @abstractmethod
    # def get_existing_pkg(self, pkg_name: str):
    #     pass

    @abstractmethod
    def switch_to_warehouse(self, warehouse: str):
        pass

    @abstractmethod
    def switch_to_package_warehouse(self, package_warehouse: str):
        pass

    # @abstractmethod
    # def create_schema(self, package_role:str, package_name: str, schema_name: str):
    #     pass

    @abstractmethod
    def create_stage(self, package_role: str, stage_schema: str, stage_fqn: str):
        pass

    @abstractmethod
    def execute_package_script_queries(
        self, queued_queries, package_scripts, package_warehouse, package_role
    ):
        pass


class SqlService(SFService):

    _sql_executor = SqlExecutor()

    def use_object(self, object_type: ObjectType, name: str):
        try:
            self._sql_executor.execute_query(f"use {object_type.value.sf_name} {name}")
        # TODO: replace with CouldNotUseObject error
        except ProgrammingError:
            raise ProgrammingError(
                f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
            )

    def get_current_role(self) -> str:
        return self._sql_executor.execute_query(f"select current_role()").fetchone()[0]

    def get_current_warehouse(self) -> str:
        wh_result = self._sql_executor.execute_query(
            f"select current_warehouse()"
        ).fetchone()
        # If user has an assigned default warehouse, prev_wh will contain a value even if the warehouse is suspended.
        try:
            curr_wh = wh_result[0]
        except:
            curr_wh = None
        return curr_wh

    @contextmanager
    def switch_to_role(self, role: str):
        prev_role = self.get_current_role()
        is_different_role = role.lower() != prev_role.lower()
        if is_different_role:
            self._log_debug(f"Assuming different role: {role}")
            self.use_object(ObjectType.ROLE, role)
        try:
            yield
        finally:
            if is_different_role:
                self.use_object(ObjectType.ROLE, prev_role)

    @contextmanager
    def switch_to_warehouse(self, warehouse: str):
        prev_wh = self.get_current_warehouse()

        # new_wh is not None, and should already be a valid identifier, no additional check is performed here.
        is_different_wh = warehouse != prev_wh
        try:
            if is_different_wh:
                self._log_debug(f"Using warehouse: {warehouse}")
                self.use_object(object_type=ObjectType.WAREHOUSE, name=warehouse)
            yield
        finally:
            if prev_wh and is_different_wh:
                self._log_debug(f"Switching back to warehouse: {prev_wh}")
                self.use_object(object_type=ObjectType.WAREHOUSE, name=prev_wh)

    @contextmanager
    def switch_to_package_warehouse(self, package_warehouse: str):
        if package_warehouse:
            with self.switch_to_warehouse(package_warehouse):
                yield
        else:
            raise ClickException(
                dedent(
                    f"""\
                Application package warehouse cannot be empty.
                Please provide a value for it in your connection information or your project definition file.
                """
                )
            )

    # TODO: How do we tell our mistakes in expanding templates apart from the user's bad
    # values? can we?
    def execute_package_script_queries(
        self, queued_queries, package_scripts, package_warehouse, package_role
    ):
        with self.switch_to_role(package_role):
            with self.switch_to_package_warehouse(package_warehouse):
                try:
                    for i, queries in enumerate(queued_queries):
                        self._cc.step(f"Applying package script: {package_scripts[i]}")
                        self._sql_executor.execute_queries(queries)
                # TODO: proper error handling here
                except ProgrammingError as err:
                    generic_sql_error_handler(
                        err, role=package_role, warehouse=package_warehouse
                    )

    def create_stage(self, package_role: str, schema_name: str, stage_fqn: str):
        with self.switch_to_role(package_role):
            self._sql_executor.execute_query(
                f"create schema if not exists {schema_name}"
            )
            self._sql_executor.execute_query(
                f"""
                    create stage if not exists {stage_fqn}
                    encryption = (TYPE = 'SNOWFLAKE_SSE')
                    DIRECTORY = (ENABLE = TRUE)"""
            )


class RESTService(SFService):
    pass


def get_service() -> SFService:
    is_sql_service = True
    return SqlService() if is_sql_service else RESTService()
