from typing import List, Optional

from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.spcs.common import (
    NoPropertiesProvidedError,
    handle_object_already_exists,
    strip_empty_lines,
)
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


class ComputePoolManager(SqlExecutionMixin):
    def create(
        self,
        pool_name: str,
        min_nodes: int,
        max_nodes: int,
        instance_family: str,
        auto_resume: bool,
        initially_suspended: bool,
        auto_suspend_secs: int,
        comment: Optional[str],
    ) -> SnowflakeCursor:
        query = f"""\
            create compute pool {pool_name}
            min_nodes = {min_nodes}
            max_nodes = {max_nodes}
            instance_family = {instance_family}
            auto_resume = {auto_resume}
            initially_suspended = {initially_suspended}
            auto_suspend_secs = {auto_suspend_secs}
            """.splitlines()
        if comment:
            query.append(f"comment = {comment}")

        try:
            return self._execute_query(strip_empty_lines(query))
        except ProgrammingError as e:
            handle_object_already_exists(e, ObjectType.COMPUTE_POOL, pool_name)

    def stop(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} stop all")

    def suspend(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} suspend")

    def resume(self, pool_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter compute pool {pool_name} resume")

    set_no_properties_message = "No properties specified for compute pool. Please provide at least one property to set."

    def set_property(
        self,
        pool_name: str,
        min_nodes: Optional[int],
        max_nodes: Optional[int],
        auto_resume: Optional[bool],
        auto_suspend_secs: Optional[int],
        comment: Optional[str],
    ) -> SnowflakeCursor:
        property_pairs = [
            ("min_nodes", min_nodes),
            ("max_nodes", max_nodes),
            ("auto_resume", auto_resume),
            ("auto_suspend_secs", auto_suspend_secs),
            ("comment", comment),
        ]

        # Check if all provided properties are set to None (no properties are being set)
        if all([value is None for property_name, value in property_pairs]):
            raise NoPropertiesProvidedError(
                self.set_no_properties_message
            )
        query: List[str] = [f"alter compute pool {pool_name} set"]
        for property_name, value in property_pairs:
            if value is not None:
                query.append(f"{property_name} = {value}")
        return self._execute_query(strip_empty_lines(query))

    unset_no_properties_message = "No properties specified for compute pool. Please provide at least one property to reset to its default value."

    def unset_property(
        self, pool_name: str, auto_resume: bool, auto_suspend_secs: bool, comment: bool
    ):
        property_pairs = [
            ("auto_resume", auto_resume),
            ("auto_suspend_secs", auto_suspend_secs),
            ("comment", comment),
        ]

        # Check if all properties provided are False (no properties are being unset)
        if not any([value for property_name, value in property_pairs]):
            raise NoPropertiesProvidedError(
                self.unset_no_properties_message
            )
        unset_list = [property_name for property_name, value in property_pairs if value]
        query = f"alter compute pool {pool_name} unset {','.join(unset_list)}"
        return self._execute_query(query)
