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
        if_not_exists: bool,
    ) -> SnowflakeCursor:
        create_statement = "CREATE COMPUTE POOL"
        if if_not_exists:
            create_statement = f"{create_statement} IF NOT EXISTS"
        query = f"""\
            {create_statement} {pool_name}
            MIN_NODES = {min_nodes}
            MAX_NODES = {max_nodes}
            INSTANCE_FAMILY = {instance_family}
            AUTO_RESUME = {auto_resume}
            INITIALLY_SUSPENDED = {initially_suspended}
            AUTO_SUSPEND_SECS = {auto_suspend_secs}
            """.splitlines()
        if comment:
            query.append(f"COMMENT = {comment}")

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
                f"No properties specified for compute pool '{pool_name}'. Please provide at least one property to set."
            )
        query: List[str] = [f"alter compute pool {pool_name} set"]
        for property_name, value in property_pairs:
            if value is not None:
                query.append(f"{property_name} = {value}")
        return self._execute_query(strip_empty_lines(query))

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
                f"No properties specified for compute pool '{pool_name}'. Please provide at least one property to reset to its default value."
            )
        unset_list = [property_name for property_name, value in property_pairs if value]
        query = f"alter compute pool {pool_name} unset {','.join(unset_list)}"
        return self._execute_query(query)

    def status(self, pool_name: str):
        return self._execute_query(
            f"call system$get_compute_pool_status('{pool_name}')"
        )
