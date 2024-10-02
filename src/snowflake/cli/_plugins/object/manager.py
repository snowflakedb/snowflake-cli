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

from textwrap import dedent
from typing import Any, Dict, Optional, Tuple, Union

from click import ClickException
from snowflake.cli.api.constants import (
    OBJECT_TO_NAMES,
    ObjectNames,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.rest_api import RestApi
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import BadRequest


def _get_object_names(object_type: str) -> ObjectNames:
    object_type = object_type.lower()
    if object_type.lower() not in OBJECT_TO_NAMES:
        raise ClickException(f"Object of type {object_type} is not supported.")
    return OBJECT_TO_NAMES[object_type]


class ObjectManager(SqlExecutionMixin):
    def show(
        self,
        *,
        object_type: str,
        like: Optional[str] = None,
        scope: Union[Tuple[str, str], Tuple[None, None]] = (None, None),
        **kwargs,
    ) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_plural_name
        query = f"show {object_name}"
        if like:
            query += f" like '{like}'"
        if scope[0] is not None:
            query += f" in {scope[0].replace('-', ' ')} {scope[1]}"
        return self._execute_query(query, **kwargs)

    def drop(self, *, object_type: str, fqn: FQN) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"drop {object_name} {fqn.sql_identifier}")

    def describe(self, *, object_type: str, fqn: FQN):
        # Image repository is the only supported object that does not have a DESCRIBE command.
        if object_type == "image-repository":
            raise ClickException(
                f"Describe is currently not supported for object of type image-repository"
            )
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"describe {object_name} {fqn.sql_identifier}")

    def object_exists(self, *, object_type: str, fqn: FQN):
        try:
            self.describe(object_type=object_type, fqn=fqn)
            return True
        except ProgrammingError:
            return False

    def create(self, object_type: str, object_data: Dict[str, Any]) -> str:
        rest = RestApi(self._conn)
        url = rest.determine_url_for_create_query(object_type=object_type)

        try:
            response = rest.send_rest_request(url=url, method="post", data=object_data)
            # workaround as SnowflakeRestful class ignores some errors, dropping their info and returns {} instead.
            if not response:
                raise ClickException(
                    dedent(
                        """                An unexpected error occurred while creating the object. Try again with --debug for more info.
                Most probable reasons:
                  * object you are trying to create already exists
                  * role you are using do not have permissions to create this object"""
                    )
                )
            return response["status"]
        except BadRequest:
            raise ClickException(
                "Incorrect object definition (arguments misspelled or malformatted)."
            )
