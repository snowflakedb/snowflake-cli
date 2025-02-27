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
from snowflake.connector.vendored.requests.exceptions import HTTPError


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
        return self.execute_query(query, **kwargs)

    def drop(self, *, object_type: str, fqn: FQN) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_name
        return self.execute_query(f"drop {object_name} {fqn.sql_identifier}")

    def describe(self, *, object_type: str, fqn: FQN, **kwargs):
        # Image repository is the only supported object that does not have a DESCRIBE command.
        if object_type == "image-repository":
            raise ClickException(
                f"Describe is currently not supported for object of type image-repository"
            )
        object_name = _get_object_names(object_type).sf_name
        return self.execute_query(
            f"describe {object_name} {fqn.sql_identifier}", **kwargs
        )

    def object_exists(self, *, object_type: str, fqn: FQN):
        try:
            self.describe(object_type=object_type, fqn=fqn)
            return True
        except ProgrammingError:
            return False

    def create(
        self,
        object_type: str,
        object_data: Dict[str, Any],
        replace: bool = False,
        if_not_exists: bool = False,
    ) -> str:
        rest = RestApi(self._conn)
        url = rest.determine_url_for_create_query(
            object_type=object_type, replace=replace, if_not_exists=if_not_exists
        )
        try:
            response = rest.send_rest_request(url=url, method="post", data=object_data)
        except Exception as err:
            _handle_create_error_codes(err)
        return response["status"]


def _handle_create_error_codes(err: Exception) -> None:
    # according to https://docs.snowflake.com/developer-guide/snowflake-rest-api/reference/
    if isinstance(err, BadRequest):
        raise ClickException(
            "400 bad request: Incorrect object definition (arguments misspelled or malformatted)."
        )
    if isinstance(err, HTTPError):
        match err_code := err.response.status_code:
            case 401:
                raise ClickException(
                    "401 unauthorized: role you are using does not have permissions to create this object."
                )
            # error 403 should be handled by connector
            # error 404 is handled by determine-url logic
            # error 405 should not happen under assumption that "all listable objects can be created"
            case 408:
                raise ClickException(
                    "408 timeout: the request timed out and was not completed by the server."
                )
            case 409:
                raise ClickException(
                    "409 conflict: object you're trying to create already exists."
                )
            # error 410 is a network maintenance debugging - should not happen to the user
            case 429:
                raise ClickException(
                    "429 too many requests. The number of requests hit the rate limit."
                )
            case 500 | 503 | 504:
                raise ClickException(f"{err_code} internal server error.")
            case _:
                raise err
