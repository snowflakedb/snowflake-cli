from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple, Union

from click import ClickException
from snowflake.cli.api.constants import OBJECT_TO_NAMES, ObjectNames
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import InterfaceError


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

    def drop(self, *, object_type, name: str) -> SnowflakeCursor:
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"drop {object_name} {name}")

    def describe(self, *, object_type: str, name: str):
        # Image repository is the only supported object that does not have a DESCRIBE command.
        if object_type == "image-repository":
            raise ClickException(
                f"Describe is currently not supported for object of type image-repository"
            )
        object_name = _get_object_names(object_type).sf_name
        return self._execute_query(f"describe {object_name} {name}")

    def object_exists(self, *, object_type: str, name: str):
        try:
            self.describe(object_type=object_type, name=name)
            return True
        except ProgrammingError:
            return False

    def _send_rest_request(
        self, url: str, method: str, data: Optional[Dict[str, Any]] = None
    ):
        # SnowflakeRestful.request assumes that API response is always a dict with "code" key.
        # This is not true in case of this API, so we need to do this workaround:
        from snowflake.connector.network import (
            CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT,
            HTTP_HEADER_CONTENT_TYPE,
            HTTP_HEADER_USER_AGENT,
            PYTHON_CONNECTOR_USER_AGENT,
        )

        self._log.debug(f"Sending {method} request to {url}")
        rest = self._conn.rest
        full_url = f"{rest.server_url}{url}"
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        return rest.fetch(
            method=method,
            full_url=full_url,
            headers=headers,
            token=rest.token,
            data=json.dumps(data if data else {}),
            no_retry=True,
        )

    def _url_exists(self, url):
        try:
            result = self._send_rest_request(url, method="get")
            return bool(result) or result == []
        except InterfaceError as err:
            if "404 Not Found" in str(err):
                return False
            raise err

    def _get_rest_api_create_url(self, object_type: str):
        plural_object_type = _get_object_names(object_type).sf_plural_name.replace(
            " ", "-"
        )
        url_prefix = "/api/v2"

        url = f"{url_prefix}/{plural_object_type}/"
        if self._url_exists(url):
            return url

        db = self._conn.database
        url = f"{url_prefix}/databases/{db}/{plural_object_type}/"
        if self._url_exists(url):
            return url

        schema = self._conn.schema
        url = f"{url_prefix}/databases/{db}/schemas/{schema}/{plural_object_type}/"
        if self._url_exists(url):
            return url

        return None

    def create(self, object_type: str, object_data: Dict[str, Any]) -> str:
        url = self._get_rest_api_create_url(object_type)
        if not url:
            return f"Create operation for type {object_type} is not supported. Try using `sql -q 'CREATE ...'` command"
        # TODO: nice exception handling (if possible) - "sth wrong" is not enough
        response = self._send_rest_request(url=url, method="post", data=object_data)
        if not response:
            # 409 conflict - ignored
            # {}, {"named": "text"} - 400 bad request - raised
            # 404 - not exists - ignored
            # 401 - not authorized - ignored
            return (
                "Something went wrong ¯\_(ツ)_/¯. Try again with --debug for more info."
            )
        return response["status"]
