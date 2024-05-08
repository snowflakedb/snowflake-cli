from __future__ import annotations

from typing import Optional, Tuple, Union

from click import ClickException
from snowflake.cli.api.constants import OBJECT_TO_NAMES, ObjectNames
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor


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

    def _send_rest_request(self, url: str, method: str, payload: Optional[str] = None):
        # TODO: change to DEBUG
        self._log.info(f"Sending {method} request to {url}")
        return self._conn.rest.request(
            method=method, url=url, body=payload, client="json"
        )

    def _get_rest_api_create_url(self, object_type: str):
        plural_object_type = _get_object_names(object_type).sf_plural_name.replace(
            " ", "-"
        )
        url_prefix = "/api/v2"
        try:
            url = f"{url_prefix}/{plural_object_type}/"
            self._send_rest_request(url, method="get")
            return url
        except:
            # TODO: nice wrap into _url_exists
            try:
                db = self._conn.database
                url = f"{url_prefix}/databases/{db}/{plural_object_type}/"
                self._send_rest_request(url, method="get")
                return url
            except:
                try:
                    schema = self._conn.schema
                    url = f"{url_prefix}/databases/{db}/schemas/{schema}/{plural_object_type}/"
                    self._send_rest_request(url, method="get")
                except Exception as e:
                    raise e

    def create(self, object_type: str, payload: str) -> str:
        url = self._get_rest_api_create_url(object_type)
        if not url:
            return f"Create operation for type {object_type} is not supported. Try using `sql -q 'CREATE ...'` command"
        response = self._send_rest_request(url=url, method="post", payload=payload)
        return response["status"]
