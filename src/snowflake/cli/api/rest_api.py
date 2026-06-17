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

import json
import logging
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlparse

from click import ClickException
from snowflake.cli.api.constants import SF_REST_API_URL_PREFIX
from snowflake.connector.connection import SnowflakeConnection

log = logging.getLogger(__name__)

# HTTP header names and content type used for Snowflake REST API requests.
# Defined natively so the CLI does not depend on connector internals
# (``snowflake.connector.network``), which differ between connector versions.
CONTENT_TYPE_APPLICATION_JSON = "application/json"
HTTP_HEADER_CONTENT_TYPE = "Content-Type"
HTTP_HEADER_ACCEPT = "accept"
HTTP_HEADER_USER_AGENT = "User-Agent"


def _pluralize_object_type(object_type: str) -> str:
    """
    Pluralize object type without depending on OBJECT_TO_NAMES.
    """
    if object_type.endswith("y"):
        return object_type[:-1].lower() + "ies"
    else:
        return object_type.lower() + "s"


class RestApi:
    def __init__(self, connection: SnowflakeConnection):
        self.conn = connection
        self.rest = connection.rest

    def get_endpoint_exists(self, url: str) -> bool:
        """
        Check whether [get] endpoint exists under given URL.
        """
        from snowflake.cli.api.connector_errors import (
            HTTP_FAILURE_ERRORS,
            get_http_status_code,
        )
        from snowflake.connector.errors import BadRequest

        try:
            self.send_rest_request(url, method="get")
            return True
        except BadRequest:
            return True
        except HTTP_FAILURE_ERRORS as err:
            code = get_http_status_code(err)
            if code == 404:
                return False
            # 400 means the endpoint exists but the probe query was rejected
            # (e.g. result set too large) -- treated as "exists", same as the
            # connector-v4 BadRequest branch above.
            if code == 400:
                return True
            raise

    def _fetch_endpoint_exists(self, url: str) -> bool:
        from snowflake.cli.api.connector_errors import (
            HTTP_FAILURE_ERRORS,
            get_http_status_code,
        )
        from snowflake.connector.errors import BadRequest

        try:
            result = self.send_rest_request(url, method="get")
            return bool(result)
        except BadRequest:
            return False
        except HTTP_FAILURE_ERRORS as err:
            # 404 (not found) and 400 (bad request, see get_endpoint_exists)
            # both map to the connector-v4 behaviour of returning False.
            if get_http_status_code(err) in (404, 400):
                return False
            raise

    def send_rest_request(
        self, url: str, method: str, data: Optional[Dict[str, Any]] = None
    ):
        """
        Executes rest request via snowflake.connector.network.SnowflakeRestful
        """
        # SnowflakeRestful.request assumes that API response is always a dict,
        # which is not true in case of this API, so we need to do this workaround:
        from snowflake.cli.api.connector_errors import get_user_agent

        log.debug("Sending %s request to %s", method, url)
        full_url = f"{self.rest.server_url}{url}"
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
        }
        if user_agent := get_user_agent(self.rest):
            headers[HTTP_HEADER_USER_AGENT] = user_agent
        return self.rest.fetch(
            method=method,
            full_url=full_url,
            headers=headers,
            token=self.rest.token,
            data=json.dumps(data) if data else None,
            no_retry=True,
            raise_raw_http_failure=True,
            external_session_id=None,  # workaround for connector 3.16 bug, to be removed SNOW-2226816
        )

    def _database_exists(self, db_name: str) -> bool:
        url = f"{SF_REST_API_URL_PREFIX}/databases/{db_name}"
        return self._fetch_endpoint_exists(url)

    def _schema_exists(self, db_name: str, schema_name: str) -> bool:
        url = f"{SF_REST_API_URL_PREFIX}/databases/{db_name}/schemas/{schema_name}"
        return self._fetch_endpoint_exists(url)

    def determine_url_for_create_query(
        self, object_type: str, replace: bool = False, if_not_exists: bool = False
    ) -> str:
        """
        Determine an url for creating an object of given type via REST API.
        If URL cannot be determined, the function throws CannotDetermineCreateURLException exception.

        URLs we check:
         * /api/v2/<type>/
         * /api/v2/databases/<database>/<type>/
         * /api/v2/databases/<database>/schemas/<schema>/<type>/

         We assume that the URLs for CREATE and LIST are the same for every type of object
         (endpoints differ by method: POST vs GET, accordingly).
         To check whether an URL exists, we send read-only GET request (LIST endpoint,
         which should imply CREATE endpoint).
        """
        plural_object_type = _pluralize_object_type(object_type)

        query_params = {}

        if replace or if_not_exists:
            param = "ifNotExists" if if_not_exists else "orReplace"
            query_params = {"createMode": param}

        if self.get_endpoint_exists(
            url := f"{SF_REST_API_URL_PREFIX}/{plural_object_type}/"
        ):
            return self._add_query_parameters_to_url(url, query_params)

        db = self.conn.database
        if not db:
            raise DatabaseNotDefinedException(
                "Database not defined in connection. Please try again with `--database` flag."
            )
        if not self._database_exists(db):
            raise DatabaseNotExistsException(f"Database '{db}' does not exist.")
        if self.get_endpoint_exists(
            url := f"{SF_REST_API_URL_PREFIX}/databases/{db}/{plural_object_type}/"
        ):
            return self._add_query_parameters_to_url(url, query_params)

        schema = self.conn.schema
        if not schema:
            raise SchemaNotDefinedException(
                "Schema not defined in connection. Please try again with `--schema` flag."
            )
        # temporarily skip schema existence check due to server-side issue (SNOW-2110515)
        # if not self._schema_exists(db_name=db, schema_name=schema):
        #     raise SchemaNotExistsException(f"Schema '{schema}' does not exist.")
        if self.get_endpoint_exists(
            url := f"{SF_REST_API_URL_PREFIX}/databases/{self.conn.database}/schemas/{self.conn.schema}/{plural_object_type}/"
        ):
            return self._add_query_parameters_to_url(url, query_params)

        raise CannotDetermineCreateURLException(
            f"Create operation for type {object_type} is not supported. Try using `sql -q 'CREATE ...'` command."
        )

    @staticmethod
    def _add_query_parameters_to_url(url: str, query_params: Dict[str, Any]) -> str:
        """
        Updates existing url with new query parameters.
        They should be passed as key-value pairs in query_params dict.
        """
        if not query_params:
            return url
        url_parts = urlparse(url)
        query = dict(parse_qsl(url_parts.query))
        query.update(query_params)
        return url_parts._replace(query=urlencode(query)).geturl()


class DatabaseNotDefinedException(ClickException):
    pass


class SchemaNotDefinedException(ClickException):
    pass


class DatabaseNotExistsException(ClickException):
    pass


class SchemaNotExistsException(ClickException):
    pass


class CannotDetermineCreateURLException(ClickException):
    pass
