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
from typing import Any, Dict, List, Optional

from snowflake.cli.api.constants import SF_REST_API_URL_PREFIX
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import InterfaceError
from snowflake.connector.network import SnowflakeRestful

log = logging.getLogger(__name__)


class RestApi:
    def __init__(self, connection: SnowflakeConnection):
        self.conn = connection
        self.rest: SnowflakeRestful = connection.rest

    def get_endpoint_exists(self, url: str) -> bool:
        """
        Check whether [get] endpoint exists under given URL.
        """
        try:
            result = self.send_rest_request(url, method="get")
            return bool(result) or result == []
        except InterfaceError as err:
            if "404 Not Found" in str(err):
                return False
            raise err

    def send_rest_request(
        self, url: str, method: str, data: Optional[Dict[str, Any]] = None
    ):
        """
        Executes rest request via snowflake.connector.network.SnowflakeRestful
        """
        # SnowflakeRestful.request assumes that API response is always a dict,
        # which is not true in case of this API, so we need to do this workaround:
        from snowflake.connector.network import (
            CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT,
            HTTP_HEADER_CONTENT_TYPE,
            HTTP_HEADER_USER_AGENT,
            PYTHON_CONNECTOR_USER_AGENT,
        )

        log.debug("Sending %s request to %s", method, url)
        full_url = f"{self.rest.server_url}{url}"
        headers = {
            HTTP_HEADER_CONTENT_TYPE: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_ACCEPT: CONTENT_TYPE_APPLICATION_JSON,
            HTTP_HEADER_USER_AGENT: PYTHON_CONNECTOR_USER_AGENT,
        }
        return self.rest.fetch(
            method=method,
            full_url=full_url,
            headers=headers,
            token=self.rest.token,
            data=json.dumps(data if data else {}),
            no_retry=True,
        )

    def determine_url_for_create_query(
        self, *, plural_object_type: str
    ) -> Optional[str]:
        """
        Determine an url for creating an object of given type via REST API.
        The function returns None if URL cannot be determined.

        URLs we check:
         * /api/v2/<type>/
         * /api/v2/databases/<database>/<type>/
         * /api/v2/databases/<database>/schemas/<schema>/<type>/

         We assume that the URLs for CREATE and LIST are the same for every type of object
         (endpoints differ by method: POST vs GET, accordingly).
         To check whether an URL exists, we send read-only GET request (LIST endpoint,
         which should imply CREATE endpoint).
        """
        urls_to_be_checked: List[Optional[str]] = [
            f"{SF_REST_API_URL_PREFIX}/{plural_object_type}/",
            (
                f"{SF_REST_API_URL_PREFIX}/databases/{self.conn.database}/{plural_object_type}/"
                if self.conn.database
                else None
            ),
            (
                f"{SF_REST_API_URL_PREFIX}/databases/{self.conn.database}/schemas/{self.conn.schema}/{plural_object_type}/"
                if self.conn.database and self.conn.schema
                else None
            ),
        ]

        for url in urls_to_be_checked:
            if url and self.get_endpoint_exists(url):
                return url

        return None
