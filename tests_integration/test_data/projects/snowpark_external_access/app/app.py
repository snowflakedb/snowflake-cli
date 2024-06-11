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

import _snowflake
from http.client import HTTPSConnection
from snowflake.snowpark import Session


def check_secret_and_get_status_function():
    return _check_secret_and_get_status()


def check_secret_and_get_status_procedure(session: Session):
    return _check_secret_and_get_status()


def _check_secret_and_get_status():
    generic_secret = _snowflake.get_generic_secret_string("generic_secret")
    assert generic_secret

    host = "docs.snowflake.com"
    conn = HTTPSConnection(host)
    conn.request("GET", "/")
    response = conn.getresponse()
    return response.status
