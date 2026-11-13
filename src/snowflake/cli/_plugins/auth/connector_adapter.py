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

"""Connector calls that differ between connector 4 and the Universal Driver.

Login goes through this module for every connector API that is not the same
on both. A move to connector 5 deletes the connector 4 branch in each function.
"""

from __future__ import annotations

from snowflake.cli.api.connector_errors import IS_V5_DRIVER


def session_user_cache_key(name: str) -> str:
    """Spelling this connector stores for the session user.

    Connector 4 uppercases the user. Connector 5 keeps a name that contains
    a double quote as written and lowercases every other name.
    """
    if IS_V5_DRIVER:
        if '"' in name:
            return name
        return name.lower()
    return name.upper()
