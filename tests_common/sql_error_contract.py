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

import re
from typing import Optional

# SQLSTATE for "cannot drop a missing DCM deployment" (paired with
# snowflake.cli.api.errno.DCM_DEPLOYMENT_DOES_NOT_EXIST).
DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE = "22023"


def snowflake_sql_error_token(errno: int, sqlstate: str) -> str:
    """Return the connector token for a Snowflake SQL error (``001569 (22023)``)."""
    return f"{errno:06d} ({sqlstate})"


def _normalize_sql_error_haystack(output: str) -> str:
    """Collapse runs of spaces so a padded token still matches."""
    return re.sub(r"\s+", " ", output).strip()


def output_contains_snowflake_sql_error(
    output: Optional[str],
    *,
    errno: int,
    sqlstate: str,
) -> bool:
    if not output:
        return False
    token = snowflake_sql_error_token(errno, sqlstate)
    return token in _normalize_sql_error_haystack(output)
