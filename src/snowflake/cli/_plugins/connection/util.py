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
import os
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, Optional

from click.exceptions import ClickException
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)


ALLOWLIST_QUERY = "SELECT SYSTEM$ALLOWLIST()"
SNOWFLAKE_DEPLOYMENT = "SNOWFLAKE_DEPLOYMENT"
LOCAL_DEPLOYMENT_REGION: str = "us-west-2"


class MissingConnectionAccountError(ClickException):
    def __init__(self, conn: SnowflakeConnection):
        super().__init__(
            "Could not determine account by system call, configured account name, or configured host. Connection: "
            + repr(conn)
        )


class MissingConnectionRegionError(ClickException):
    def __init__(self, host: str | None):
        super().__init__(
            f"The connection host ({host}) was missing or not in "
            "the expected format "
            "(<account>.<deployment>.snowflakecomputing.com)"
        )


class UIParameter(Enum):
    NA_ENABLE_REGIONLESS_REDIRECT = "UI_SNOWSIGHT_ENABLE_REGIONLESS_REDIRECT"
    NA_EVENT_SHARING_V2 = "ENABLE_EVENT_SHARING_V2_IN_THE_SAME_ACCOUNT"
    NA_ENFORCE_MANDATORY_FILTERS = (
        "ENFORCE_MANDATORY_FILTERS_FOR_SAME_ACCOUNT_INSTALLATION"
    )
    NA_FEATURE_RELEASE_CHANNELS = "FEATURE_RELEASE_CHANNELS"


def get_ui_parameter(
    conn: SnowflakeConnection, parameter: UIParameter, default: Any
) -> Any:
    """
    Returns the value of a single UI parameter.
    If the parameter is not found, the default value is returned.
    """

    ui_parameters = get_ui_parameters(conn)
    return ui_parameters.get(parameter, default)


@lru_cache()
def get_ui_parameters(conn: SnowflakeConnection) -> Dict[UIParameter, Any]:
    """
    Returns the UI parameters from the SYSTEM$BOOTSTRAP_DATA_REQUEST function
    """

    parameters_to_fetch = [param.value for param in UIParameter]

    # Parsing of the Json and the filtering is happening here in Snowflake CLI
    # in order to avoid requiring a warehouse in Snowflake
    query = "call system$bootstrap_data_request('CLIENT_PARAMS_INFO')"
    *_, cursor = conn.execute_string(query)

    json_map = json.loads(cursor.fetchone()[0])

    return {
        UIParameter(row["name"]): row["value"]
        for row in json_map["clientParamsInfo"]
        if row["name"] in parameters_to_fetch
    }


def clear_ui_parameters_cache() -> None:
    """
    Clear the LRU cache for UI parameters to ensure fresh state.

    This function should be called between tests or operations to prevent
    state pollution from cached UI parameters. It clears the cache for
    get_ui_parameters function.
    """
    get_ui_parameters.cache_clear()


def is_regionless_redirect(conn: SnowflakeConnection) -> bool:
    """
    Determines if the deployment this connection refers to uses
    regionless URLs in Snowsight (/orgname/account) or regional URLs
    (/region/account). If we cannot determine the correct value we
    assume it's regionless, as this is true for most production deployments.
    """
    try:
        return get_ui_parameter(conn, UIParameter.NA_ENABLE_REGIONLESS_REDIRECT, True)
    except:
        log.warning(
            "Cannot determine regionless redirect; assuming True.", exc_info=True
        )
        return True


def get_host_region(host: str) -> str | None:
    """
    Looks for hosts of form
    <account>.[x.y.z].snowflakecomputing.com
    Returns the three-part [region identifier] or None.
    """
    host_parts = host.split(".")
    if host_parts[-1] == "local":
        return LOCAL_DEPLOYMENT_REGION
    elif len(host_parts) == 6:
        return ".".join(host_parts[1:4])
    return None


def guess_regioned_host_from_allowlist(conn: SnowflakeConnection) -> str | None:
    """
    Use SYSTEM$ALLOWLIST to find a regioned host (<account>.x.y.z.snowflakecomputing.com)
    that corresponds to the given Snowflake connection object.
    """
    try:
        *_, cursor = conn.execute_string(ALLOWLIST_QUERY, cursor_class=DictCursor)
        allowlist_tuples = json.loads(cursor.fetchone()["SYSTEM$ALLOWLIST()"])
        for t in allowlist_tuples:
            if t["type"] == SNOWFLAKE_DEPLOYMENT:
                if get_host_region(t["host"]) is not None:
                    return t["host"]
    except:
        log.warning(
            "Could not call SYSTEM$ALLOWLIST; returning an empty guess.", exc_info=True
        )
    return None


def get_region(conn: SnowflakeConnection) -> str:
    """
    Get the region of the given connection, or raise MissingConnectionRegionError.
    """
    if conn.host:
        if region := get_host_region(conn.host):
            return region

    if host := guess_regioned_host_from_allowlist(conn):
        if region := get_host_region(host):
            return region

    raise MissingConnectionRegionError(host or conn.host)


def get_context(conn: SnowflakeConnection) -> str:
    """
    Determines the first part of the path in a Snowsight URL.
    This could be a region or it could be an organization, depending
    on whether or not the underlying deployment uses regionless URLs.
    """
    if is_regionless_redirect(conn):
        *_, cursor = conn.execute_string(
            f"select system$return_current_org_name()", cursor_class=DictCursor
        )
        return cursor.fetchone()["SYSTEM$RETURN_CURRENT_ORG_NAME()"]

    return get_region(conn)


def get_account(conn: SnowflakeConnection) -> str:
    """
    Determines the account that this connection refers to.
    """
    try:
        *_, cursor = conn.execute_string(
            f"select current_account_name()", cursor_class=DictCursor
        )
        return cursor.fetchone()["CURRENT_ACCOUNT_NAME()"].lower()
    except Exception as e:
        # try to extract the account from the connection information
        if conn.account:
            return conn.account

        if conn.host:
            host_parts = conn.host.split(".")
            return host_parts[0]

        raise MissingConnectionAccountError(conn)


def get_snowsight_host(conn: SnowflakeConnection) -> str:
    try:
        *_, cursor = conn.execute_string(
            f"select system$get_snowsight_host()", cursor_class=DictCursor
        )
        return cursor.fetchone()["SYSTEM$GET_SNOWSIGHT_HOST()"]
    except Exception as e:
        # if we cannot determine the host, assume we're on prod
        return "https://app.snowflake.com"


def make_snowsight_url(conn: SnowflakeConnection, path: str) -> str:
    """
    Returns a URL on the correct Snowsight instance for the connected account.
    The path that is passed in must already be properly URL-encoded, and
    can optionally contain a hash/fragment (e.g. #).

    See also identifier_for_url.
    """
    snowsight_host = get_snowsight_host(conn)
    deployment = get_context(conn)
    account = get_account(conn)
    path_with_slash = path if path.startswith("/") else f"/{path}"
    return f"{snowsight_host}/{deployment}/{account}{path_with_slash}"


def strip_if_value_present(value: Optional[str]) -> Optional[str]:
    return value.strip() if value else value


def ensure_that_path_exist(path: Optional[str]) -> Optional[str]:
    if path and not os.path.exists(path):
        raise ClickException(f"Path {path} does not exist.")
    return path


def strip_and_check_if_exists(value: Optional[str]) -> Optional[str]:
    return ensure_that_path_exist(strip_if_value_present(value))
