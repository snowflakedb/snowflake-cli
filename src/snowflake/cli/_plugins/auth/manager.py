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

from snowflake.cli.api.config import (
    ConnectionConfig,
    get_all_connections,
    get_connections_file,
    get_default_connection_name,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import CollectionResult
from snowflake.cli.api.sanitizers import sanitize_for_terminal

# Snowflake OAuth for local applications uses the built-in client. A connection
# that sets its own client id or secret is a different OAuth client.
AUTHENTICATOR = "OAUTH_AUTHORIZATION_CODE"


class AuthManager:
    """Saved connections that use Snowflake OAuth for local applications."""

    def list_connections(self) -> CollectionResult:
        """Saved connections, including ones with no session."""
        connections_file = get_connections_file()
        if connections_file.exists():
            path = sanitize_for_terminal(str(connections_file))
            cli_console.warning(
                f"Reading connections from {path}. Entries from config.toml are ignored."
            )
        default_name = get_default_connection_name()
        rows = (
            {
                "connection_name": name,
                "account": config.account or "",
                "user": config.user or "",
                "host": config.host or "",
                "database": config.database or "",
                "schema": config.schema or "",
                "warehouse": config.warehouse or "",
                "role": config.role or "",
                "is_default": name == default_name,
            }
            for name, config in get_all_connections().items()
            if _is_local_oauth(config)
        )
        return CollectionResult(rows)


def _is_local_oauth(config: ConnectionConfig) -> bool:
    """Snowflake OAuth for local applications, which uses the built-in client."""
    if (config.authenticator or "").upper() != AUTHENTICATOR:
        return False
    return not config.oauth_client_id and not config.oauth_client_secret
