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

import contextlib
import logging
import os
from typing import Dict, Optional

import snowflake.connector
from click.exceptions import ClickException
from snowflake.cli._app.constants import (
    PARAM_APPLICATION_NAME,
)
from snowflake.cli._app.telemetry import command_info
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import (
    get_connection_dict,
    get_default_connection_dict,
    get_default_connection_name,
)
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import (
    InvalidConnectionConfiguration,
    SnowflakeConnectionError,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError, ForbiddenError

log = logging.getLogger(__name__)


ENCRYPTED_PKCS8_PK_HEADER = b"-----BEGIN ENCRYPTED PRIVATE KEY-----"
UNENCRYPTED_PKCS8_PK_HEADER = b"-----BEGIN PRIVATE KEY-----"


def connect_to_snowflake(
    temporary_connection: bool = False,
    mfa_passcode: Optional[str] = None,
    enable_diag: Optional[bool] = False,
    diag_log_path: Optional[str] = None,
    diag_allowlist_path: Optional[str] = None,
    connection_name: Optional[str] = None,
    **overrides,
) -> SnowflakeConnection:
    if temporary_connection and connection_name:
        raise ClickException("Can't use connection name and temporary connection.")

    using_session_token = (
        "session_token" in overrides and overrides["session_token"] is not None
    )
    using_master_token = (
        "master_token" in overrides and overrides["master_token"] is not None
    )
    _raise_errors_related_to_session_token(
        temporary_connection, using_session_token, using_master_token
    )

    if connection_name:
        connection_parameters = get_connection_dict(connection_name)
        connection_parameters = get_connection_dict(connection_name)
    elif temporary_connection:
        connection_parameters = {}  # we will apply overrides in next step
    else:
        connection_parameters = get_default_connection_dict()
        get_cli_context().connection_context.set_connection_name(
            get_default_connection_name()
        )

    # Apply overrides to connection details
    for key, value in overrides.items():
        # Command line override case
        if value:
            connection_parameters[key] = value
            continue

        # Generic environment variable case, apply only if value not passed via flag or connection variable
        generic_env_value = os.environ.get(f"SNOWFLAKE_{key}".upper())
        if key not in connection_parameters and generic_env_value:
            connection_parameters[key] = generic_env_value
            continue

    # Clean up connection params
    connection_parameters = {
        k: v for k, v in connection_parameters.items() if v is not None
    }

    connection_parameters = update_connection_details_with_private_key(
        connection_parameters
    )

    if mfa_passcode:
        connection_parameters["passcode"] = mfa_passcode

    if connection_parameters.get("authenticator") == "username_password_mfa":
        connection_parameters["client_request_mfa_token"] = True

    if enable_diag:
        connection_parameters["enable_connection_diag"] = enable_diag
        if diag_log_path:
            connection_parameters["connection_diag_log_path"] = diag_log_path
        if diag_allowlist_path:
            connection_parameters[
                "connection_diag_allowlist_path"
            ] = diag_allowlist_path

    # Make sure the connection is not closed if it was shared to the SnowCLI, instead of being created in the SnowCLI
    _avoid_closing_the_connection_if_it_was_shared(
        using_session_token, using_master_token, connection_parameters
    )

    _update_connection_application_name(connection_parameters)

    try:
        # Whatever output is generated when creating connection,
        # we don't want it in our output. This is particularly important
        # for cases when external browser and json format are used.
        # Redirecting both stdout and stderr for offline usage.
        with contextlib.redirect_stdout(None), contextlib.redirect_stderr(None):
            return snowflake.connector.connect(
                application=command_info(),
                **connection_parameters,
            )
    except ForbiddenError as err:
        raise SnowflakeConnectionError(err)
    except DatabaseError as err:
        raise InvalidConnectionConfiguration(err.msg)


def _avoid_closing_the_connection_if_it_was_shared(
    using_session_token: bool, using_master_token: bool, connection_parameters: Dict
):
    if using_session_token and using_master_token:
        connection_parameters["server_session_keep_alive"] = True


def _raise_errors_related_to_session_token(
    temporary_connection: bool, using_session_token: bool, using_master_token: bool
):
    if not temporary_connection and (using_session_token or using_master_token):
        raise ClickException(
            "When using a session or master token, you must use a temporary connection"
        )
    if using_session_token and not using_master_token:
        raise ClickException(
            "When using a session token, you must provide the corresponding master token"
        )
    if using_master_token and not using_session_token:
        raise ClickException(
            "When using a master token, you must provide the corresponding session token"
        )


def update_connection_details_with_private_key(connection_parameters: Dict):
    if "private_key_file" in connection_parameters:
        _load_private_key(connection_parameters, "private_key_file")
    elif "private_key_path" in connection_parameters:
        _load_private_key(connection_parameters, "private_key_path")
    return connection_parameters


def _load_private_key(connection_parameters: Dict, private_key_var_name: str) -> None:
    if connection_parameters.get("authenticator") == "SNOWFLAKE_JWT":
        private_key = _load_pem_to_der(connection_parameters[private_key_var_name])
        connection_parameters["private_key"] = private_key
        del connection_parameters[private_key_var_name]
    else:
        raise ClickException(
            "Private Key authentication requires authenticator set to SNOWFLAKE_JWT"
        )


def _update_connection_application_name(connection_parameters: Dict):
    """Update version and name of app handling connection."""
    connection_application_params = {
        "application_name": PARAM_APPLICATION_NAME,
    }
    connection_parameters.update(connection_application_params)


def _load_pem_to_der(private_key_file: str) -> bytes:
    """
    Given a private key file path (in PEM format), decode key data into DER
    format
    """

    with SecurePath(private_key_file).open(
        "rb", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
    ) as f:
        private_key_pem = f.read()

    private_key_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE", None)
    if (
        private_key_pem.startswith(ENCRYPTED_PKCS8_PK_HEADER)
        and private_key_passphrase is None
    ):
        raise ClickException(
            "Encrypted private key, you must provide the"
            "passphrase in the environment variable PRIVATE_KEY_PASSPHRASE"
        )

    if not private_key_pem.startswith(
        ENCRYPTED_PKCS8_PK_HEADER
    ) and not private_key_pem.startswith(UNENCRYPTED_PKCS8_PK_HEADER):
        raise ClickException(
            "Private key provided is not in PKCS#8 format. Please use correct format."
        )

    if private_key_pem.startswith(UNENCRYPTED_PKCS8_PK_HEADER):
        private_key_passphrase = None

    return prepare_private_key(private_key_pem, private_key_passphrase)


def prepare_private_key(private_key_pem, private_key_passphrase=None):
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        load_pem_private_key,
    )

    private_key = load_pem_private_key(
        private_key_pem,
        (
            str.encode(private_key_passphrase)
            if private_key_passphrase is not None
            else private_key_passphrase
        ),
        default_backend(),
    )
    return private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
