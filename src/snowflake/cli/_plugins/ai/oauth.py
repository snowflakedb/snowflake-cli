"""Process-local credential helper for Snowflake-hosted authorization-code OAuth."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import re
import signal
import stat
import sys
from pathlib import Path
from types import MethodType

from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.project.util import (
    is_valid_identifier,
    is_valid_quoted_identifier,
    unquote_identifier,
)
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.secure_path import SecurePath

BINDING_ENV = "SNOWFLAKE_AI_OAUTH_BINDING"
HELPER_TIMEOUT = 40
SUPPORTED_CONNECTOR = "4.7.5"
CONNECTOR_VERSION_MESSAGE = (
    f"Renewable OAuth is validated against connector {SUPPORTED_CONNECTOR} only; "
    f"install {SUPPORTED_CONNECTOR} or use a PAT/raw OAuth connection."
)
REAUTH_MESSAGE = "OAuth renewal failed. Sign in again with snow connection test, then relaunch snow ai."
ERROR_MESSAGES = {
    1: REAUTH_MESSAGE,
    2: "OAuth helper configuration is unsupported or invalid. Check the snow ai setup guide and relaunch.",
    3: "OAuth credential storage or lock is unavailable. Check local ownership, permissions and secure storage, then relaunch.",
    4: "OAuth login identity changed. Check the selected user and role, then sign in again and relaunch.",
    5: "OAuth renewal timed out. Check network and proxy connectivity, then retry.",
    6: "OAuth helper failed unexpectedly. Check the supported CLI and connector versions, then relaunch.",
}


class OAuthHelperError(CliError):
    def __init__(self, category: int, message: str | None = None):
        super().__init__(message or ERROR_MESSAGES[category])
        self.category = category


def validate_role(role: str | None) -> str:
    if not isinstance(role, str) or not role.strip() or not is_valid_identifier(role):
        raise CliError("Renewable OAuth requires an explicit valid role identifier.")
    return role if is_valid_quoted_identifier(role) else role.upper()


def validate_binding(binding: dict) -> dict:
    required = {"account", "host", "user", "role"}
    if (
        not isinstance(binding, dict)
        or set(binding) != required
        or any(
            not isinstance(value, str) or not value.strip() or len(value) > 512
            for value in binding.values()
        )
    ):
        raise CliError("Invalid OAuth launch binding.")
    if not re.fullmatch(r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*", binding["host"]):
        raise CliError("Invalid OAuth account hostname.")
    if not binding["host"].endswith(
        (".snowflakecomputing.com", ".snowflakecomputing.cn")
    ):
        raise CliError("Renewable OAuth requires a Snowflake account hostname.")
    return {**binding, "role": validate_role(binding["role"])}


def binding_for(context, connection) -> dict:
    """Snapshot the authenticated identity, without persisting or exporting secrets."""
    import snowflake.connector

    if snowflake.connector.__version__ != SUPPORTED_CONNECTOR:
        raise CliError(CONNECTOR_VERSION_MESSAGE)
    if sys.platform == "win32":
        raise CliError("Renewable snow ai OAuth is not yet supported on Windows.")
    if context.temporary_connection or not context.connection_name:
        raise CliError("Renewable OAuth requires a named Snow CLI connection.")
    if (
        context.client_store_temporary_credential is not True
        or context.oauth_enable_refresh_tokens is not True
    ):
        raise CliError(
            "Renewable OAuth requires credential caching and oauth_enable_refresh_tokens = true."
        )
    if any(
        getattr(context, name, None)
        for name in (
            "oauth_client_id",
            "oauth_client_secret",
            "oauth_authorization_url",
            "oauth_token_request_url",
            "oauth_scope",
            "oauth_disable_pkce",
            "oauth_enable_single_use_refresh_tokens",
            "secondary_roles",
            "token",
            "token_file_path",
            "session_token",
            "master_token",
        )
    ):
        raise CliError(
            "Renewable OAuth currently supports only Snowflake-hosted default-client OAuth without custom scopes or secondary roles."
        )
    # Query/session preferences and unused credentials are not forwarded.
    transport_options = (
        "port",
        "protocol",
        "proxy_host",
        "proxy_port",
        "proxy_user",
        "proxy_password",
        "no_proxy",
        "insecure_mode",
        "disable_ocsp_checks",
        "ocsp_fail_open",
        "ocsp_response_cache_filename",
        "ocsp_root_certs_dict_lock_timeout",
        "cert_revocation_check_mode",
        "allow_certificates_without_crl_url",
        "crl_connection_timeout_ms",
        "crl_read_timeout_ms",
        "disable_request_pooling",
        "use_openssl_only",
        "network_timeout",
        "socket_timeout",
        "backoff_policy",
        "oauth_redirect_uri",
        "oauth_socket_uri",
    )
    unsupported = [
        name for name in transport_options if getattr(context, name, None) is not None
    ]
    if getattr(context, "enable_diag", False):
        unsupported.append("enable_diag")
    if unsupported:
        raise CliError(
            "Renewable OAuth does not support configured transport/client options: "
            f"{', '.join(unsupported)}. Use default HTTPS/443 transport or a PAT/raw "
            "OAuth connection; see the snow ai setup guide."
        )
    if not isinstance(getattr(context, "user", None), str) or not context.user.strip():
        raise CliError("Renewable OAuth requires an explicit user.")
    role = validate_role(getattr(context, "role", None))
    auth = getattr(connection, "auth_class", None)
    if str(getattr(auth, "_idp_host", "")).lower() != connection.host.lower():
        raise CliError("OAuth issuer must match the authenticated account host.")
    if (
        unquote_identifier(role) != connection.role
        or not isinstance(connection.user, str)
        or context.user.upper() != connection.user.upper()
    ):
        raise CliError("OAuth login identity differs from the selected connection.")
    return validate_binding(
        {
            "account": connection.account,
            "host": connection.host.lower(),
            "user": connection.user,
            "role": role,
        }
    )


def helper_argv() -> list[str]:
    if getattr(sys, "frozen", False):
        raise CliError(
            "Renewable OAuth is not yet supported by standalone Snow CLI executables."
        )
    return [
        str(Path(sys.executable).absolute()),
        "-I",
        "-m",
        "snowflake.cli._plugins.ai.oauth",
    ]


@contextlib.contextmanager
def renewal_lock(binding: dict):
    """Coordinate helper processes using the connector's issuer/user cache identity."""
    import fcntl

    directory = SecurePath(Path.home() / ".snowflake" / "ai-oauth-locks")
    directory.mkdir(parents=True, exist_ok=True)
    info = directory.path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise OAuthHelperError(
            3, "OAuth lock directory must be owned by this user with permissions 0700."
        )
    identity = f"{binding['host'].upper()}:{binding['user'].upper()}"
    name = hashlib.sha256(identity.encode()).hexdigest() + ".lock"
    descriptor = os.open(
        directory.path / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
    )
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise OAuthHelperError(3)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def obtain_token(binding: dict) -> SecretType:
    """Authenticate without browser fallback and capture the accepted access token.

    This deliberately uses a version-checked first-party auth instance instead of
    modifying connector classes or reading the cache after authentication.
    """
    if sys.platform == "win32":
        raise OAuthHelperError(2)

    import snowflake.connector
    from snowflake.connector.auth.oauth_code import AuthByOauthCode
    from snowflake.connector.token_cache import TokenCache

    binding = validate_binding(binding)
    if snowflake.connector.__version__ != SUPPORTED_CONNECTOR:
        raise CliError(CONNECTOR_VERSION_MESSAGE)
    host = binding["host"]
    try:
        token_cache = TokenCache.make()
    except Exception:
        raise OAuthHelperError(3) from None
    auth = AuthByOauthCode(
        application="snowflake-cli",
        client_id="",
        client_secret="",
        authentication_url=f"https://{host}/oauth/authorize",
        token_request_url=f"https://{host}/oauth/token-request",
        redirect_uri="http://127.0.0.1",
        scope=f"session:role:{binding['role']}",
        host=host,
        token_cache=token_cache,
        refresh_token_enabled=True,
        timeout=15,
    )
    accepted: list[SecretType] = []
    original_update = auth.update_body

    def refuse_browser(instance, **kwargs):
        raise OAuthHelperError(1)

    def capture(instance, body):
        original_update(body)
        accepted[:] = [SecretType(body["data"]["TOKEN"])]

    setattr(auth, "_request_tokens", MethodType(refuse_browser, auth))
    auth.update_body = MethodType(capture, auth)
    with renewal_lock(binding):
        with snowflake.connector.connect(
            **binding,
            authenticator="oauth_authorization_code",
            auth_class=auth,
            client_store_temporary_credential=True,
            oauth_enable_refresh_tokens=True,
            login_timeout=15,
            network_timeout=15,
            socket_timeout=15,
            session_parameters={"CLIENT_TELEMETRY_ENABLED": False},
        ) as connection:
            if (
                connection.host.lower() != host
                or connection.user.upper() != binding["user"].upper()
                or connection.role != unquote_identifier(binding["role"])
            ):
                raise OAuthHelperError(4)
            if (
                not accepted
                or not accepted[0]
                or not isinstance(accepted[0].value, str)
            ):
                raise OAuthHelperError(1)
            return accepted[0]


def main() -> int:
    def timed_out(signum, frame):
        os.write(2, (ERROR_MESSAGES[5] + "\n").encode())
        os._exit(5)

    previous_logging_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        if sys.platform == "win32":
            raise CliError("Windows is not yet supported.")
        signal.signal(signal.SIGALRM, timed_out)
        signal.alarm(HELPER_TIMEOUT)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
            io.StringIO()
        ):
            token = obtain_token(json.loads(os.environ[BINDING_ENV]))
        if (
            not re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", token.value)
            or len(token.value) > 32768
        ):
            raise CliError("Invalid access token format.")
        sys.stdout.write(token.value)
        return 0
    except Exception as error:
        from snowflake.connector.errors import Error as ConnectorError

        if isinstance(error, OAuthHelperError):
            category = error.category
        elif isinstance(error, TimeoutError):
            category = 5
        elif isinstance(error, OSError):
            category = 3
        elif isinstance(error, (CliError, ValueError, KeyError, TypeError)):
            category = 2
        elif isinstance(error, ConnectorError):
            category = 1
        else:
            category = 6
        sys.stderr.write(ERROR_MESSAGES[category] + "\n")
        return category
    finally:
        logging.disable(previous_logging_level)
        if sys.platform != "win32":
            signal.alarm(0)


if __name__ == "__main__":
    sys.exit(main())
