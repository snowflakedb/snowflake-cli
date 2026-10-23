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

"""The account login signs in to, and the host it stores when one was given."""

from __future__ import annotations

from urllib.parse import urlparse

from snowflake.cli.api.exceptions import CliArgumentError

_ACCOUNT_REQUIRED = "Enter an account to sign in."


class SignInTarget:
    """Account identifier plus the host to pass to ``connect()``, if any.

    A bare account leaves ``host`` empty so the connector uses the public
    account host. A host flag or an account URL is stored and passed through.
    """

    def __init__(self, account: str, host: str | None):
        self.account = account
        self.host = host

    @classmethod
    def from_login(cls, account: str | None, host: str | None) -> SignInTarget:
        text = _account_name(account or "")
        explicit_host = _hostname(host) if host and host.strip() else None
        stored_host: str | None
        if _is_account_host(text):
            account_host = _hostname(text)
            identifier = _account_from_host(account_host)
            stored_host = explicit_host or account_host
        else:
            identifier = text
            stored_host = explicit_host
        return cls(_account_name(identifier), stored_host)


def _account_name(text: str) -> str:
    name = text.strip()
    if not name:
        raise CliArgumentError(_ACCOUNT_REQUIRED)
    return name


def _is_account_host(account: str) -> bool:
    """An account URL or account host, as opposed to a bare account."""
    return "://" in account or "snowflakecomputing" in account.lower()


def _account_from_host(account_host: str) -> str:
    """Account name from an account host.

    Connector 4 exposed this as ``parse_account``. Connector 5 removed it.
    The first label is the account. A ``*.global`` host drops the external id
    after the last hyphen.
    """
    parts = account_host.split(".")
    if len(parts) <= 1:
        return account_host
    if parts[1] == "global":
        dash = parts[0].rfind("-")
        if dash == -1:
            return parts[0]
        return parts[0][:dash]
    return parts[0]


def _hostname(value: str) -> str:
    text = value.strip()
    if "://" not in text:
        return text.split("/")[0]
    host = urlparse(text).hostname
    if not host:
        raise CliArgumentError(
            f"Could not read an account host from {value}. "
            "Pass an account host such as "
            "myorg-myacct.privatelink.snowflakecomputing.com."
        )
    return host
