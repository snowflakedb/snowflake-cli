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

"""Connectivity diagnostics for `snow connection test --enable-diag`.

Replicates the per-endpoint checks SnowCD used to provide. Endpoints come from
`SYSTEM$ALLOWLIST()` (or a JSON file via `--diag-allowlist-path`); each one is
probed with TCP connect, plus a TLS handshake on port 443 to capture the cert
issuer and expiry.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import ssl
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Optional

from snowflake.cli._plugins.connection.util import ALLOWLIST_QUERY
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import identifier_to_show_like_pattern
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS: float = 5.0
TLS_PORT: int = 443
ALLOWLIST_PRIVATELINK_QUERY = "SELECT SYSTEM$ALLOWLIST_PRIVATELINK()"
ALLOWLIST_FILE_SIZE_LIMIT_MB: float = 1.0
NETWORK_POLICY_PARAM_NAME = "NETWORK_POLICY"

Status = Literal["Healthy", "Unhealthy", "Skipped"]


@dataclass
class EndpointCheck:
    host: str
    port: int
    type: str  # noqa: A003 — public field name; matches `SYSTEM$ALLOWLIST()` JSON
    status: Status
    error: Optional[str] = None
    cert_issuer: Optional[str] = None
    cert_expires: Optional[str] = None
    latency_ms: Optional[float] = None


@dataclass
class DiagnosticReport:
    checks: list[EndpointCheck] = field(default_factory=list)

    def _count(self, status: Status) -> int:
        return sum(1 for c in self.checks if c.status == status)

    @property
    def healthy(self) -> int:
        return self._count("Healthy")

    @property
    def unhealthy(self) -> int:
        return self._count("Unhealthy")

    @property
    def skipped(self) -> int:
        return self._count("Skipped")

    @property
    def tested(self) -> int:
        return self.healthy + self.unhealthy

    def summary_line(self) -> str:
        return (
            f"Results: {self.healthy} Healthy, {self.unhealthy} Unhealthy "
            f"out of {self.tested} endpoints. "
            f"{self.skipped} skipped (non-resolvable patterns)."
        )


# --------------------------------------------------------------------------- #
# Allowlist loading
# --------------------------------------------------------------------------- #


def _query_json_list(conn: SnowflakeConnection, sql: str, key: str) -> list[dict]:
    """Run an allowlist-returning system function; return its JSON list, or [] on any failure.

    Failures are silent because either query may be unavailable: low-priv roles
    can't call `SYSTEM$ALLOWLIST()`, and `SYSTEM$ALLOWLIST_PRIVATELINK()` isn't
    enabled in every deployment. The caller decides what to do with an empty
    result (typically: fall back to the connection host).
    """
    try:
        *_, cursor = conn.execute_string(sql, cursor_class=DictCursor)
        row = cursor.fetchone()
        raw = row.get(key) if row else None
        parsed = json.loads(raw) if raw else []
    except Exception:
        log.debug("Allowlist query failed: %s", sql, exc_info=True)
        return []
    return parsed if isinstance(parsed, list) else []


def _dedupe_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicate `(type, host, port)` triples, preserving first-seen order."""
    seen: set[tuple[str, str, Any]] = set()
    out: list[dict[str, Any]] = []
    for e in entries:
        key = (str(e.get("type", "")), str(e.get("host", "")), e.get("port"))
        if key not in seen:
            seen.add(key)
            out.append(e)
    return out


def load_allowlist(
    conn: SnowflakeConnection, allowlist_path: Optional[Path]
) -> list[dict[str, Any]]:
    """Return the raw allowlist as a list of `{type, host, port}` dicts.

    If `allowlist_path` is given, parse it as JSON. Otherwise merge
    `SYSTEM$ALLOWLIST()` and `SYSTEM$ALLOWLIST_PRIVATELINK()` from the open
    connection, deduping `(type, host, port)`. Either query can fail silently
    (low-priv role, function unavailable); the caller treats an empty result
    as a signal to fall back to the connection host.
    """
    if allowlist_path is not None:
        try:
            payload = json.loads(
                SecurePath(allowlist_path).read_text(
                    file_size_limit_mb=ALLOWLIST_FILE_SIZE_LIMIT_MB
                )
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise CliError(
                f"Could not read allowlist file {allowlist_path}: {exc}. "
                "Check the path and that the file is valid JSON."
            )
        if not isinstance(payload, list):
            raise CliError(
                f"Allowlist file {allowlist_path} must contain a JSON array "
                "of {type, host, port} entries."
            )
        return payload

    public = _query_json_list(conn, ALLOWLIST_QUERY, "SYSTEM$ALLOWLIST()")
    privatelink = _query_json_list(
        conn, ALLOWLIST_PRIVATELINK_QUERY, "SYSTEM$ALLOWLIST_PRIVATELINK()"
    )
    return _dedupe_entries([*public, *privatelink])


# --------------------------------------------------------------------------- #
# Endpoint probing
# --------------------------------------------------------------------------- #


def is_resolvable(host: str) -> bool:
    """False for hostnames the OS resolver can never satisfy (wildcards, empty)."""
    return bool(host) and "*" not in host


def _resolve_cafile() -> Optional[str]:
    """Honour `REQUESTS_CA_BUNDLE` then `SSL_CERT_FILE`, matching the connector.

    Returns `None` if neither points at a readable file, in which case
    `ssl.create_default_context()` uses the system trust store.
    """
    for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        path = os.environ.get(var)
        if path and os.path.isfile(path):
            return path
    return None


def _issuer_org_name(cert: dict) -> Optional[str]:
    """Extract organizationName from a peer cert's issuer RDN sequence."""
    for rdn in cert.get("issuer") or ():
        for entry in rdn:
            if (
                isinstance(entry, tuple)
                and len(entry) == 2
                and entry[0] == "organizationName"
            ):
                return entry[1]
    return None


def _probe_tls(sock: socket.socket, host: str) -> tuple[Optional[str], Optional[str]]:
    """Wrap `sock` in TLS; return `(issuer_org, not_after)` from the peer cert."""
    context = ssl.create_default_context(cafile=_resolve_cafile())
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    with context.wrap_socket(sock, server_hostname=host) as ssock:
        cert: Any = ssock.getpeercert() or {}
    not_after = cert.get("notAfter")
    return _issuer_org_name(cert), not_after if isinstance(not_after, str) else None


def check_endpoint(
    host: str,
    port: int,
    type_: str,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> EndpointCheck:
    """Probe a single endpoint. Never raises; failures map to Unhealthy."""
    if not is_resolvable(host):
        return EndpointCheck(
            host, port, type_, "Skipped", error="non-resolvable pattern"
        )

    try:
        start = time.perf_counter()
        sock = socket.create_connection((host, port), timeout=timeout)
        latency_ms = round((time.perf_counter() - start) * 1000, 1)
    except OSError as exc:
        return EndpointCheck(host, port, type_, "Unhealthy", error=str(exc))

    try:
        if port == TLS_PORT:
            try:
                issuer, expires = _probe_tls(sock, host)
            except (ssl.SSLError, OSError) as exc:
                return EndpointCheck(
                    host,
                    port,
                    type_,
                    "Unhealthy",
                    error=f"TLS handshake failed: {exc}",
                )
        else:
            issuer = expires = None
    finally:
        try:
            sock.close()
        except OSError:
            pass

    return EndpointCheck(
        host,
        port,
        type_,
        "Healthy",
        cert_issuer=issuer,
        cert_expires=expires,
        latency_ms=latency_ms,
    )


def _normalise_entries(
    entries: Iterable[dict[str, Any]],
) -> list[tuple[str, int, str]]:
    """Pull `(host, port, type)` triples from allowlist entries, dropping malformed rows."""
    out: list[tuple[str, int, str]] = []
    for entry in entries:
        host = entry.get("host")
        if not isinstance(host, str):
            continue
        try:
            port = int(entry.get("port", TLS_PORT))
        except (TypeError, ValueError):
            continue
        out.append((host, port, str(entry.get("type", "UNKNOWN"))))
    return out


def run_diagnostic(
    conn: SnowflakeConnection,
    allowlist_path: Optional[Path],
    on_check: Callable[[EndpointCheck], None] = lambda _check: None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> DiagnosticReport:
    """Load the allowlist, probe each entry, return a full report.

    Calls `on_check(...)` once per endpoint in input order so the CLI can
    stream `Checking <TYPE>: <host> ✅` lines as the run progresses.

    If the queryable allowlist comes back empty (typically permission denied
    on `SYSTEM$ALLOWLIST()` for low-privilege roles) and no `allowlist_path`
    was supplied, fall back to checking just `(conn.host, 443)`.
    """
    allowlist = load_allowlist(conn, allowlist_path)
    if not allowlist and allowlist_path is None:
        log.warning(
            "Allowlist empty (likely permission denied on SYSTEM$ALLOWLIST()); "
            "falling back to the connection host."
        )
        allowlist = [
            {"type": "SNOWFLAKE_DEPLOYMENT", "host": conn.host, "port": TLS_PORT}
        ]

    report = DiagnosticReport()
    for host, port, type_ in _normalise_entries(allowlist):
        check = check_endpoint(host, port, type_, timeout=timeout)
        report.checks.append(check)
        on_check(check)
    return report


def status_line(check: EndpointCheck) -> str:
    """Format a streaming line: `Checking <TYPE>: <host> <icon>[ (detail)]`.

    Server-derived fields (`host`, `type`) are sanitised before display so
    crafted ANSI sequences in an allowlist response cannot rewrite the
    terminal.
    """
    icon = {"Healthy": "✅", "Unhealthy": "❌", "Skipped": "⏭"}[check.status]
    host = sanitize_for_terminal(check.host) or ""
    type_ = sanitize_for_terminal(check.type) or ""
    if check.status == "Healthy" and check.latency_ms is not None:
        suffix = f" ({check.latency_ms} ms)"
    elif check.status == "Unhealthy" and check.error:
        suffix = f" ({sanitize_for_terminal(check.error)})"
    else:
        suffix = ""
    return f"Checking {type_}: {host} {icon}{suffix}"


# --------------------------------------------------------------------------- #
# Network policy snapshot
# --------------------------------------------------------------------------- #


@dataclass
class NetworkRule:
    name: str
    mode: str  # INGRESS | EGRESS | INTERNAL_STAGE
    type: str  # noqa: A003 — IPV4 | HOST_PORT | AWSVPCEID | AZURELINKID | PRIVATE_HOST_PORT
    values: list[str] = field(default_factory=list)


@dataclass
class NetworkPolicySnapshot:
    """Snapshot of network-policy state at the time of `snow connection test`.

    `account_policy` and `user_policy` are the raw values from
    `SHOW PARAMETERS LIKE 'NETWORK_POLICY'` at each scope; `effective_policy`
    is `user_policy or account_policy` per Snowflake precedence.
    """

    current_ip: Optional[str] = None
    account_policy: Optional[str] = None
    user_policy: Optional[str] = None
    effective_policy: Optional[str] = None
    allowed_ip_list: list[str] = field(default_factory=list)
    blocked_ip_list: list[str] = field(default_factory=list)
    allowed_network_rule_list: list[str] = field(default_factory=list)
    blocked_network_rule_list: list[str] = field(default_factory=list)
    rules: list[NetworkRule] = field(default_factory=list)
    error: Optional[str] = None

    def has_policy(self) -> bool:
        return self.effective_policy is not None


def _query_rows(conn: SnowflakeConnection, sql: str) -> list[dict]:
    """Run a query and return rows as case-folded dicts; `[]` on any failure.

    Network-policy diagnostics are best-effort — a low-priv role may not be
    able to `SHOW PARAMETERS` at account scope or `DESC NETWORK POLICY`. Fail
    closed and let the caller surface the gap.
    """
    try:
        *_, cursor = conn.execute_string(sql, cursor_class=DictCursor)
        return [{str(k).lower(): v for k, v in row.items()} for row in cursor]
    except Exception as exc:
        log.debug("Network-policy query failed: %s\n%s", sql, exc)
        return []


def _scalar_value(rows: list[dict], column: str) -> Optional[str]:
    if not rows:
        return None
    val = rows[0].get(column.lower())
    return str(val) if val not in (None, "") else None


def _split_csv(value: Optional[str]) -> list[str]:
    return [v.strip() for v in (value or "").split(",") if v.strip()]


def _safe_fqn(name: str) -> Optional[FQN]:
    """Parse an identifier, returning `None` if it fails the FQN regex."""
    try:
        return FQN.from_string(name)
    except Exception:
        log.debug("Could not parse identifier as FQN: %r", name)
        return None


def _describe_network_policy(
    conn: SnowflakeConnection, name: str
) -> Optional[dict[str, list[str]]]:
    """Return parsed allow/block lists from `DESC NETWORK POLICY`, or None on failure."""
    fqn = _safe_fqn(name)
    if fqn is None:
        return None
    rows = _query_rows(conn, f"DESC NETWORK POLICY {fqn.sql_identifier}")
    if not rows:
        return None
    by_name = {
        str(r["name"]).upper(): str(r["value"])
        for r in rows
        if r.get("name") and r.get("value") is not None
    }
    return {
        "allowed_ip_list": _split_csv(by_name.get("ALLOWED_IP_LIST")),
        "blocked_ip_list": _split_csv(by_name.get("BLOCKED_IP_LIST")),
        "allowed_network_rule_list": _split_csv(
            by_name.get("ALLOWED_NETWORK_RULE_LIST")
        ),
        "blocked_network_rule_list": _split_csv(
            by_name.get("BLOCKED_NETWORK_RULE_LIST")
        ),
    }


def _describe_network_rule(
    conn: SnowflakeConnection, qualified_name: str
) -> Optional[NetworkRule]:
    fqn = _safe_fqn(qualified_name)
    if fqn is None:
        return None
    rows = _query_rows(conn, f"DESC NETWORK RULE {fqn.sql_identifier}")
    if not rows:
        return None
    head = rows[0]
    return NetworkRule(
        name=qualified_name,
        mode=str(head.get("mode") or ""),
        type=str(head.get("type") or ""),
        values=_split_csv(str(head.get("value_list") or "")),
    )


def collect_network_policy(
    conn: SnowflakeConnection, user: Optional[str] = None
) -> NetworkPolicySnapshot:
    """Best-effort snapshot of the active network policy and its referenced rules.

    Order of precedence:
      1. `SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER <user>` (user wins)
      2. `SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT`
      3. `DESC NETWORK POLICY <effective>` for inline allow/block lists
      4. `DESC NETWORK RULE <each>` for referenced rule values

    Always returns a snapshot; failures populate `snapshot.error` instead.
    """
    snapshot = NetworkPolicySnapshot()

    snapshot.current_ip = _scalar_value(
        _query_rows(conn, "SELECT CURRENT_IP_ADDRESS() AS IP"), "IP"
    )
    policy_pattern = identifier_to_show_like_pattern(NETWORK_POLICY_PARAM_NAME)
    snapshot.account_policy = _scalar_value(
        _query_rows(conn, f"SHOW PARAMETERS LIKE {policy_pattern} IN ACCOUNT"),
        "value",
    )
    if isinstance(user, str) and user.strip():
        user_fqn = _safe_fqn(user)
        if user_fqn is not None:
            snapshot.user_policy = _scalar_value(
                _query_rows(
                    conn,
                    f"SHOW PARAMETERS LIKE {policy_pattern} "
                    f"FOR USER {user_fqn.sql_identifier}",
                ),
                "value",
            )
    snapshot.effective_policy = snapshot.user_policy or snapshot.account_policy

    if not snapshot.effective_policy:
        return snapshot

    described = _describe_network_policy(conn, snapshot.effective_policy)
    if described is None:
        snapshot.error = (
            f"Could not DESC NETWORK POLICY {snapshot.effective_policy} "
            "(role lacks privilege?). Allowed/blocked lists not shown."
        )
        return snapshot

    snapshot.allowed_ip_list = described["allowed_ip_list"]
    snapshot.blocked_ip_list = described["blocked_ip_list"]
    snapshot.allowed_network_rule_list = described["allowed_network_rule_list"]
    snapshot.blocked_network_rule_list = described["blocked_network_rule_list"]

    for rule_name in (
        *snapshot.allowed_network_rule_list,
        *snapshot.blocked_network_rule_list,
    ):
        rule = _describe_network_rule(conn, rule_name)
        if rule is not None:
            snapshot.rules.append(rule)

    return snapshot
