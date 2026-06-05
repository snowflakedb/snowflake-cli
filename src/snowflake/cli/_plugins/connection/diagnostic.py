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

Replicates the per-endpoint checks that SnowCD used to provide, sourcing
endpoints from `SYSTEM$ALLOWLIST()` (or a JSON file passed via
`--diag-allowlist-path`). Each resolvable endpoint is probed with a TCP
connect, and TLS port 443 endpoints additionally have their certificate
issuer and expiry recorded.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import ssl
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Optional

from click.exceptions import ClickException
from snowflake.cli._plugins.connection.util import ALLOWLIST_QUERY
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS: float = 5.0
TLS_PORT: int = 443

Status = Literal["Healthy", "Unhealthy", "Skipped"]


@dataclass
class EndpointCheck:
    host: str
    port: int
    type: str  # noqa: A003 — intentional public field name, matches `SYSTEM$ALLOWLIST()` JSON
    status: Status
    error: Optional[str] = None
    cert_issuer: Optional[str] = None
    cert_expires: Optional[str] = None
    latency_ms: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DiagnosticReport:
    checks: list[EndpointCheck] = field(default_factory=list)

    @property
    def healthy(self) -> int:
        return sum(1 for c in self.checks if c.status == "Healthy")

    @property
    def unhealthy(self) -> int:
        return sum(1 for c in self.checks if c.status == "Unhealthy")

    @property
    def skipped(self) -> int:
        return sum(1 for c in self.checks if c.status == "Skipped")

    @property
    def tested(self) -> int:
        return self.healthy + self.unhealthy

    def summary_line(self) -> str:
        return (
            f"Results: {self.healthy} Healthy, {self.unhealthy} Unhealthy "
            f"out of {self.tested} endpoints. "
            f"{self.skipped} skipped (non-resolvable patterns)."
        )


ALLOWLIST_PRIVATELINK_QUERY = "SELECT SYSTEM$ALLOWLIST_PRIVATELINK()"


def _query_allowlist(conn: SnowflakeConnection, sql: str, key: str) -> list[dict]:
    """Run an allowlist-returning system function and parse its single JSON cell.

    Returns `[]` on any failure (permission denied, function not present in
    the deployment, malformed JSON). The caller must not let this raise.
    """
    try:
        *_, cursor = conn.execute_string(sql, cursor_class=DictCursor)
        row = cursor.fetchone()
        if not row:
            return []
        raw = row.get(key)
        if not raw:
            return []
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        log.debug("Allowlist query failed: %s", sql, exc_info=True)
        return []


def _dedupe_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop duplicate `(type, host, port)` triples, preserving first-seen order."""
    seen: set[tuple[str, str, Any]] = set()
    out: list[dict[str, Any]] = []
    for e in entries:
        key = (str(e.get("type", "")), str(e.get("host", "")), e.get("port"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def load_allowlist(
    conn: SnowflakeConnection, allowlist_path: Optional[Path]
) -> list[dict[str, Any]]:
    """Return the raw allowlist as a list of `{type, host, port}` dicts.

    If `allowlist_path` is given, parse it as JSON and use it directly.
    Otherwise merge `SYSTEM$ALLOWLIST()` and `SYSTEM$ALLOWLIST_PRIVATELINK()`
    on the open connection. PrivateLink entries are appended after the public
    set; duplicate `(type, host, port)` triples are dropped. Either query may
    fail (low-priv role, function unavailable in deployment) — failures are
    silent and we return whatever we managed to collect, with the public
    allowlist's failure handled by `run_diagnostic` (fall back to the
    connection host) and the privatelink failure simply skipping that source.
    """
    if allowlist_path is not None:
        try:
            payload = json.loads(Path(allowlist_path).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise ClickException(
                f"Could not read allowlist file {allowlist_path}: {exc}"
            )
        if not isinstance(payload, list):
            raise ClickException(
                f"Allowlist file {allowlist_path} must contain a JSON array."
            )
        return payload

    # Public allowlist: let exceptions propagate so run_diagnostic's
    # fallback-to-conn.host branch fires when the role lacks privileges.
    *_, cursor = conn.execute_string(ALLOWLIST_QUERY, cursor_class=DictCursor)
    public = json.loads(cursor.fetchone()["SYSTEM$ALLOWLIST()"])
    privatelink = _query_allowlist(
        conn, ALLOWLIST_PRIVATELINK_QUERY, "SYSTEM$ALLOWLIST_PRIVATELINK()"
    )
    if not isinstance(public, list):
        public = []
    return _dedupe_entries([*public, *privatelink])


def is_resolvable(host: str) -> bool:
    """Return False for hostnames the OS resolver can never satisfy.

    Allowlist entries can include wildcard patterns (e.g.
    `*.region.snowflakecomputing.com`) and bare placeholder values; those are
    counted as `Skipped` rather than `Unhealthy`.
    """
    if not host:
        return False
    if "*" in host:
        return False
    return True


def _resolve_cafile() -> Optional[str]:
    """Honour the same CA bundle env vars `snowflake-connector-python` reads.

    `REQUESTS_CA_BUNDLE` wins (matches `requests` and the connector); falls
    back to `SSL_CERT_FILE` (the OpenSSL convention). Returns `None` if
    neither points at a readable file, in which case `ssl.create_default_context()`
    will use the system trust store.
    """
    for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        path = os.environ.get(var)
        if path and os.path.isfile(path):
            return path
    return None


def _probe_tls(sock: socket.socket, host: str) -> tuple[Optional[str], Optional[str]]:
    """Wrap `sock` in TLS and return `(issuer, not_after)` from the peer cert.

    Caller is responsible for closing the underlying socket.
    """
    context = ssl.create_default_context(cafile=_resolve_cafile())
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    with context.wrap_socket(sock, server_hostname=host) as ssock:
        cert: Any = ssock.getpeercert() or {}
    issuer_raw: Any = cert.get("issuer") or ()
    issuer_cn: Optional[str] = None
    for rdn in issuer_raw:
        for entry in rdn:
            if (
                isinstance(entry, tuple)
                and len(entry) == 2
                and entry[0] == "organizationName"
            ):
                issuer_cn = entry[1]
                break
        if issuer_cn:
            break
    not_after = cert.get("notAfter")
    return issuer_cn, not_after if isinstance(not_after, str) else None


def check_endpoint(
    host: str,
    port: int,
    type_: str,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> EndpointCheck:
    """Probe a single endpoint. Never raises; failures map to Unhealthy."""
    if not is_resolvable(host):
        return EndpointCheck(
            host=host,
            port=port,
            type=type_,
            status="Skipped",
            error="non-resolvable pattern",
        )

    try:
        # getaddrinfo + connect together cover DNS and TCP reachability.
        start = time.perf_counter()
        sock = socket.create_connection((host, port), timeout=timeout)
        latency_ms = round((time.perf_counter() - start) * 1000, 1)
    except OSError as exc:
        return EndpointCheck(
            host=host,
            port=port,
            type=type_,
            status="Unhealthy",
            error=str(exc),
        )

    issuer = expires = None
    try:
        if port == TLS_PORT:
            try:
                issuer, expires = _probe_tls(sock, host)
            except (ssl.SSLError, OSError) as exc:
                return EndpointCheck(
                    host=host,
                    port=port,
                    type=type_,
                    status="Unhealthy",
                    error=f"TLS handshake failed: {exc}",
                )
    finally:
        try:
            sock.close()
        except OSError:
            pass

    return EndpointCheck(
        host=host,
        port=port,
        type=type_,
        status="Healthy",
        cert_issuer=issuer,
        cert_expires=expires,
        latency_ms=latency_ms,
    )


def _normalise_entries(
    entries: Iterable[dict[str, Any]],
) -> list[tuple[str, int, str]]:
    """Pull (host, port, type) triples out of allowlist entries, dropping malformed rows."""
    out: list[tuple[str, int, str]] = []
    for entry in entries:
        host = entry.get("host")
        port = entry.get("port", TLS_PORT)
        type_ = entry.get("type", "UNKNOWN")
        if not isinstance(host, str):
            continue
        try:
            port_int = int(port)
        except (TypeError, ValueError):
            continue
        out.append((host, port_int, str(type_)))
    return out


def run_diagnostic(
    conn: SnowflakeConnection,
    allowlist_path: Optional[Path],
    on_check: Callable[[EndpointCheck], None] = lambda _check: None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> DiagnosticReport:
    """Load the allowlist, probe each entry, return a full report.

    Calls `on_check(...)` once per endpoint in input order; this is how the CLI
    streams `Checking <TYPE>: <host> ✅` lines as the run progresses.

    If `SYSTEM$ALLOWLIST()` cannot be queried (no file path and the call
    fails — typically a permission error for low-privilege roles), the
    diagnostic falls back to checking just `(conn.host, 443)`.
    """
    try:
        allowlist = load_allowlist(conn, allowlist_path)
    except ClickException:
        raise
    except Exception as exc:
        log.warning(
            "Could not call SYSTEM$ALLOWLIST(); falling back to the connection host.",
            exc_info=True,
        )
        allowlist = [
            {"type": "SNOWFLAKE_DEPLOYMENT", "host": conn.host, "port": TLS_PORT}
        ]
        # Surface the fallback so users notice it in --enable-diag output.
        log.info("Allowlist fetch failed: %s", exc)

    report = DiagnosticReport()
    for host, port, type_ in _normalise_entries(allowlist):
        check = check_endpoint(host, port, type_, timeout=timeout)
        report.checks.append(check)
        on_check(check)
    return report


def status_line(check: EndpointCheck) -> str:
    """Return the streaming-line representation: `Checking <TYPE>: <host> <icon>`."""
    icon = {"Healthy": "✅", "Unhealthy": "❌", "Skipped": "⏭"}[check.status]
    suffix = f" ({check.error})" if check.status == "Unhealthy" and check.error else ""
    if check.status == "Healthy" and check.latency_ms is not None:
        suffix = f" ({check.latency_ms} ms)"
    return f"Checking {check.type}: {check.host} {icon}{suffix}"


# --------------------------------------------------------------------------- #
# Network policy / IP rule diagnostic
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

    `account_policy` and `user_policy` are the names returned by
    `SHOW PARAMETERS LIKE 'NETWORK_POLICY'` at each scope; the effective
    policy is `user_policy or account_policy` per Snowflake precedence rules.
    """

    current_ip: Optional[str] = None
    account_policy: Optional[str] = None
    user_policy: Optional[str] = None
    effective_policy: Optional[str] = None
    allowed_ip_list: list[str] = field(default_factory=list)
    blocked_ip_list: list[str] = field(default_factory=list)
    allowed_rule_list: list[str] = field(default_factory=list)
    blocked_rule_list: list[str] = field(default_factory=list)
    rules: list[NetworkRule] = field(default_factory=list)
    error: Optional[str] = None

    def has_policy(self) -> bool:
        return self.effective_policy is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_ip": self.current_ip,
            "account_policy": self.account_policy,
            "user_policy": self.user_policy,
            "effective_policy": self.effective_policy,
            "allowed_ip_list": self.allowed_ip_list,
            "blocked_ip_list": self.blocked_ip_list,
            "allowed_network_rule_list": self.allowed_rule_list,
            "blocked_network_rule_list": self.blocked_rule_list,
            "rules": [asdict(r) for r in self.rules],
            "error": self.error,
        }


def _safe_query(conn: SnowflakeConnection, sql: str) -> Optional[list[dict]]:
    """Run a query through `execute_string`; return rows as dicts, or None on error.

    Network-policy diagnostics are best-effort: a low-priv role may not be
    able to call `SHOW PARAMETERS` at account scope or `DESCRIBE NETWORK
    POLICY`. Fail closed and let the caller surface the gap.
    """
    try:
        *_, cursor = conn.execute_string(sql, cursor_class=DictCursor)
        return list(cursor)
    except Exception as exc:
        log.debug("Network-policy query failed: %s\n%s", sql, exc)
        return None


def _scalar_value(rows: Optional[list[dict]], column: str) -> Optional[str]:
    if not rows:
        return None
    val = rows[0].get(column)
    return str(val) if val not in (None, "") else None


def _split_csv(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _describe_network_policy(
    conn: SnowflakeConnection, name: str, snapshot: NetworkPolicySnapshot
) -> bool:
    """Populate snapshot allow/block lists from `DESC NETWORK POLICY <name>`.

    Returns True if the DESC succeeded (any row came back), False otherwise.
    """
    rows = _safe_query(conn, f"DESC NETWORK POLICY IDENTIFIER('{name}')")
    if not rows:
        return False
    by_name: dict[str, str] = {}
    for row in rows:
        prop = row.get("name") or row.get("NAME")
        val = row.get("value") or row.get("VALUE")
        if prop and val is not None:
            by_name[str(prop).upper()] = str(val)
    snapshot.allowed_ip_list = _split_csv(by_name.get("ALLOWED_IP_LIST"))
    snapshot.blocked_ip_list = _split_csv(by_name.get("BLOCKED_IP_LIST"))
    snapshot.allowed_rule_list = _split_csv(by_name.get("ALLOWED_NETWORK_RULE_LIST"))
    snapshot.blocked_rule_list = _split_csv(by_name.get("BLOCKED_NETWORK_RULE_LIST"))
    return True


def _describe_network_rule(
    conn: SnowflakeConnection, qualified_name: str
) -> Optional[NetworkRule]:
    rows = _safe_query(conn, f"DESC NETWORK RULE IDENTIFIER('{qualified_name}')")
    if not rows:
        return None
    head = rows[0]
    keys = {k.lower(): k for k in head.keys()}
    mode = head.get(keys.get("mode", ""), "") or ""
    type_ = head.get(keys.get("type", ""), "") or ""
    raw_values = head.get(keys.get("value_list", ""), "") or ""
    return NetworkRule(
        name=qualified_name,
        mode=str(mode),
        type=str(type_),
        values=_split_csv(str(raw_values)),
    )


def collect_network_policy(
    conn: SnowflakeConnection, user: Optional[str] = None
) -> NetworkPolicySnapshot:
    """Best-effort snapshot of the active network policy and its referenced rules.

    Sources, in order of precedence:
      1. `SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER <user>` (user-level wins)
      2. `SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT`
      3. `DESC NETWORK POLICY <effective_name>` for the inline allow/block lists
      4. `DESC NETWORK RULE <each>` for referenced rule values

    Always returns a snapshot; failures populate `snapshot.error` instead.
    """
    snapshot = NetworkPolicySnapshot()

    # CURRENT_IP_ADDRESS() is the documented function. SYSTEM$GET_CLIENT_IP
    # exists in some internal deployments but is not generally available.
    ip_rows = _safe_query(conn, "SELECT CURRENT_IP_ADDRESS() AS IP")
    if ip_rows:
        snapshot.current_ip = _scalar_value(ip_rows, "IP")

    account_rows = _safe_query(conn, "SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT")
    snapshot.account_policy = _scalar_value(account_rows, "value")

    if user:
        user_rows = _safe_query(
            conn,
            f"SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER IDENTIFIER('{user}')",
        )
        snapshot.user_policy = _scalar_value(user_rows, "value")

    snapshot.effective_policy = snapshot.user_policy or snapshot.account_policy

    if snapshot.effective_policy:
        described = _describe_network_policy(conn, snapshot.effective_policy, snapshot)
        if not described:
            snapshot.error = (
                f"Could not DESC NETWORK POLICY {snapshot.effective_policy} "
                "(role lacks privilege?). Allowed/blocked lists not shown."
            )
        for rule_name in (
            *snapshot.allowed_rule_list,
            *snapshot.blocked_rule_list,
        ):
            rule = _describe_network_rule(conn, rule_name)
            if rule is not None:
                snapshot.rules.append(rule)

    return snapshot
