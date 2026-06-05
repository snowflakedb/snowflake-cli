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

"""Tests for `snow connection test --enable-diag` SnowCD-style diagnostics."""

from __future__ import annotations

import json
import socket
import ssl
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from snowflake.cli._plugins.connection.diagnostic import (
    DiagnosticReport,
    EndpointCheck,
    NetworkPolicySnapshot,
    check_endpoint,
    collect_network_policy,
    is_resolvable,
    load_allowlist,
    run_diagnostic,
    status_line,
)
from snowflake.cli.api.exceptions import CliError

ALLOWLIST_FIXTURE = [
    {
        "type": "SNOWFLAKE_DEPLOYMENT",
        "host": "acct.snowflakecomputing.com",
        "port": 443,
    },
    {"type": "OCSP_RESPONDER", "host": "ocsp.x.com", "port": 80},
    {"type": "STAGE", "host": "*.region.snowflakecomputing.com", "port": 443},
]


@pytest.mark.parametrize(
    "host, expected",
    [
        ("acct.snowflakecomputing.com", True),
        ("sfc-stage.s3.amazonaws.com", True),
        ("*.region.snowflakecomputing.com", False),
        ("", False),
    ],
)
def test_is_resolvable(host, expected):
    assert is_resolvable(host) is expected


def test_status_line_includes_latency_for_healthy():
    line = status_line(
        EndpointCheck("x.com", 443, "SNOWFLAKE_DEPLOYMENT", "Healthy", latency_ms=12.3)
    )
    assert "✅" in line
    assert "x.com" in line
    assert "12.3 ms" in line


def test_status_line_includes_error_for_unhealthy():
    line = status_line(
        EndpointCheck(
            "x.com", 443, "SNOWFLAKE_DEPLOYMENT", "Unhealthy", error="DNS fail"
        )
    )
    assert "❌" in line
    assert "DNS fail" in line


def test_status_line_strips_ansi_from_server_host():
    """Server-supplied host must not be able to inject ANSI escapes into the terminal."""
    malicious = "\x1b[31mevil.com\x1b[0m"
    line = status_line(
        EndpointCheck(malicious, 443, "SNOWFLAKE_DEPLOYMENT", "Healthy", latency_ms=1.0)
    )
    assert "\x1b" not in line
    assert "evil.com" in line


def test_load_allowlist_from_file():
    with NamedTemporaryFile("w+", suffix=".json", delete=False) as f:
        f.write(json.dumps(ALLOWLIST_FIXTURE))
        path = Path(f.name)
    conn = mock.MagicMock()
    out = load_allowlist(conn, path)
    assert out == ALLOWLIST_FIXTURE
    conn.execute_string.assert_not_called()


def test_load_allowlist_file_invalid_json_raises():
    with NamedTemporaryFile("w+", suffix=".json", delete=False) as f:
        f.write("not json")
        path = Path(f.name)
    with pytest.raises(CliError):
        load_allowlist(mock.MagicMock(), path)


def test_load_allowlist_from_query_when_no_path():
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = {"SYSTEM$ALLOWLIST()": json.dumps(ALLOWLIST_FIXTURE)}
    conn = mock.MagicMock()
    conn.execute_string.return_value = (cursor,)
    assert load_allowlist(conn, None) == ALLOWLIST_FIXTURE


def test_load_allowlist_merges_privatelink_entries():
    """SYSTEM$ALLOWLIST_PRIVATELINK() entries are appended after the public set."""
    pl_extra = [
        {
            "type": "SNOWFLAKE_DEPLOYMENT",
            "host": "acct.privatelink.snowflakecomputing.com",
            "port": 443,
        }
    ]

    def execute_string(sql, cursor_class=None):
        cur = mock.MagicMock()
        if "PRIVATELINK" in sql:
            cur.fetchone.return_value = {
                "SYSTEM$ALLOWLIST_PRIVATELINK()": json.dumps(pl_extra)
            }
        else:
            cur.fetchone.return_value = {
                "SYSTEM$ALLOWLIST()": json.dumps(ALLOWLIST_FIXTURE)
            }
        return (cur,)

    conn = mock.MagicMock()
    conn.execute_string.side_effect = execute_string
    out = load_allowlist(conn, None)
    assert out == ALLOWLIST_FIXTURE + pl_extra


def test_load_allowlist_dedupes_overlap_with_privatelink():
    """A (type, host, port) appearing in both lists collapses to one entry."""
    overlap = ALLOWLIST_FIXTURE[0]  # SNOWFLAKE_DEPLOYMENT acct...

    def execute_string(sql, cursor_class=None):
        cur = mock.MagicMock()
        if "PRIVATELINK" in sql:
            cur.fetchone.return_value = {
                "SYSTEM$ALLOWLIST_PRIVATELINK()": json.dumps([overlap])
            }
        else:
            cur.fetchone.return_value = {
                "SYSTEM$ALLOWLIST()": json.dumps(ALLOWLIST_FIXTURE)
            }
        return (cur,)

    conn = mock.MagicMock()
    conn.execute_string.side_effect = execute_string
    out = load_allowlist(conn, None)
    # Same length — the duplicate from PL was dropped.
    assert len(out) == len(ALLOWLIST_FIXTURE)


def test_load_allowlist_silently_skips_failing_privatelink_query():
    """If SYSTEM$ALLOWLIST_PRIVATELINK is unavailable the public list is still returned."""

    def execute_string(sql, cursor_class=None):
        if "PRIVATELINK" in sql:
            raise RuntimeError("Function not enabled in this deployment")
        cur = mock.MagicMock()
        cur.fetchone.return_value = {
            "SYSTEM$ALLOWLIST()": json.dumps(ALLOWLIST_FIXTURE)
        }
        return (cur,)

    conn = mock.MagicMock()
    conn.execute_string.side_effect = execute_string
    assert load_allowlist(conn, None) == ALLOWLIST_FIXTURE


def test_check_endpoint_skips_wildcards():
    result = check_endpoint("*.x.com", 443, "STAGE")
    assert result.status == "Skipped"
    assert result.error == "non-resolvable pattern"


def test_check_endpoint_marks_unhealthy_on_dns_failure():
    with mock.patch(
        "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
        side_effect=socket.gaierror("Name or service not known"),
    ):
        result = check_endpoint("nonexistent.invalid", 443, "SNOWFLAKE_DEPLOYMENT")
    assert result.status == "Unhealthy"
    assert "Name or service" in (result.error or "")
    assert result.latency_ms is None


def test_check_endpoint_marks_unhealthy_on_tls_failure():
    fake_sock = mock.MagicMock()
    with (
        mock.patch(
            "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
            return_value=fake_sock,
        ),
        mock.patch(
            "snowflake.cli._plugins.connection.diagnostic._probe_tls",
            side_effect=ssl.SSLError("cert verify failed"),
        ),
    ):
        result = check_endpoint("x.com", 443, "SNOWFLAKE_DEPLOYMENT")
    assert result.status == "Unhealthy"
    assert "TLS handshake failed" in (result.error or "")


def test_check_endpoint_records_latency_and_cert():
    fake_sock = mock.MagicMock()
    with (
        mock.patch(
            "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
            return_value=fake_sock,
        ),
        mock.patch(
            "snowflake.cli._plugins.connection.diagnostic._probe_tls",
            return_value=("DigiCert Inc", "Jan 19 23:59:59 2027 GMT"),
        ),
    ):
        result = check_endpoint("x.com", 443, "SNOWFLAKE_DEPLOYMENT")
    assert result.status == "Healthy"
    assert result.cert_issuer == "DigiCert Inc"
    assert result.cert_expires == "Jan 19 23:59:59 2027 GMT"
    assert result.latency_ms is not None
    assert result.latency_ms >= 0


def test_check_endpoint_skips_tls_for_port_80():
    fake_sock = mock.MagicMock()
    with (
        mock.patch(
            "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
            return_value=fake_sock,
        ),
        mock.patch(
            "snowflake.cli._plugins.connection.diagnostic._probe_tls"
        ) as probe_tls,
    ):
        result = check_endpoint("crl.example.com", 80, "CRL_DISTRIBUTION_POINT")
    probe_tls.assert_not_called()
    assert result.status == "Healthy"
    assert result.cert_issuer is None
    assert result.cert_expires is None


def test_diagnostic_report_counts():
    report = DiagnosticReport(
        checks=[
            EndpointCheck("a", 443, "X", "Healthy"),
            EndpointCheck("b", 443, "X", "Healthy"),
            EndpointCheck("c", 443, "X", "Unhealthy"),
            EndpointCheck("d", 443, "X", "Skipped"),
        ]
    )
    assert report.healthy == 2
    assert report.unhealthy == 1
    assert report.skipped == 1
    assert report.tested == 3
    assert (
        report.summary_line()
        == "Results: 2 Healthy, 1 Unhealthy out of 3 endpoints. 1 skipped (non-resolvable patterns)."
    )


def test_run_diagnostic_streams_per_endpoint(monkeypatch):
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = {"SYSTEM$ALLOWLIST()": json.dumps(ALLOWLIST_FIXTURE)}
    conn = mock.MagicMock()
    conn.execute_string.return_value = (cursor,)

    fake_sock = mock.MagicMock()
    monkeypatch.setattr(
        "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
        lambda *a, **kw: fake_sock,
    )
    monkeypatch.setattr(
        "snowflake.cli._plugins.connection.diagnostic._probe_tls",
        lambda sock, host: ("DigiCert Inc", "Jan 19 23:59:59 2027 GMT"),
    )

    streamed = []
    report = run_diagnostic(conn, allowlist_path=None, on_check=streamed.append)

    assert len(streamed) == 3
    assert report.healthy == 2
    assert report.skipped == 1
    assert [c.type for c in streamed] == [
        "SNOWFLAKE_DEPLOYMENT",
        "OCSP_RESPONDER",
        "STAGE",
    ]


def test_run_diagnostic_falls_back_when_allowlist_query_fails(monkeypatch):
    conn = mock.MagicMock()
    conn.host = "fallback.snowflakecomputing.com"
    conn.execute_string.side_effect = RuntimeError(
        "permission denied on SYSTEM$ALLOWLIST"
    )

    fake_sock = mock.MagicMock()
    monkeypatch.setattr(
        "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
        lambda *a, **kw: fake_sock,
    )
    monkeypatch.setattr(
        "snowflake.cli._plugins.connection.diagnostic._probe_tls",
        lambda sock, host: (None, None),
    )

    report = run_diagnostic(conn, allowlist_path=None)
    assert report.tested == 1
    assert report.checks[0].host == "fallback.snowflakecomputing.com"
    assert report.checks[0].type == "SNOWFLAKE_DEPLOYMENT"


def test_run_diagnostic_normalises_malformed_entries(monkeypatch):
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = {
        "SYSTEM$ALLOWLIST()": json.dumps(
            [
                {"type": "X", "host": "good.com", "port": 443},
                {"type": "X", "host": None, "port": 443},
                {"type": "X", "host": "bad.com", "port": "not-a-number"},
            ]
        )
    }
    conn = mock.MagicMock()
    conn.execute_string.return_value = (cursor,)
    monkeypatch.setattr(
        "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
        lambda *a, **kw: mock.MagicMock(),
    )
    monkeypatch.setattr(
        "snowflake.cli._plugins.connection.diagnostic._probe_tls",
        lambda *a, **kw: (None, None),
    )
    report = run_diagnostic(conn, allowlist_path=None)
    assert [c.host for c in report.checks] == ["good.com"]


def _scripted_cursor(rows):
    cur = mock.MagicMock()
    cur.__iter__ = lambda self: iter(rows)
    return cur


def test_collect_network_policy_user_overrides_account():
    conn = mock.MagicMock()
    seen_sql: list[str] = []

    def execute_string(sql, cursor_class=None):
        seen_sql.append(sql)
        if "CURRENT_IP_ADDRESS" in sql:
            return (_scripted_cursor([{"IP": "1.2.3.4"}]),)
        if "SHOW PARAMETERS" in sql and "IN ACCOUNT" in sql:
            return (_scripted_cursor([{"value": "ACCT_POLICY"}]),)
        if "SHOW PARAMETERS" in sql and "FOR USER" in sql:
            return (_scripted_cursor([{"value": "USER_POLICY"}]),)
        if "DESC NETWORK POLICY" in sql:
            return (
                _scripted_cursor(
                    [
                        {"name": "ALLOWED_IP_LIST", "value": "0.0.0.0/0"},
                        {"name": "BLOCKED_IP_LIST", "value": ""},
                        {"name": "ALLOWED_NETWORK_RULE_LIST", "value": "DB.SCHEMA.NR1"},
                        {"name": "BLOCKED_NETWORK_RULE_LIST", "value": ""},
                    ]
                ),
            )
        if "DESC NETWORK RULE" in sql:
            return (
                _scripted_cursor(
                    [
                        {
                            "mode": "INGRESS",
                            "type": "IPV4",
                            "value_list": "1.0.0.0/8,2.0.0.0/8",
                        }
                    ]
                ),
            )
        return (_scripted_cursor([]),)

    conn.execute_string.side_effect = execute_string
    snap = collect_network_policy(conn, user="alice")
    assert snap.current_ip == "1.2.3.4"
    assert snap.account_policy == "ACCT_POLICY"
    assert snap.user_policy == "USER_POLICY"
    assert snap.effective_policy == "USER_POLICY"
    assert snap.allowed_ip_list == ["0.0.0.0/0"]
    assert snap.allowed_network_rule_list == ["DB.SCHEMA.NR1"]
    assert len(snap.rules) == 1
    assert snap.rules[0].values == ["1.0.0.0/8", "2.0.0.0/8"]
    assert snap.rules[0].mode == "INGRESS"
    assert snap.rules[0].type == "IPV4"
    # Identifiers must be wrapped in IDENTIFIER('...') — never bare-interpolated.
    user_sql = next(s for s in seen_sql if "FOR USER" in s)
    assert "IDENTIFIER('alice')" in user_sql
    desc_sql = next(s for s in seen_sql if "DESC NETWORK POLICY" in s)
    assert "IDENTIFIER('USER_POLICY')" in desc_sql
    rule_sql = next(s for s in seen_sql if "DESC NETWORK RULE" in s)
    assert "IDENTIFIER('DB.SCHEMA.NR1')" in rule_sql


def test_collect_network_policy_handles_no_policy():
    conn = mock.MagicMock()
    conn.execute_string.return_value = (_scripted_cursor([]),)
    snap = collect_network_policy(conn, user="alice")
    assert snap.has_policy() is False
    assert snap.effective_policy is None


def test_collect_network_policy_survives_query_errors():
    conn = mock.MagicMock()
    conn.execute_string.side_effect = RuntimeError("insufficient privileges")
    snap = collect_network_policy(conn, user="alice")
    assert isinstance(snap, NetworkPolicySnapshot)
    assert snap.has_policy() is False


def test_resolve_cafile_prefers_requests_ca_bundle(tmp_path, monkeypatch):
    from snowflake.cli._plugins.connection.diagnostic import _resolve_cafile

    primary = tmp_path / "primary.pem"
    primary.write_text("primary")
    secondary = tmp_path / "secondary.pem"
    secondary.write_text("secondary")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(primary))
    monkeypatch.setenv("SSL_CERT_FILE", str(secondary))
    assert _resolve_cafile() == str(primary)


def test_resolve_cafile_falls_back_to_ssl_cert_file(tmp_path, monkeypatch):
    from snowflake.cli._plugins.connection.diagnostic import _resolve_cafile

    bundle = tmp_path / "bundle.pem"
    bundle.write_text("bundle")
    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.setenv("SSL_CERT_FILE", str(bundle))
    assert _resolve_cafile() == str(bundle)


def test_resolve_cafile_returns_none_when_env_points_at_missing_file(monkeypatch):
    from snowflake.cli._plugins.connection.diagnostic import _resolve_cafile

    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/nonexistent/ca.pem")
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    assert _resolve_cafile() is None


def test_resolve_cafile_returns_none_when_unset(monkeypatch):
    from snowflake.cli._plugins.connection.diagnostic import _resolve_cafile

    monkeypatch.delenv("REQUESTS_CA_BUNDLE", raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    assert _resolve_cafile() is None


def test_probe_tls_passes_cafile_to_ssl_context(tmp_path, monkeypatch):
    """_probe_tls plumbs the resolved CA bundle into ssl.create_default_context."""
    bundle = tmp_path / "bundle.pem"
    bundle.write_text("bundle")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(bundle))

    fake_context = mock.MagicMock()
    fake_ssock = mock.MagicMock()
    fake_ssock.__enter__ = lambda self: fake_ssock
    fake_ssock.__exit__ = lambda self, *a: False
    fake_ssock.getpeercert.return_value = {}
    fake_context.wrap_socket.return_value = fake_ssock

    with mock.patch(
        "snowflake.cli._plugins.connection.diagnostic.ssl.create_default_context",
        return_value=fake_context,
    ) as create_default_context:
        from snowflake.cli._plugins.connection.diagnostic import _probe_tls

        _probe_tls(mock.MagicMock(), "x.com")
    create_default_context.assert_called_once_with(cafile=str(bundle))
