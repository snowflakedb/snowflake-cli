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
from click.exceptions import ClickException
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
    with pytest.raises(ClickException):
        load_allowlist(mock.MagicMock(), path)


def test_load_allowlist_from_query_when_no_path():
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = {"SYSTEM$ALLOWLIST()": json.dumps(ALLOWLIST_FIXTURE)}
    conn = mock.MagicMock()
    conn.execute_string.return_value = (cursor,)
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
    with mock.patch(
        "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
        return_value=fake_sock,
    ), mock.patch(
        "snowflake.cli._plugins.connection.diagnostic._probe_tls",
        side_effect=ssl.SSLError("cert verify failed"),
    ):
        result = check_endpoint("x.com", 443, "SNOWFLAKE_DEPLOYMENT")
    assert result.status == "Unhealthy"
    assert "TLS handshake failed" in (result.error or "")


def test_check_endpoint_records_latency_and_cert():
    fake_sock = mock.MagicMock()
    with mock.patch(
        "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
        return_value=fake_sock,
    ), mock.patch(
        "snowflake.cli._plugins.connection.diagnostic._probe_tls",
        return_value=("DigiCert Inc", "Jan 19 23:59:59 2027 GMT"),
    ):
        result = check_endpoint("x.com", 443, "SNOWFLAKE_DEPLOYMENT")
    assert result.status == "Healthy"
    assert result.cert_issuer == "DigiCert Inc"
    assert result.cert_expires == "Jan 19 23:59:59 2027 GMT"
    assert result.latency_ms is not None
    assert result.latency_ms >= 0


def test_check_endpoint_skips_tls_for_port_80():
    fake_sock = mock.MagicMock()
    with mock.patch(
        "snowflake.cli._plugins.connection.diagnostic.socket.create_connection",
        return_value=fake_sock,
    ), mock.patch(
        "snowflake.cli._plugins.connection.diagnostic._probe_tls"
    ) as probe_tls:
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

    def execute_string(sql, cursor_class=None):
        if "SYSTEM$GET_CLIENT_IP" in sql:
            return (_scripted_cursor([{"IP": "1.2.3.4"}]),)
        if "NETWORK_POLICY' IN ACCOUNT" in sql:
            return (_scripted_cursor([{"value": "ACCT_POLICY"}]),)
        if "NETWORK_POLICY' FOR USER" in sql:
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
    assert snap.allowed_rule_list == ["DB.SCHEMA.NR1"]
    assert len(snap.rules) == 1
    assert snap.rules[0].values == ["1.0.0.0/8", "2.0.0.0/8"]
    assert snap.rules[0].mode == "INGRESS"
    assert snap.rules[0].type == "IPV4"


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
