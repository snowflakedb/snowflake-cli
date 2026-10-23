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

from snowflake.cli._plugins.feedback.constants import MAX_MESSAGE_BYTES, SECRET_MASK
from snowflake.cli._plugins.feedback.redact import prepare_message


def test_prepare_message_strips_and_preserves_text():
    redacted, truncated = prepare_message("  the sql command is confusing  \n")
    assert redacted == "the sql command is confusing"
    assert truncated is False


def test_prepare_message_masks_github_token():
    token = "gho_abcdefghijklmnopqrstuvwxyz012345"
    redacted, _ = prepare_message(f"login failed with {token}")
    assert token not in redacted
    assert SECRET_MASK in redacted


def test_prepare_message_masks_openai_and_gitlab_tokens():
    sk = "sk-abcdefghijklmnopqrstuvwxyz0123456789"
    glpat = "glpat-abcdefghijklmnopqrstuvwxyz"
    ghs = "ghs_abcdefghijklmnopqrstuvwxyz012345"
    redacted, _ = prepare_message(f"{sk} {glpat} {ghs}")
    assert sk not in redacted
    assert glpat not in redacted
    assert ghs not in redacted


def test_prepare_message_masks_jwt():
    jwt = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiIxMjM0NTY3ODkwIn0."
        "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    )
    redacted, _ = prepare_message(f"auth header {jwt}")
    assert jwt not in redacted
    assert SECRET_MASK in redacted


def test_prepare_message_masks_pem_block():
    pem = (
        "-----BEGIN PRIVATE KEY-----\n"
        "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n"
        "-----END PRIVATE KEY-----"
    )
    redacted, _ = prepare_message(f"key:\n{pem}\nafter")
    assert "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7" not in redacted
    assert "key:" in redacted
    assert "after" in redacted


def test_prepare_message_masks_unclosed_pem_block():
    body = "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7"
    redacted, _ = prepare_message(
        f"note before\n-----BEGIN PRIVATE KEY-----\n{body}\nstill the key"
    )
    assert body not in redacted
    assert "BEGIN PRIVATE KEY" not in redacted
    assert "note before" in redacted
    assert "still the key" not in redacted


def test_prepare_message_strips_ansi_and_controls():
    redacted, _ = prepare_message("hello\x1b[31m red\x1b[0m\x07 world")
    assert "\x1b" not in redacted
    assert "\x07" not in redacted
    assert "hello" in redacted
    assert "world" in redacted


def test_prepare_message_caps_utf8_bytes():
    # 2-byte chars so a char-count cap would be wrong
    payload = "é" * (MAX_MESSAGE_BYTES // 2 + 50)
    redacted, truncated = prepare_message(payload)
    assert truncated is True
    assert len(redacted.encode("utf-8")) <= MAX_MESSAGE_BYTES


def test_prepare_message_empty_after_strip_stays_empty():
    redacted, truncated = prepare_message("   \n\t")
    assert redacted == ""
    assert truncated is False


def test_prepare_message_masks_token_split_by_controls():
    token = "gho_abcdefghijklmnopqrstuvwxyz012345"
    redacted, _ = prepare_message("gh\x00o_" + token[4:])
    assert "gho_" not in redacted
    assert SECRET_MASK in redacted


def test_prepare_message_masks_token_split_by_ansi():
    token = "gho_abcdefghijklmnopqrstuvwxyz012345"
    redacted, _ = prepare_message("gho_\x1b[0m" + token[4:])
    assert token not in redacted
    assert SECRET_MASK in redacted


def test_prepare_message_masks_pem_before_byte_cap():
    body = "A" * (MAX_MESSAGE_BYTES + 200)
    pem = f"-----BEGIN PRIVATE KEY-----\n{body}\n-----END PRIVATE KEY-----"
    redacted, truncated = prepare_message(pem)
    assert "BEGIN PRIVATE KEY" not in redacted
    assert body[:40] not in redacted
    assert truncated is False
    assert SECRET_MASK in redacted


def test_prepare_message_strips_osc_before_mask():
    token = "gho_abcdefghijklmnopqrstuvwxyz012345"
    redacted, _ = prepare_message("gho_\x1b]0;x\x07" + token[4:])
    assert token not in redacted
    assert SECRET_MASK in redacted


def test_prepare_message_masks_ghp_token():
    token = "ghp_abcdefghijklmnopqrstuvwxyz012345"
    redacted, _ = prepare_message(f"login failed with {token}")
    assert token not in redacted
    assert SECRET_MASK in redacted


def test_prepare_message_masks_password_keyword():
    redacted, _ = prepare_message("I ran snow sql with password=Hunter2Hunter2")
    assert "Hunter2Hunter2" not in redacted


def test_prepare_message_detector_error_masks_whole_note():
    from unittest import mock

    fake = mock.Mock()
    fake.masked_text = None
    fake.error_str = "boom"
    with mock.patch(
        "snowflake.connector.secret_detector.SecretDetector.mask_secrets",
        return_value=fake,
    ):
        redacted, truncated = prepare_message("keep this secret out")
    assert redacted == SECRET_MASK
    assert truncated is False


def test_prepare_message_masks_whole_note_when_secret_detector_missing(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _blocked(name, global_ns=None, local_ns=None, fromlist=(), level=0):
        if name == "snowflake.connector.secret_detector":
            raise ModuleNotFoundError(
                "No module named 'snowflake.connector.secret_detector'"
            )
        return real_import(name, global_ns, local_ns, fromlist, level)

    token = "gho_abcdefghijklmnopqrstuvwxyz012345"
    monkeypatch.setattr(builtins, "__import__", _blocked)
    redacted, truncated = prepare_message(f"login failed with {token}")
    assert redacted == SECRET_MASK
    assert token not in redacted
    assert truncated is False


def test_redact_does_not_import_secret_detector():
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[2]
        / "src/snowflake/cli/_plugins/feedback/redact.py"
    ).read_text(encoding="utf-8")
    assert "secret_detector" not in source
    assert "SecretDetector" not in source
