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

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from snowflake.cli.api.constants import IS_WINDOWS
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secure_utils import (
    _decode_windows_tool_output,
    _get_windows_username,
    _get_windows_whitelisted_users,
    _icacls,
    _lookup_windows_account_name,
    _windows_account_name,
    _windows_restrict_file_permissions,
    get_windows_permission_warning,
    windows_get_not_whitelisted_users_with_access,
)

# On Linux, Path treats the whole string as a single path component, which is fine —
# we only call str() on it and the mock output is built from the same value.
_FILE_PATH = Path(r"C:\Users\bob\.snowflake\config.toml")

# Mirrors _get_windows_whitelisted_users() with the current user resolved to "bob".
_WHITELISTED = [
    "SYSTEM",
    "Administrators",
    "Administrator",
    "Domain Admins",
    "bob",
]


def _mock_username(name: str = r"CONTOSO\bob"):
    # _get_windows_whitelisted_users() shells out to whoami for the current user, so
    # tests exercising the real whitelist have to stub it out.
    return patch(
        "snowflake.cli.api.secure_utils._get_windows_username", return_value=name
    )


def _mock_account_names(system: str = "SYSTEM", administrators: str = "Administrators"):
    mapping = {
        "S-1-5-18": system,
        "S-1-5-32-544": administrators,
    }
    return patch(
        "snowflake.cli.api.secure_utils._lookup_windows_account_name",
        side_effect=lambda sid: mapping.get(sid),
    )


def _mock_icacls(output: str):
    return patch("snowflake.cli.api.secure_utils._icacls", return_value=output)


def _mock_whitelist(users=None):
    return patch(
        "snowflake.cli.api.secure_utils._get_windows_whitelisted_users",
        return_value=users if users is not None else _WHITELISTED,
    )


def _build_icacls_output(*aces: str) -> str:
    """
    Build realistic icacls output: first ACE on the file-path line, subsequent ones
    indented by the same number of spaces as the filepath+space prefix.
    """
    prefix = f"{_FILE_PATH} "
    indent = " " * len(prefix)
    lines = [f"{prefix}{aces[0]}"]
    for ace in aces[1:]:
        lines.append(f"{indent}{ace}")
    lines += ["", "Successfully processed 1 files; Failed processing 0 files"]
    return "\n".join(lines)


@pytest.fixture()
def clear_username_cache():
    _get_windows_username.cache_clear()
    _lookup_windows_account_name.cache_clear()
    yield
    _get_windows_username.cache_clear()
    _lookup_windows_account_name.cache_clear()


@pytest.mark.parametrize(
    "exc, match",
    [
        pytest.param(
            FileNotFoundError("whoami not found"), "not found", id="not_found"
        ),
        pytest.param(
            subprocess.CalledProcessError(1, "whoami.exe"),
            "non-zero",
            id="nonzero_exit",
        ),
        pytest.param(
            subprocess.TimeoutExpired("whoami.exe", 10), "timed out", id="timeout"
        ),
    ],
)
def test_get_windows_username_raises_on_whoami_failure(
    clear_username_cache, exc, match
):
    with patch("subprocess.run", side_effect=exc):
        with pytest.raises(CliError, match=match):
            _get_windows_username()


def test_icacls_raises_cli_error_on_timeout():
    with patch(
        "subprocess.run",
        side_effect=subprocess.TimeoutExpired("icacls.exe", 10),
    ):
        with pytest.raises(CliError, match="timed out"):
            _icacls(_FILE_PATH)


def test_icacls_raises_cli_error_when_not_found():
    with patch(
        "subprocess.run",
        side_effect=FileNotFoundError("icacls.exe not found"),
    ):
        with pytest.raises(CliError, match="not found"):
            _icacls(_FILE_PATH)


def test_icacls_raises_cli_error_on_nonzero_exit():
    with patch(
        "subprocess.run",
        return_value=MagicMock(returncode=1, stderr=b"Access denied"),
    ):
        with pytest.raises(CliError, match="Access denied"):
            _icacls(_FILE_PATH)


def test_icacls_strips_ansi_from_stderr():
    ansi_stderr = b"\x1B[31mAccess denied\x1B[0m"
    with patch(
        "subprocess.run",
        return_value=MagicMock(returncode=1, stderr=ansi_stderr),
    ):
        with pytest.raises(CliError) as exc_info:
            _icacls(_FILE_PATH)
    assert "\x1B" not in str(exc_info.value)
    assert "Access denied" in str(exc_info.value)


def test_decode_windows_tool_output_oem_eacute_rejected_by_cp1252():
    # French icacls prints AUTORITÉ; É is byte 0x90 in OEM cp850 and undefined in cp1252.
    raw = "AUTORITÉ NT\\Système".encode("cp850")
    assert b"\x90" in raw
    with pytest.raises(UnicodeDecodeError):
        raw.decode("cp1252")
    assert _decode_windows_tool_output(raw, encoding="cp850") == "AUTORITÉ NT\\Système"


def test_decode_windows_tool_output_replaces_undefined_bytes():
    assert _decode_windows_tool_output(b"\x90", encoding="cp1252") == "\ufffd"
    assert _decode_windows_tool_output(None) == ""


def test_icacls_decodes_piped_oem_output():
    raw = f"{_FILE_PATH} AUTORITÉ NT\\Système:(F)\r\n".encode("cp850")
    completed = subprocess.CompletedProcess(["icacls"], 0, stdout=raw, stderr=b"")
    with (
        patch(
            "snowflake.cli.api.secure_utils._windows_oem_encoding",
            return_value="cp850",
        ),
        patch("subprocess.run", return_value=completed) as mock_run,
    ):
        output = _icacls(_FILE_PATH)
    assert "AUTORITÉ NT\\Système:(F)" in output
    assert mock_run.call_args.kwargs.get("text") is None
    assert mock_run.call_args.kwargs.get("encoding") is None


def test_icacls_none_stdout_does_not_raise():
    with patch(
        "subprocess.run",
        return_value=MagicMock(returncode=0, stdout=None, stderr=None),
    ):
        assert _icacls(_FILE_PATH) == ""


def test_localized_system_and_administrators_stay_whitelisted():
    # French icacls prints names, not SIDs. Everyone, Domain Users, and Network
    # stay unauthorized. icacls has no /sid switch.
    output = _build_icacls_output(
        "AUTORITÉ NT\\Système:(F)",
        "BUILTIN\\Administrateurs:(F)",
        "Everyone:(R)",
        "CONTOSO\\Domain Users:(RX)",
        "NT AUTHORITY\\NETWORK:(R)",
    )
    raw = output.encode("cp850")
    completed = subprocess.CompletedProcess(["icacls"], 0, stdout=raw, stderr=b"")
    with (
        _mock_username(),
        _mock_account_names(system="Système", administrators="Administrateurs"),
        patch(
            "snowflake.cli.api.secure_utils._windows_oem_encoding",
            return_value="cp850",
        ),
        patch("subprocess.run", return_value=completed) as mock_run,
    ):
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Domain Users", "Everyone", "NETWORK"]
    assert "/sid" not in mock_run.call_args.args[0]
    assert mock_run.call_args.args[0][1:] == [str(_FILE_PATH)]


def test_whitelist_uses_lookup_names_not_english_or_sids():
    with (
        _mock_username(),
        _mock_account_names(system="Système", administrators="Administrateurs"),
    ):
        whitelisted = _get_windows_whitelisted_users()
    assert "Système" in whitelisted
    assert "Administrateurs" in whitelisted
    assert "SYSTEM" not in whitelisted
    assert "Administrators" not in whitelisted
    assert "S-1-5-18" not in whitelisted


def test_account_name_falls_back_when_lookup_returns_none():
    with patch(
        "snowflake.cli.api.secure_utils._lookup_windows_account_name",
        return_value=None,
    ):
        assert _windows_account_name("S-1-5-18") == "SYSTEM"
        assert _windows_account_name("S-1-5-32-544") == "Administrators"


@pytest.mark.skipif(IS_WINDOWS, reason="LookupAccountSid is available on Windows")
def test_lookup_windows_account_name_is_none_off_windows(clear_username_cache):
    assert _lookup_windows_account_name("S-1-5-18") is None


@pytest.mark.skipif(not IS_WINDOWS, reason="requires LookupAccountSid")
def test_lookup_windows_account_name_resolves_well_known_sids(clear_username_cache):
    system = _lookup_windows_account_name("S-1-5-18")
    administrators = _lookup_windows_account_name("S-1-5-32-544")
    assert system
    assert administrators
    assert not system.upper().startswith("S-1-")
    assert not administrators.upper().startswith("S-1-")


def test_get_windows_username_decodes_oem(clear_username_cache):
    raw = "CONTOSO\\Élodie\r\n".encode("cp850")
    completed = subprocess.CompletedProcess(["whoami"], 0, stdout=raw, stderr=b"")
    with (
        patch(
            "snowflake.cli.api.secure_utils._windows_oem_encoding",
            return_value="cp850",
        ),
        patch("subprocess.run", return_value=completed),
    ):
        assert _get_windows_username() == "CONTOSO\\Élodie"


def test_windows_restrict_file_permissions_raises_on_icacls_failure(tmp_path):
    test_file = tmp_path / "config.toml"
    test_file.touch()
    with _mock_username(), patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stderr=b"Access denied")
        with pytest.raises(CliError, match="Failed to set strict permissions"):
            _windows_restrict_file_permissions(test_file)


def test_restrict_file_permissions_remove_acl_raises_on_failure(tmp_path):
    test_file = tmp_path / "config.toml"
    test_file.touch()
    with (
        _mock_username(),
        patch(
            "snowflake.cli.api.secure_utils.windows_get_not_whitelisted_users_with_access",
            return_value=["Everyone"],
        ),
        patch("subprocess.run") as mock_run,
    ):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout=b"", stderr=b""),  # /inheritance:r /grant
            MagicMock(returncode=1, stderr=b"Access denied"),  # /remove:g
        ]
        with pytest.raises(CliError, match="Failed to remove ACE for Everyone"):
            _windows_restrict_file_permissions(test_file)


@pytest.mark.skipif(not IS_WINDOWS, reason="requires icacls.exe")
def test_restrict_file_permissions_does_not_duplicate_ace(tmp_path):
    test_file = tmp_path / "config.toml"
    test_file.touch()

    _windows_restrict_file_permissions(test_file)
    _windows_restrict_file_permissions(test_file)

    username = _get_windows_username()
    short_name = username.rsplit("\\", 1)[-1]
    output = _icacls(test_file)
    # /grant:r replaces an existing ACE; /grant would append a duplicate.
    # Both 'DOMAIN\bob:(F)' and plain 'bob:(F)' forms contain 'bob:('.
    assert output.count(f"{short_name}:(") == 1


def test_everyone_on_continuation_line_is_detected():
    """
    icacls renders well-known SIDs without a domain prefix on indented continuation
    lines — e.g. 'Everyone:(R)' with no 'DOMAIN\\' before it.  The parser must
    recognise these bare entries even though they have no backslash.
    """
    output = _build_icacls_output(
        r"BUILTIN\Administrators:(F)",
        r"NT AUTHORITY\SYSTEM:(F)",
        "Everyone:(R)",
    )
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Everyone"]


def test_creator_owner_on_continuation_line_is_detected():
    # CREATOR OWNER has an embedded space and no domain prefix — same bare-SID
    # format as Everyone but with a multi-word name.
    output = _build_icacls_output(
        r"BUILTIN\Administrators:(F)",
        "CREATOR OWNER:(OI)(CI)(IO)(F)",
    )
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["CREATOR OWNER"]


def test_everyone_on_filepath_line_captured_correctly():
    # icacls places the first ACE on the same line as the file path.
    # The parser must extract just 'Everyone', not the path fragment before it.
    output = (
        f"{_FILE_PATH} Everyone:(R)\n\n"
        "Successfully processed 1 files; Failed processing 0 files"
    )
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Everyone"]


def test_whitelisted_builtins_not_returned():
    output = _build_icacls_output(
        r"BUILTIN\Administrators:(F)",
        r"NT AUTHORITY\SYSTEM:(F)",
        r"bob:(F)",
    )
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == []


def test_deny_ace_not_counted_as_unauthorized_access():
    output = _build_icacls_output("Everyone:(DENY)")
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == []


def test_special_access_ace_is_conservatively_flagged():
    # icacls emits "(special access:)" when an ACE can't be summarised as a short
    # token. The continuation lines listing the actual permission names are not
    # parsed, so the ACE is treated as a grant and the user is flagged.
    output = _build_icacls_output(r"BUILTIN\Users:(I)(special access:)")
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Users"]


def test_deny_special_access_ace_not_counted_as_unauthorized_access():
    # A DENY token present alongside "special access" must still be detected now
    # that the regex captures the full line instead of stopping at lowercase.
    output = _build_icacls_output(r"BUILTIN\Users:(I)(DENY)(special access:)")
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == []


def test_authenticated_users_with_nt_authority_prefix_detected():
    # Unlike Everyone, Authenticated Users carries a 'NT AUTHORITY\\' prefix,
    # so it exercises the domain\\user code path rather than the bare-SID path.
    output = _build_icacls_output(
        r"BUILTIN\Administrators:(F)",
        r"NT AUTHORITY\Authenticated Users:(M)",
    )
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Authenticated Users"]


def test_mixed_whitelisted_and_unauthorized_users():
    output = _build_icacls_output(
        r"BUILTIN\Administrators:(F)",
        r"NT AUTHORITY\SYSTEM:(F)",
        r"CONTOSO\Alice:(R)",
    )
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Alice"]


def test_summary_line_not_parsed_as_ace():
    """The 'Successfully processed' trailing line must never yield a false positive."""
    output = "Successfully processed 1 files; Failed processing 0 files\n"
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == []


def test_unauthorized_users_are_returned_in_sorted_order():
    """
    The result is interpolated into the permission warning and drives the /remove:g
    loop in _windows_restrict_file_permissions, so its order must not depend on the
    hash seed.  Listed here in non-alphabetical order to pin the sort.
    """
    output = _build_icacls_output(
        r"CONTOSO\Charlie:(R)",
        "Everyone:(R)",
        r"CONTOSO\Alice:(R)",
    )
    with _mock_icacls(output), _mock_whitelist():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Alice", "Charlie", "Everyone"]


def test_broad_principals_are_not_whitelisted():
    """
    "Domain Users" and "Network" were dropped from the whitelist for SNOW-3649686.
    Re-adding either silently restores the finding, so pin their absence against the
    real whitelist rather than the _WHITELISTED stub.
    """
    with _mock_username(), _mock_account_names():
        whitelisted = _get_windows_whitelisted_users()

    assert "bob" in whitelisted, "current user must stay whitelisted"
    assert "SYSTEM" in whitelisted
    assert "Administrators" in whitelisted
    assert "S-1-5-18" not in whitelisted
    assert "S-1-5-32-544" not in whitelisted
    assert "Domain Users" not in whitelisted
    assert "Network" not in whitelisted
    assert "S-1-5-2" not in whitelisted


def test_domain_users_ace_is_reported():
    """
    The exact ACE from the SNOW-3649686 exploit narrative: a domain-joined host whose
    profile directory grants CONTOSO\\Domain Users read access. Uses the real whitelist,
    not the stub, so this fails if the entry comes back.
    """
    output = _build_icacls_output(
        r"BUILTIN\Administrators:(F)",
        r"NT AUTHORITY\SYSTEM:(F)",
        r"CONTOSO\Domain Users:(RX)",
    )
    with _mock_icacls(output), _mock_username(), _mock_account_names():
        result = windows_get_not_whitelisted_users_with_access(_FILE_PATH)
    assert result == ["Domain Users"]


@pytest.mark.parametrize(
    "whoami_output, icacls_ace, expected",
    [
        pytest.param(
            r"CONTOSO\bob",
            r"CONTOSO\Bob:(F)",
            [],
            id="whoami_lower_icacls_title_case",
        ),
        pytest.param(
            r"CONTOSO\BOB",
            r"CONTOSO\bob:(F)",
            [],
            id="whoami_upper_icacls_lower",
        ),
        pytest.param(
            r"CONTOSO\Bob",
            r"CONTOSO\BOB:(F)",
            [],
            id="whoami_title_icacls_upper",
        ),
        pytest.param(
            r"CONTOSO\bob",
            r"CONTOSO\Alice:(F)",
            ["Alice"],
            id="different_user_still_flagged",
        ),
    ],
)
def test_whitelist_check_is_case_insensitive(
    tmp_path, clear_username_cache, whoami_output, icacls_ace, expected
):
    # Windows identity matching is case-insensitive — the current user must not be
    # flagged as unauthorized regardless of which casing whoami vs icacls use.
    # The last case guards that a genuinely different user is still reported.
    config = tmp_path / "config.toml"
    with _mock_username(whoami_output), _mock_account_names():
        with _mock_icacls(_build_icacls_output_for_path(config, icacls_ace)):
            result = windows_get_not_whitelisted_users_with_access(config)
    assert result == expected


def _build_icacls_output_for_path(path: Path, *aces: str) -> str:
    prefix = f"{path} "
    indent = " " * len(prefix)
    lines = [f"{prefix}{aces[0]}"]
    for ace in aces[1:]:
        lines.append(f"{indent}{ace}")
    lines += ["", "Successfully processed 1 files; Failed processing 0 files"]
    return "\n".join(lines)


def test_get_windows_permission_warning_file(tmp_path):
    config = tmp_path / "config.toml"
    config.touch()
    output = _build_icacls_output_for_path(config, "Everyone:(R)")
    with _mock_icacls(output), _mock_username(r"CONTOSO\bob"), _mock_account_names():
        result = get_windows_permission_warning(config)
    assert " * Users with unauthorized access: Everyone" in result
    assert " * To restrict permissions, run:" in result
    assert "/inheritance:r /grant:r" in result
    assert r'CONTOSO\bob:(F)"' in result
    assert '/remove:g "Everyone"' in result
    assert "(OI)(CI)" not in result


def test_get_windows_permission_warning_directory(tmp_path):
    output = _build_icacls_output_for_path(tmp_path, "Everyone:(R)")
    with _mock_icacls(output), _mock_username(r"CONTOSO\bob"), _mock_account_names():
        result = get_windows_permission_warning(tmp_path)
    assert " * Users with unauthorized access: Everyone" in result
    assert " * To restrict permissions, run:" in result
    assert "/inheritance:r /grant:r" in result
    assert r'CONTOSO\bob:(OI)(CI)(F)"' in result
    assert '/remove:g "Everyone"' in result


def test_get_windows_permission_warning_strips_ansi(tmp_path):
    config = tmp_path / "config.toml"
    config.touch()
    ansi_user = "\x1B[31mEveryone\x1B[0m"
    output = _build_icacls_output_for_path(config, f"{ansi_user}:(R)")
    with (
        _mock_icacls(output),
        _mock_username("\x1B[32mCONTOSO\\bob\x1B[0m"),
        _mock_account_names(),
    ):
        result = get_windows_permission_warning(config)
    assert "\x1B" not in result
    assert "Everyone" in result
    assert r"CONTOSO\bob" in result
