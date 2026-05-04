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

from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli.api import secure_utils
from snowflake.cli.api.secure_utils import (
    _BUILTIN_ADMINISTRATORS_FALLBACK_NAME,
    _BUILTIN_ADMINISTRATORS_SID,
    _LOCAL_SYSTEM_FALLBACK_NAME,
    _LOCAL_SYSTEM_SID,
    _get_windows_whitelisted_users,
    windows_get_not_whitelisted_users_with_access,
)


@pytest.fixture
def fake_getlogin():
    with mock.patch.object(secure_utils, "_get_windows_whitelisted_users") as _:
        pass
    # Actually just patch os.getlogin via a fresh patcher each test.
    with mock.patch("os.getlogin", return_value="testuser") as m:
        yield m


def _patch_sid_lookup(mapping):
    """Patch ``_lookup_windows_sid_account_name`` to return names from a dict.

    Unmapped SIDs resolve to ``None`` so the production fallback path runs.
    """
    return mock.patch.object(
        secure_utils,
        "_lookup_windows_sid_account_name",
        side_effect=lambda sid: mapping.get(sid),
    )


def test_whitelisted_users_includes_english_names_on_english_windows(fake_getlogin):
    """On English Windows, resolved names match the English fallbacks, and the
    whitelist contains the expected set with no duplicates."""
    mapping = {
        _LOCAL_SYSTEM_SID: "SYSTEM",
        _BUILTIN_ADMINISTRATORS_SID: "Administrators",
    }
    with _patch_sid_lookup(mapping):
        users = _get_windows_whitelisted_users()

    assert "SYSTEM" in users
    assert "Administrators" in users
    assert "Network" in users
    assert "Domain Admins" in users
    assert "Domain Users" in users
    assert "testuser" in users
    # No duplicates: the English fallbacks should not be appended a second time
    # when the resolved names already match.
    assert users.count("SYSTEM") == 1
    assert users.count("Administrators") == 1


def test_whitelisted_users_includes_localized_names_on_french_windows(fake_getlogin):
    """On French Windows, the localized names are whitelisted alongside the
    English fallbacks so mixed-language inventories still pass."""
    mapping = {
        _LOCAL_SYSTEM_SID: "Système",
        _BUILTIN_ADMINISTRATORS_SID: "Administrateurs",
    }
    with _patch_sid_lookup(mapping):
        users = _get_windows_whitelisted_users()

    assert "Système" in users
    assert "Administrateurs" in users
    # English fallbacks are preserved to avoid regressing mixed-language setups.
    assert "SYSTEM" in users
    assert "Administrators" in users
    assert "testuser" in users


def test_whitelisted_users_falls_back_when_sid_lookup_fails(fake_getlogin):
    """If the SID cannot be resolved (API failure, non-Windows host), fall
    back to the English names so the whitelist never ends up empty."""
    with _patch_sid_lookup({}):  # every lookup returns None
        users = _get_windows_whitelisted_users()

    assert _LOCAL_SYSTEM_FALLBACK_NAME in users
    assert _BUILTIN_ADMINISTRATORS_FALLBACK_NAME in users


def test_not_whitelisted_users_on_french_windows_does_not_flag_localized_admins(
    fake_getlogin,
):
    """Regression test for SNOW-3018675 / #2743: on French Windows, the
    ``Administrateurs`` group reported by ``icacls`` must not appear in the
    unauthorized-users list."""
    icacls_output = (
        r"C:\Users\testuser\.snowflake\config.toml AUTORITE NT\Système:(I)(F)"
        "\n"
        r"                                         BUILTIN\Administrateurs:(I)(F)"
        "\n"
        r"                                         DESKTOP-ABC\testuser:(I)(F)"
        "\n"
    )
    mapping = {
        _LOCAL_SYSTEM_SID: "Système",
        _BUILTIN_ADMINISTRATORS_SID: "Administrateurs",
    }
    with _patch_sid_lookup(mapping), mock.patch.object(
        secure_utils, "_run_icacls", return_value=icacls_output
    ):
        result = windows_get_not_whitelisted_users_with_access(
            Path(r"C:\Users\testuser\.snowflake\config.toml")
        )

    assert result == []


def test_not_whitelisted_users_on_english_windows_unchanged(fake_getlogin):
    """Pre-existing English-Windows behavior must still pass."""
    icacls_output = (
        r"C:\Users\testuser\.snowflake\config.toml NT AUTHORITY\SYSTEM:(I)(F)"
        "\n"
        r"                                         BUILTIN\Administrators:(I)(F)"
        "\n"
        r"                                         DESKTOP-ABC\testuser:(I)(F)"
        "\n"
    )
    mapping = {
        _LOCAL_SYSTEM_SID: "SYSTEM",
        _BUILTIN_ADMINISTRATORS_SID: "Administrators",
    }
    with _patch_sid_lookup(mapping), mock.patch.object(
        secure_utils, "_run_icacls", return_value=icacls_output
    ):
        result = windows_get_not_whitelisted_users_with_access(
            Path(r"C:\Users\testuser\.snowflake\config.toml")
        )

    assert result == []


def test_not_whitelisted_users_still_detects_unauthorized_user(fake_getlogin):
    """A genuinely unauthorized user must still surface in the warning list."""
    icacls_output = (
        r"C:\Users\testuser\.snowflake\config.toml NT AUTHORITY\SYSTEM:(I)(F)"
        "\n"
        r"                                         BUILTIN\Administrators:(I)(F)"
        "\n"
        r"                                         DESKTOP-ABC\attacker:(I)(F)"
        "\n"
    )
    mapping = {
        _LOCAL_SYSTEM_SID: "SYSTEM",
        _BUILTIN_ADMINISTRATORS_SID: "Administrators",
    }
    with _patch_sid_lookup(mapping), mock.patch.object(
        secure_utils, "_run_icacls", return_value=icacls_output
    ):
        result = windows_get_not_whitelisted_users_with_access(
            Path(r"C:\Users\testuser\.snowflake\config.toml")
        )

    assert result == ["attacker"]


def test_sid_lookup_returns_none_on_non_windows():
    """On non-Windows hosts, ``_lookup_windows_sid_account_name`` must return
    ``None`` without raising (ctypes.WinDLL is not available)."""
    # Call the real implementation — on Linux CI, WinDLL does not exist, so
    # the helper should return None rather than propagate the AttributeError.
    import sys

    if sys.platform == "win32":
        pytest.skip("This test covers non-Windows fallback behavior.")

    assert secure_utils._lookup_windows_sid_account_name(_LOCAL_SYSTEM_SID) is None
