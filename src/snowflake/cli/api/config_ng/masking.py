"""Utilities for masking sensitive configuration values."""

from __future__ import annotations

from typing import Any, Final, Literal, Tuple

MaskToken = Literal["****"]

SENSITIVE_KEY_FRAGMENT = Literal[
    "password",
    "pwd",
    "oauth_client_secret",
    "token",
    "session_token",
    "master_token",
    "mfa_passcode",
    "private_key",
    "passphrase",
    "secret",
]

PATH_KEY_FRAGMENT = Literal[
    "private_key_file",
    "private_key_path",
    "token_file_path",
]

MASKED_VALUE: Final[MaskToken] = "****"

SENSITIVE_KEYS: Final[Tuple[SENSITIVE_KEY_FRAGMENT, ...]] = (
    "password",
    "pwd",
    "oauth_client_secret",
    "token",
    "session_token",
    "master_token",
    "mfa_passcode",
    "private_key",
    "passphrase",
    "secret",
)

PATH_KEYS: Final[Tuple[PATH_KEY_FRAGMENT, ...]] = (
    "private_key_file",
    "private_key_path",
    "token_file_path",
)


def should_mask_value(key: str) -> bool:
    """
    Determine if the value associated with the key is sensitive.

    Keys containing path segments should not be masked because they refer to
    file locations rather than secrets.
    """
    key_lower = key.lower()

    if any(path_fragment in key_lower for path_fragment in PATH_KEYS):
        return False

    return any(fragment in key_lower for fragment in SENSITIVE_KEYS)


def mask_sensitive_value(key: str, value: Any) -> Any:
    """Mask sensitive values; otherwise return the original value."""
    if should_mask_value(key):
        return MASKED_VALUE

    return value


def stringify_masked_value(key: str, value: Any) -> str:
    """
    Helper for presentation components that expect string values.

    This avoids duplicating string coercion logic across call sites.
    """
    masked = mask_sensitive_value(key, value)
    return "None" if masked is None else str(masked)
