from typing import Literal, Optional

from snowflake.cli.api.config import get_config_bool_value, get_config_value

# TODO: add validation
EncodingName = Literal["utf-8", "utf-16", "cp1252", "cp932", "cp936", "ascii"]


def get_file_io_encoding() -> Optional[str]:
    """
    Get configured file I/O encoding, or None for platform default.

    Returns None when not configured - this ensures Unix users with proper
    locales experience NO behavior change (platform default is used).
    """
    # default is None = use platform default (transparent for Unix users)
    return get_config_value("cli", "encoding", key="file_io", default=None)


def get_subprocess_encoding() -> Optional[str]:
    """Get configured subprocess encoding, or None for platform default"""
    # default is None = use platform default (transparent for Unix users)
    return get_config_value("cli", "encoding", key="subprocess", default=None)


def should_show_warnings() -> bool:
    """Whether to show encoding warnings"""
    return get_config_bool_value("cli", "encoding", key="show_warnings", default=True)


def is_strict_mode() -> bool:
    """Whether to use strict error handling"""
    return get_config_bool_value("cli", "encoding", key="strict", default=False)
