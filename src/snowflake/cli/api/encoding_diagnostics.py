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

"""Encoding diagnostics: the report behind ``snow helpers detect-encoding``
and the startup encoding warning.
"""

from __future__ import annotations

import codecs
import locale
import sys
import warnings

from snowflake.cli.api.windows_console_encoding import (
    _console_needs_utf8_configuration,
)

_CONSOLE_ENCODING_DOCS_URL = (
    "https://docs.snowflake.com/en/developer-guide/snowflake-cli/"
    "connecting/configure-cli#additional-step-for-windows-powershell-5x"
)
_CHCP_FIX_INSTRUCTION = (
    "run 'chcp.com 65001' to switch the console to the UTF-8 codepage"
)
_CONSOLE_ENCODING_WARNING = (
    "Console is not configured for UTF-8 output. If you're using cmd.exe or "
    f"Git Bash, {_CHCP_FIX_INSTRUCTION}. If you're using PowerShell 5.x, see: "
    f"{_CONSOLE_ENCODING_DOCS_URL}"
)
_CONSOLE_ENCODING_REMEDIATION = (
    f"- cmd.exe or Git Bash: {_CHCP_FIX_INSTRUCTION}\n"
    f"- PowerShell 5.x: see {_CONSOLE_ENCODING_DOCS_URL}"
)
_CONSOLE_NEEDS_CONFIG_MESSAGE = (
    "CLI encoding is correctly configured. However, your console is not configured for UTF-8 output:\n"
    f"{_CONSOLE_ENCODING_REMEDIATION}"
)
_ADDITIONAL_CONSOLE_NOTE = (
    "\nAdditionally, your console is not configured for UTF-8 output:\n"
    f"{_CONSOLE_ENCODING_REMEDIATION}"
)
_NO_ENCODING_ISSUES_MESSAGE = "No encoding issues - your system is properly configured."


def _canonical_encoding(enc: str) -> str:
    """Return the canonical codec name for enc.

    Routes through codecs.lookup so that aliases such as 'utf8', 'UTF_8', or
    'u8' all resolve to 'utf-8', keeping mismatch detection consistent with
    how _validate_encoding works.  Falls back to simple lower/replace for
    unrecognised strings so detection never crashes on exotic platform values.
    """
    try:
        return codecs.lookup(enc).name
    except LookupError:
        return enc.lower().replace("_", "-")


def get_encoding_diagnostics(*, all_cli_encodings_configured: bool) -> str:
    """Return a detailed encoding diagnostics report for the current environment.

    Used by ``snow helpers detect-encoding`` to give the user actionable detail
    about the encoding setup. Always reports a misconfigured console,
    regardless of whether the Python encodings are otherwise clean — unlike
    :func:`detect_encoding_environment`, whose startup warning only mentions
    the console when it isn't already warning about a Python encoding
    mismatch.
    """
    fs_encoding = _canonical_encoding(sys.getfilesystemencoding())
    default_encoding = _canonical_encoding(sys.getdefaultencoding())
    locale_encoding = _canonical_encoding(locale.getpreferredencoding())

    encodings = {fs_encoding, default_encoding, locale_encoding}

    actionable_section = (
        "This may cause file corruption when sharing projects across platforms.\n"
        "Recommended actions:\n"
        "1. Set environment variable: PYTHONUTF8=1\n"
        "2. Configure encoding in config.toml:\n"
        "   [cli.encoding]\n"
        '   file_io = "utf-8"\n'
        '   subprocess = "utf-8"\n'
        '   stdout = "utf-8"\n'
        "3. Set environment variables: SNOWFLAKE_CLI_ENCODING_FILE_IO='utf-8', "
        "SNOWFLAKE_CLI_ENCODING_SUBPROCESS='utf-8', "
        "and SNOWFLAKE_CLI_ENCODING_STDOUT='utf-8'"
    )

    console_needs_utf8_configuration = _console_needs_utf8_configuration()

    if all_cli_encodings_configured:
        if console_needs_utf8_configuration:
            return _CONSOLE_NEEDS_CONFIG_MESSAGE
        return _NO_ENCODING_ISSUES_MESSAGE

    console_section = (
        _ADDITIONAL_CONSOLE_NOTE if console_needs_utf8_configuration else ""
    )

    if len(encodings) > 1:
        return (
            f"Encoding mismatch detected:\n"
            f"  Filesystem: {fs_encoding}\n"
            f"  Default:    {default_encoding}\n"
            f"  Locale:     {locale_encoding}\n"
            f"\n{actionable_section}"
            f"{console_section}"
        )

    if locale_encoding != "utf-8":
        return (
            f"Platform encoding is {locale_encoding}, not utf-8.\n"
            f"\n{actionable_section}"
            f"{console_section}"
        )

    if console_needs_utf8_configuration:
        return _CONSOLE_NEEDS_CONFIG_MESSAGE

    return _NO_ENCODING_ISSUES_MESSAGE


def detect_encoding_environment(*, all_cli_encodings_configured: bool) -> None:
    """Detect encoding configuration and warn about mismatches"""
    fs_encoding = _canonical_encoding(sys.getfilesystemencoding())
    default_encoding = _canonical_encoding(sys.getdefaultencoding())
    locale_encoding = _canonical_encoding(locale.getpreferredencoding())

    # Warn on mismatches
    encodings = {fs_encoding, default_encoding, locale_encoding}

    console_needs_utf8_configuration = _console_needs_utf8_configuration()

    # if all encoding options are configured we assume the user knows what they are doing
    if all_cli_encodings_configured:
        if console_needs_utf8_configuration:
            warnings.warn(_CONSOLE_ENCODING_WARNING)
        return
    if len(encodings) > 1 or locale_encoding != "utf-8":
        msg = (
            "Encoding mismatch detected. "
            "Run 'snow helpers detect-encoding' for more details."
        )
        warnings.warn(msg)
    elif console_needs_utf8_configuration:
        warnings.warn(_CONSOLE_ENCODING_WARNING)
