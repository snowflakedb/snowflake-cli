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

from __future__ import annotations

import locale
import logging
import os
import sys
from typing import Dict, Optional

log = logging.getLogger(__name__)


def get_file_io_encoding() -> Optional[str]:
    """Get configured file I/O encoding, or None for platform default.

    Resolution order:
    1. SNOWFLAKE_CLI_ENCODING_FILE_IO environment variable
    2. config.toml [cli.encoding] file_io value
    3. None (use platform default)
    """
    env_encoding = os.environ.get("SNOWFLAKE_CLI_ENCODING_FILE_IO")
    if env_encoding:
        return env_encoding

    try:
        from snowflake.cli.api.config import get_config_value

        return get_config_value("cli", "encoding", key="file_io")
    except Exception:
        return None


def get_subprocess_encoding() -> Optional[str]:
    """Get configured subprocess encoding, or None for platform default.

    Resolution order:
    1. SNOWFLAKE_CLI_ENCODING_SUBPROCESS environment variable
    2. config.toml [cli.encoding] subprocess value
    3. None (use platform default)
    """
    env_encoding = os.environ.get("SNOWFLAKE_CLI_ENCODING_SUBPROCESS")
    if env_encoding:
        return env_encoding

    try:
        from snowflake.cli.api.config import get_config_value

        return get_config_value("cli", "encoding", key="subprocess")
    except Exception:
        return None


def detect_encoding_environment() -> Dict[str, str]:
    """Detect and log encoding environment information.

    Returns dict with filesystem, default, and locale encoding values.
    Logs a warning if encodings are inconsistent.
    """
    env_info = {
        "filesystem": sys.getfilesystemencoding(),
        "default": sys.getdefaultencoding(),
        "locale": locale.getpreferredencoding(),
    }

    encodings = {v.lower().replace("-", "") for v in env_info.values()}
    if len(encodings) > 1:
        log.warning(
            "Encoding mismatch detected: filesystem=%s, default=%s, locale=%s. "
            "Set SNOWFLAKE_CLI_ENCODING_FILE_IO=utf-8 for consistency.",
            env_info["filesystem"],
            env_info["default"],
            env_info["locale"],
        )

    configured = get_file_io_encoding()
    if configured:
        env_info["configured"] = configured

    return env_info
