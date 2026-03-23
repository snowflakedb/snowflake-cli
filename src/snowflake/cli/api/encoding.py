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

import codecs
import logging
import os
from typing import Optional

from snowflake.cli.api.config import ENCODING_SECTION_PATH, get_config_value
from snowflake.cli.api.exceptions import CliError
from snowflake.connector.errors import ConfigSourceError, MissingConfigOptionError
from tomlkit.exceptions import NonExistentKey

log = logging.getLogger(__name__)

_MISSING_ENCODING_EXCEPTIONS = (
    KeyError,
    NonExistentKey,
    MissingConfigOptionError,
    ConfigSourceError,
)


def _validate_encoding(encoding: str, source: str) -> None:
    """Raise CliError if *encoding* is not a known codec name.

    *source* is included in the error message to help the user locate the
    bad value (e.g. the env var name or the config key path).
    """
    try:
        codecs.lookup(encoding)
    except LookupError:
        raise CliError(
            f"Unknown encoding '{encoding}'. "
            f"Check {source} or [cli.encoding] file_io / subprocess in config.toml."
        )


def get_file_io_encoding() -> Optional[str]:
    """Get configured file I/O encoding, or None for platform default.

    Resolution order:
    1. SNOWFLAKE_CLI_ENCODING_FILE_IO environment variable
    2. config.toml [cli.encoding] file_io value
    3. None (use platform default)
    """
    env_encoding = os.environ.get("SNOWFLAKE_CLI_ENCODING_FILE_IO")
    if env_encoding:
        _validate_encoding(env_encoding, "SNOWFLAKE_CLI_ENCODING_FILE_IO")
        return env_encoding

    try:
        value = get_config_value(*ENCODING_SECTION_PATH, key="file_io")
    except _MISSING_ENCODING_EXCEPTIONS:
        log.debug("No file_io encoding in config.toml; using platform default.")
        return None

    if value is not None:
        _validate_encoding(value, "[cli.encoding] file_io")
    return value


def get_subprocess_encoding() -> Optional[str]:
    """Get configured subprocess encoding, or None for platform default.

    Resolution order:
    1. SNOWFLAKE_CLI_ENCODING_SUBPROCESS environment variable
    2. config.toml [cli.encoding] subprocess value
    3. None (use platform default)
    """
    env_encoding = os.environ.get("SNOWFLAKE_CLI_ENCODING_SUBPROCESS")
    if env_encoding:
        _validate_encoding(env_encoding, "SNOWFLAKE_CLI_ENCODING_SUBPROCESS")
        return env_encoding

    try:
        value = get_config_value(*ENCODING_SECTION_PATH, key="subprocess")
    except _MISSING_ENCODING_EXCEPTIONS:
        log.debug("No subprocess encoding in config.toml; using platform default.")
        return None

    if value is not None:
        _validate_encoding(value, "[cli.encoding] subprocess")
    return value
