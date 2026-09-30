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

import os
from typing import Iterable, Optional

from snowflake.cli.api.config import (
    AUTO_UPGRADE_ALLOW_MAJOR_KEY,
    AUTO_UPGRADE_KEY,
    CLI_SECTION,
    get_config_bool_value,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.utils.types import try_cast_to_bool

NO_AUTO_UPGRADE_FLAG = "--no-auto-upgrade"
AUTO_UPGRADE_DONE_ENV = "SNOWFLAKE_CLI_AUTO_UPGRADE_DONE"


def _env_is_true(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return False
    try:
        return try_cast_to_bool(raw) is True
    except ValueError:
        return False


def is_auto_upgrade_enabled() -> bool:
    """Whether the product opt-in is on for this process (excludes argv).

    Returns False when ``SNOWFLAKE_CLI_AUTO_UPGRADE_DONE=1``, when
    ``ENABLE_SNOW_AUTO_UPGRADE`` is off, or when env/config opt-in is off
    (``SNOWFLAKE_CLI_AUTO_UPGRADE`` overrides ``cli.auto_upgrade``).

    One-run off via ``--no-auto-upgrade`` is handled by ``skip_due_to_cli_flag()``.
    ``cli.ignore_new_version_warning`` does not affect this.
    """
    if _env_is_true(AUTO_UPGRADE_DONE_ENV):
        return False
    if not FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE.is_enabled():
        return False
    return (
        get_config_bool_value(CLI_SECTION, key=AUTO_UPGRADE_KEY, default=False) is True
    )


def allow_major() -> bool:
    """Whether auto-upgrade may cross a major version. Manual ``snow upgrade`` ignores this."""
    return (
        get_config_bool_value(
            CLI_SECTION, key=AUTO_UPGRADE_ALLOW_MAJOR_KEY, default=False
        )
        is True
    )


def skip_due_to_cli_flag(argv: Optional[Iterable[str]] = None) -> bool:
    """True when ``--no-auto-upgrade`` is present in argv (root one-run off).

    Does not read ``sys.argv``; callers must pass argv. ``None`` is empty.
    """
    tokens = list(argv) if argv is not None else []
    return NO_AUTO_UPGRADE_FLAG in tokens
