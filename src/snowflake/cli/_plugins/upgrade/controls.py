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

from typing import Iterable, Optional

from snowflake.cli.api.config import (
    AUTO_UPGRADE_ALLOW_MAJOR_KEY,
    AUTO_UPGRADE_KEY,
    CLI_SECTION,
    get_config_bool_value,
    is_air_gapped,
)
from snowflake.cli.api.feature_flags import FeatureFlag

NO_AUTO_UPGRADE_FLAG = "--no-auto-upgrade"
AUTO_UPGRADE_DONE_ENV = "SNOWFLAKE_CLI_AUTO_UPGRADE_DONE"


def is_auto_upgrade_enabled() -> bool:
    """Whether the product opt-in is on (excludes argv and the spawn latch).

    Returns False when ``ENABLE_SNOW_AUTO_UPGRADE`` is off, when the process
    is air-gapped, or when env/config opt-in is off
    (``SNOWFLAKE_CLI_AUTO_UPGRADE`` overrides ``cli.auto_upgrade``).

    ``SNOWFLAKE_CLI_AUTO_UPGRADE_DONE`` is not consulted. That variable only
    stops a second upgrade in the spawned child. The new-version banner uses
    this function, so the child still hides the PyPI/Homebrew banner.

    One-run off via ``--no-auto-upgrade`` is handled by ``skip_due_to_cli_flag()``.
    ``cli.ignore_new_version_warning`` does not affect this.
    """
    if is_air_gapped():
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
