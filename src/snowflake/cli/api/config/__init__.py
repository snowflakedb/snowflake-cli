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

"""
Configuration package with support for legacy and next-generation config systems.

This package provides a feature flag mechanism to switch between the legacy
configuration system and the new config-ng system using the environment variable
SNOWFLAKE_CLI_CONFIG_NG.

Usage:
    # Enable new config system
    export SNOWFLAKE_CLI_CONFIG_NG=1

    # Use legacy config system (default)
    unset SNOWFLAKE_CLI_CONFIG_NG

Environment Variables:
    SNOWFLAKE_CLI_CONFIG_NG: Set to '1', 'true', 'yes', or 'on' to enable config-ng
"""

from __future__ import annotations

import os
from typing import Literal

# Environment variable to enable the new config-ng system
CONFIG_NG_ENV_VAR: Literal["SNOWFLAKE_CLI_CONFIG_NG"] = "SNOWFLAKE_CLI_CONFIG_NG"


def _is_config_ng_enabled() -> bool:
    """
    Check if the new config-ng system should be used.

    Returns:
        True if config-ng should be used, False for legacy system
    """
    return os.environ.get(CONFIG_NG_ENV_VAR, "").lower() in ("1", "true", "yes", "on")


# Conditionally import from legacy or config-ng based on environment variable
if _is_config_ng_enabled():
    # Import ONLY from the new config-ng system
    # This will fail if config-ng is not fully implemented, which is intentional
    # to help identify what needs to be implemented
    try:
        from .config_ng import *  # noqa: F403, F401
    except ImportError as e:
        # If config-ng is not fully implemented, fail fast with a clear error
        raise ImportError(
            f"Config-ng is enabled via {CONFIG_NG_ENV_VAR} but is not fully implemented. "
            f"Missing implementation: {e}. "
            f"Either implement the missing functionality in config_ng.py or "
            f"disable config-ng by unsetting {CONFIG_NG_ENV_VAR}."
        ) from e
else:
    # Use the legacy configuration system
    from .legacy import *  # noqa: F403, F401

# Export the feature flag utilities and factory method for external use
__all__ = ["_is_config_ng_enabled", "CONFIG_NG_ENV_VAR", "get_config_manager"]
