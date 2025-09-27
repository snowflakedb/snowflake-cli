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
Next-generation configuration system (config-ng).

This module will contain the new configuration handling implementation.
It provides a modern, type-safe, and extensible configuration system
that will eventually replace the legacy configuration system.

TODO: Implement the new configuration system with:
- Type-safe configuration schemas using Pydantic or similar
- Better validation and error handling
- Improved environment variable support with type coercion
- Plugin-based configuration providers
- Configuration migration utilities from legacy system
- Support for multiple configuration formats (TOML, YAML, JSON)
- Configuration templates and inheritance
- Better error messages with suggestions
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# TODO: Implement new configuration system
# This is a clean implementation that will help identify what needs to be implemented
# by failing fast when config-ng is enabled but functionality is missing


def config_init(config_file: Optional[Path]) -> None:
    """
    Initialize the next-generation configuration system.

    This is a placeholder for the new configuration initialization.
    When config-ng is enabled, this function should be fully implemented
    without relying on legacy code.

    Args:
        config_file: Optional path to configuration file

    TODO: Implement new features:
    - Configuration schema validation
    - Better error handling and user feedback
    - Configuration migration from legacy format
    - Support for configuration inheritance
    - Environment-specific configuration overlays
    """
    log.info("Using next-generation configuration system (config-ng)")
    # TODO: Implement new config initialization
    raise NotImplementedError(
        "config_init is not implemented in config-ng. "
        "This helps identify what needs to be implemented for the new system."
    )


# TODO: Add all other configuration functions and constants that are needed
# Each missing function will cause an ImportError when config-ng is enabled,
# helping identify what needs to be implemented

# Example of what needs to be implemented:
# - ConnectionConfig class
# - CONFIG_MANAGER equivalent
# - All configuration constants (CLI_SECTION, CONNECTIONS_SECTION, etc.)
# - All configuration functions (set_config_value, get_config_value, etc.)

# For now, we'll let ImportError guide us to what needs to be implemented
