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
Enhanced Configuration System - Next Generation (NG)

This package implements a simple, extensible configuration system with:
- List-order precedence (explicit ordering in source list)
- Migration support (SnowCLI and SnowSQL compatibility)
- Complete resolution history tracking
- Read-only, immutable configuration sources
"""

from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionEntry,
    ResolutionHistory,
    ValueSource,
)
from snowflake.cli.api.config_ng.resolution_logger import (
    check_value_source,
    explain_configuration,
    export_resolution_history,
    format_summary_for_display,
    get_resolution_summary,
    get_resolver,
    is_resolution_logging_available,
    show_all_resolution_chains,
    show_resolution_chain,
)
from snowflake.cli.api.config_ng.resolver import (
    ConfigurationResolver,
    ResolutionHistoryTracker,
)
from snowflake.cli.api.config_ng.sources import (
    CliConfigFile,
    CliEnvironment,
    CliParameters,
    ConnectionsConfigFile,
    ConnectionSpecificEnvironment,
    SnowSQLConfigFile,
    SnowSQLEnvironment,
)

__all__ = [
    "check_value_source",
    "CliConfigFile",
    "CliEnvironment",
    "CliParameters",
    "ConfigurationResolver",
    "ConfigValue",
    "ConnectionsConfigFile",
    "ConnectionSpecificEnvironment",
    "explain_configuration",
    "export_resolution_history",
    "format_summary_for_display",
    "get_resolution_summary",
    "get_resolver",
    "is_resolution_logging_available",
    "ResolutionEntry",
    "ResolutionHistory",
    "ResolutionHistoryTracker",
    "show_all_resolution_chains",
    "show_resolution_chain",
    "SnowSQLConfigFile",
    "SnowSQLEnvironment",
    "ValueSource",
]
