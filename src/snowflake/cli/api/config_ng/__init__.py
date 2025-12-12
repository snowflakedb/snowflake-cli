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
- Two-phase resolution: file sources use connection-level replacement,
  overlay sources (env/CLI) use field-level merging
- List-order precedence (explicit ordering in source list)
- Migration support (SnowCLI and SnowSQL compatibility)
- Complete resolution history tracking
- Read-only, immutable configuration sources
"""

from snowflake.cli.api.config_ng.constants import (
    FILE_SOURCE_NAMES,
    INTERNAL_CLI_PARAMETERS,
    ConfigSection,
    ConfigSourceName,
)
from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionEntry,
    ResolutionHistory,
    SourceDiagnostic,
    SourceType,
    ValueSource,
)
from snowflake.cli.api.config_ng.dict_utils import deep_merge
from snowflake.cli.api.config_ng.merge_operations import (
    create_default_connection_from_params,
    extract_root_level_connection_params,
    merge_params_into_connections,
)
from snowflake.cli.api.config_ng.observers import (
    ResolutionHistoryTracker,
    ResolutionObserver,
    TelemetryObserver,
    create_observer_bundle,
)
from snowflake.cli.api.config_ng.parsers import SnowSQLParser, TOMLParser
from snowflake.cli.api.config_ng.presentation import ResolutionPresenter
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
from snowflake.cli.api.config_ng.resolver import ConfigurationResolver
from snowflake.cli.api.config_ng.source_factory import create_default_sources
from snowflake.cli.api.config_ng.source_manager import SourceManager
from snowflake.cli.api.config_ng.sources import (
    CliConfigFile,
    CliEnvironment,
    CliParameters,
    ConnectionsConfigFile,
    ConnectionSpecificEnvironment,
    SnowSQLConfigFile,
    SnowSQLEnvironment,
    SnowSQLSection,
    get_merged_variables,
)
from snowflake.cli.api.config_ng.telemetry_integration import (
    get_config_telemetry_payload,
    record_config_source_usage,
)

__all__ = [
    "check_value_source",
    "CliConfigFile",
    "CliEnvironment",
    "CliParameters",
    "ConfigSection",
    "ConfigurationResolver",
    "create_observer_bundle",
    "ConfigValue",
    "ConnectionsConfigFile",
    "ConnectionSpecificEnvironment",
    "ConfigSourceName",
    "create_default_connection_from_params",
    "create_default_sources",
    "deep_merge",
    "explain_configuration",
    "export_resolution_history",
    "extract_root_level_connection_params",
    "FILE_SOURCE_NAMES",
    "format_summary_for_display",
    "get_config_telemetry_payload",
    "get_merged_variables",
    "get_resolution_summary",
    "get_resolver",
    "INTERNAL_CLI_PARAMETERS",
    "is_resolution_logging_available",
    "merge_params_into_connections",
    "record_config_source_usage",
    "ResolutionEntry",
    "ResolutionHistory",
    "ResolutionHistoryTracker",
    "SourceDiagnostic",
    "ResolutionObserver",
    "ResolutionPresenter",
    "show_all_resolution_chains",
    "show_resolution_chain",
    "SnowSQLConfigFile",
    "SnowSQLEnvironment",
    "SnowSQLParser",
    "SnowSQLSection",
    "SourceManager",
    "SourceType",
    "SourceDiagnostic",
    "TelemetryObserver",
    "TOMLParser",
    "ValueSource",
]
