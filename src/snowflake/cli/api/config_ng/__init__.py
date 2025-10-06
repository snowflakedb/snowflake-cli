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

This package implements a layered, extensible configuration system with:
- Clear precedence rules (CLI > Environment > Files)
- Migration support (SnowCLI and SnowSQL compatibility)
- Complete resolution history tracking
- Read-only, immutable configuration sources
"""

from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionEntry,
    ResolutionHistory,
    SourcePriority,
    ValueSource,
)
from snowflake.cli.api.config_ng.env_handlers import (
    SnowCliEnvHandler,
    SnowSqlEnvHandler,
)
from snowflake.cli.api.config_ng.sources import (
    CliArgumentSource,
    ConfigurationSource,
    EnvironmentSource,
    FileSource,
)

__all__ = [
    "CliArgumentSource",
    "ConfigurationSource",
    "ConfigValue",
    "EnvironmentSource",
    "FileSource",
    "ResolutionEntry",
    "ResolutionHistory",
    "SnowCliEnvHandler",
    "SnowSqlEnvHandler",
    "SourcePriority",
    "ValueSource",
]
