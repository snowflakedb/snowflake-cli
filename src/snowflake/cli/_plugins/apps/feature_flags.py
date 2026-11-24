# Copyright (c) 2026 Snowflake Inc.
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

"""Feature flags for Snowflake App Runtime commands.

They are read like the flags in :mod:`snowflake.cli.api.feature_flags`: from
``[cli.features]`` in ``config.toml``, or from
``SNOWFLAKE_CLI_FEATURES_<NAME>``.
"""

from enum import unique

from snowflake.cli.api.feature_flags import BooleanFlag, FeatureFlagMixin


@unique
class SnowflakeAppFeatureFlag(FeatureFlagMixin):
    ENABLE_APP_TEMPLATES = BooleanFlag("ENABLE_APP_TEMPLATES", False)
