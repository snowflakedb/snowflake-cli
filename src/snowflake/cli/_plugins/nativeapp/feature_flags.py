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

from enum import Enum, unique
from typing import Any, NamedTuple, Optional

from snowflake.cli.api.config import FEATURE_FLAGS_SECTION_PATH, get_config_value
from snowflake.cli.api.utils.types import try_cast_to_bool


class OptionalBooleanFlag(NamedTuple):
    name: str
    default: Optional[bool] = None


@unique
class OptionalFeatureFlagMixin(Enum):
    """
    Mixin for feature flags that can be enabled, disabled, or unset.
    """

    def get_flag_value(self) -> Optional[bool]:
        value = self._get_raw_value()
        if value is None:
            return self.value.default
        return try_cast_to_bool(value)

    def _get_raw_value(self) -> Any:
        return get_config_value(
            *FEATURE_FLAGS_SECTION_PATH,
            key=self.value.name.lower(),
            default=None,
        )


@unique
class FeatureFlag(OptionalFeatureFlagMixin):
    """
    Enum for Native Apps feature flags.
    """

    ENABLE_NATIVE_APP_PYTHON_SETUP = OptionalBooleanFlag(
        "ENABLE_NATIVE_APP_PYTHON_SETUP", False
    )
    ENABLE_RELEASE_CHANNELS = OptionalBooleanFlag("ENABLE_RELEASE_CHANNELS", None)
