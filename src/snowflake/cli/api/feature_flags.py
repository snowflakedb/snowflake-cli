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
from typing import NamedTuple

from snowflake.cli.api.config import (
    FEATURE_FLAGS_SECTION_PATH,
    get_config_bool_value,
    get_env_variable_name,
)


class BooleanFlag(NamedTuple):
    name: str
    default: bool = False


@unique
class FeatureFlagMixin(Enum):
    def is_enabled(self) -> bool:
        return get_config_bool_value(
            *FEATURE_FLAGS_SECTION_PATH,
            key=self.value.name.lower(),
            default=self.value.default,
        )

    def is_disabled(self):
        return not self.is_enabled()

    def env_variable(self):
        return get_env_variable_name(*FEATURE_FLAGS_SECTION_PATH, key=self.value.name)


@unique
class FeatureFlag(FeatureFlagMixin):
    ENABLE_STREAMLIT_EMBEDDED_STAGE = BooleanFlag(
        "ENABLE_STREAMLIT_EMBEDDED_STAGE", False
    )
    ENABLE_STREAMLIT_NO_CHECKOUTS = BooleanFlag("ENABLE_STREAMLIT_NO_CHECKOUTS", False)
    ENABLE_STREAMLIT_VERSIONED_STAGE = BooleanFlag(
        "ENABLE_STREAMLIT_VERSIONED_STAGE", False
    )
    ENABLE_PROJECT_DEFINITION_V2 = BooleanFlag("ENABLE_PROJECT_DEFINITION_V2", False)
