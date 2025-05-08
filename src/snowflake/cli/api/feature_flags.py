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
    default: bool | None = False


@unique
class FeatureFlagMixin(Enum):
    def get_value(self) -> bool | None:
        return get_config_bool_value(
            *FEATURE_FLAGS_SECTION_PATH,
            key=self.value.name.lower(),
            default=self.value.default,
        )

    def is_enabled(self) -> bool:
        return self.get_value() is True

    def is_disabled(self) -> bool:
        return self.get_value() is False

    def is_set(self) -> bool:
        return (
            get_config_bool_value(
                *FEATURE_FLAGS_SECTION_PATH, key=self.value.name.lower(), default=None
            )
            is not None
        )

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
    ENABLE_SEPARATE_AUTHENTICATION_POLICY_ID = BooleanFlag(
        "ENABLE_SEPARATE_AUTHENTICATION_POLICY_ID", False
    )
    ENABLE_SNOWPARK_GLOB_SUPPORT = BooleanFlag("ENABLE_SNOWPARK_GLOB_SUPPORT", False)
    ENABLE_SPCS_SERVICE_EVENTS = BooleanFlag("ENABLE_SPCS_SERVICE_EVENTS", False)
    ENABLE_AUTH_KEYPAIR = BooleanFlag("ENABLE_AUTH_KEYPAIR", False)
    ENABLE_NATIVE_APP_PYTHON_SETUP = BooleanFlag(
        "ENABLE_NATIVE_APP_PYTHON_SETUP", False
    )
    ENABLE_NATIVE_APP_CHILDREN = BooleanFlag("ENABLE_NATIVE_APP_CHILDREN", False)
    # TODO 4.0: remove ENABLE_RELEASE_CHANNELS
    ENABLE_RELEASE_CHANNELS = BooleanFlag("ENABLE_RELEASE_CHANNELS", None)
