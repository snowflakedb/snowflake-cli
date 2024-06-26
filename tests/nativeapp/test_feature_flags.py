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

from unittest import mock

import pytest
from snowflake.cli.plugins.nativeapp.feature_flags import FeatureFlag


@mock.patch("snowflake.cli.api.config.get_config_value")
@pytest.mark.parametrize("value_from_config", [True, False])
def test_feature_setup_script_generation_enabled(
    mock_get_config_value, value_from_config
):
    mock_get_config_value.return_value = value_from_config

    assert FeatureFlag.ENABLE_NATIVE_APP_PYTHON_SETUP.is_enabled() is value_from_config
    mock_get_config_value.assert_called_once_with(
        "cli", "features", key="enable_native_app_python_setup", default=False
    )
