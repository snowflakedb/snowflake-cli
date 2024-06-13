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
from click import ClickException
from snowflake.cli.api.feature_flags import BooleanFlag, FeatureFlagMixin


class _TestFlags(FeatureFlagMixin):
    # Intentional inconsistency between constant and the enum name to make sure there's no strict relation
    ENABLED_BY_DEFAULT = BooleanFlag("ENABLED_DEFAULT", True)
    DISABLED_BY_DEFAULT = BooleanFlag("DISABLED_DEFAULT", False)
    NON_BOOLEAN = BooleanFlag("NON_BOOLEAN", "xys")  # type: ignore


def test_flag_value_has_to_be_boolean():
    with pytest.raises(ClickException):
        _TestFlags.NON_BOOLEAN.is_enabled()


def test_flag_is_enabled():
    assert _TestFlags.ENABLED_BY_DEFAULT.is_enabled() is True
    assert _TestFlags.ENABLED_BY_DEFAULT.is_disabled() is False


def test_flag_is_disabled():
    assert _TestFlags.DISABLED_BY_DEFAULT.is_enabled() is False
    assert _TestFlags.DISABLED_BY_DEFAULT.is_disabled() is True


def test_flag_env_variable_value():
    assert (
        _TestFlags.ENABLED_BY_DEFAULT.env_variable()
        == "SNOWFLAKE_CLI_FEATURES_ENABLED_DEFAULT"
    )
    assert (
        _TestFlags.DISABLED_BY_DEFAULT.env_variable()
        == "SNOWFLAKE_CLI_FEATURES_DISABLED_DEFAULT"
    )


@mock.patch("snowflake.cli.api.config.get_config_value")
@pytest.mark.parametrize("value_from_config", [True, False])
def test_flag_from_config_file(mock_get_config_value, value_from_config):
    mock_get_config_value.return_value = value_from_config

    assert _TestFlags.DISABLED_BY_DEFAULT.is_enabled() is value_from_config
    mock_get_config_value.assert_called_once_with(
        "cli", "features", key="disabled_default", default=False
    )


@pytest.mark.parametrize("value_from_env", ["1", "true", "True", "TRUE", "TruE"])
def test_flag_is_enabled_from_env_var(value_from_env):
    with mock.patch.dict(
        "os.environ", {"SNOWFLAKE_CLI_FEATURES_DISABLED_DEFAULT": value_from_env}
    ):
        assert _TestFlags.DISABLED_BY_DEFAULT.is_enabled() is True


@pytest.mark.parametrize("value_from_env", ["0", "false", "False", "FALSE", "FaLse"])
def test_flag_is_disabled_from_env_var(value_from_env):
    with mock.patch.dict(
        "os.environ", {"SNOWFLAKE_CLI_FEATURES_ENABLED_DEFAULT": value_from_env}
    ):
        assert _TestFlags.ENABLED_BY_DEFAULT.is_enabled() is False
