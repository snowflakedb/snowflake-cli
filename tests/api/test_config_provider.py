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

import os

import pytest
from snowflake.cli.api.config_provider import (
    ALTERNATIVE_CONFIG_ENV_VAR,
    AlternativeConfigProvider,
    LegacyConfigProvider,
    get_config_provider,
    reset_config_provider,
)


def test_legacy_provider_by_default():
    """Should use legacy provider when env var not set."""
    if ALTERNATIVE_CONFIG_ENV_VAR in os.environ:
        del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]

    reset_config_provider()
    provider = get_config_provider()
    assert isinstance(provider, LegacyConfigProvider)


def test_alternative_provider_when_enabled():
    """Should use alternative provider when env var is set."""
    os.environ[ALTERNATIVE_CONFIG_ENV_VAR] = "1"

    reset_config_provider()
    provider = get_config_provider()
    assert isinstance(provider, AlternativeConfigProvider)

    del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "yes", "Yes", "on", "1"])
def test_alternative_provider_various_values(value):
    """Should enable alternative provider for various truthy values."""
    os.environ[ALTERNATIVE_CONFIG_ENV_VAR] = value

    reset_config_provider()
    provider = get_config_provider()
    assert isinstance(provider, AlternativeConfigProvider)

    del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]


@pytest.mark.parametrize("value", ["0", "false", "False", "no", "off", ""])
def test_legacy_provider_for_falsy_values(value):
    """Should use legacy provider for falsy env var values."""
    os.environ[ALTERNATIVE_CONFIG_ENV_VAR] = value

    reset_config_provider()
    provider = get_config_provider()
    assert isinstance(provider, LegacyConfigProvider)

    del os.environ[ALTERNATIVE_CONFIG_ENV_VAR]


def test_provider_singleton():
    """Should return same instance on multiple calls."""
    reset_config_provider()
    from snowflake.cli.api.config_provider import get_config_provider_singleton

    provider1 = get_config_provider_singleton()
    provider2 = get_config_provider_singleton()
    assert provider1 is provider2


def test_reset_provider():
    """Should create new instance after reset."""
    reset_config_provider()
    from snowflake.cli.api.config_provider import get_config_provider_singleton

    provider1 = get_config_provider_singleton()
    reset_config_provider()
    provider2 = get_config_provider_singleton()
    assert provider1 is not provider2


def test_alternative_provider_has_all_required_methods():
    """AlternativeConfigProvider should have all ConfigProvider methods implemented."""
    provider = AlternativeConfigProvider()

    # Verify all abstract methods are implemented and callable
    # Note: These are smoke tests - comprehensive tests are in test_config_provider_integration.py
    assert callable(provider.get_section)
    assert callable(provider.get_value)
    assert callable(provider.set_value)
    assert callable(provider.unset_value)
    assert callable(provider.section_exists)
    assert callable(provider.read_config)
    assert callable(provider.get_connection_dict)
    assert callable(provider.get_all_connections)


def test_legacy_provider_has_all_required_methods():
    """LegacyConfigProvider should have all ConfigProvider methods implemented."""
    provider = LegacyConfigProvider()

    # Verify all abstract methods are implemented and callable
    assert callable(provider.get_section)
    assert callable(provider.get_value)
    assert callable(provider.set_value)
    assert callable(provider.unset_value)
    assert callable(provider.section_exists)
    assert callable(provider.read_config)
    assert callable(provider.get_connection_dict)
    assert callable(provider.get_all_connections)
