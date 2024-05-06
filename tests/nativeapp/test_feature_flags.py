from unittest import mock

import pytest
from snowflake.cli.plugins.nativeapp.feature_flags import FeatureFlag


@mock.patch("snowflake.cli.api.config.get_config_value")
@pytest.mark.parametrize("value_from_config", [True, False])
def test_feature_setup_script_generation_enabled(
    mock_get_config_value, value_from_config
):
    mock_get_config_value.return_value = value_from_config

    assert FeatureFlag.ENABLE_SETUP_SCRIPT_GENERATION.is_enabled() is value_from_config
    mock_get_config_value.assert_called_once_with(
        "cli", "features", key="enable_setup_script_generation", default=False
    )
