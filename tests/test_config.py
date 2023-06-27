import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from snowcli.config import CliConfigManager


def test_empty_config_file_is_created_if_not_present():
    with TemporaryDirectory() as tmp_dir:
        config_file = Path(tmp_dir) / "sub" / "config.toml"
        assert config_file.exists() is False

        cm = CliConfigManager(file_path=config_file)
        cm.from_context(config_path_override=None)
        assert config_file.exists() is True
        assert config_file.read_text() == """[connections]\n"""


def test_environment_variables_override_configuration_value(test_snowcli_config):
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()

    assert cm.get("connections", "default", key="warehouse") == "xs"
    with mock.patch.dict(
        os.environ, {"SNOWFLAKE_CONNECTIONS_DEFAULT_WAREHOUSE": "foo42"}
    ):
        assert cm.get("connections", "default", key="warehouse") == "foo42"


def test_environment_variables_works_if_config_value_not_present(test_snowcli_config):
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()

    with pytest.raises(Exception):
        assert cm.get("connections", "2o3nei23ne2oixn", key="warehouse") == "xs"

    with mock.patch.dict(
        os.environ, {"SNOWFLAKE_CONNECTIONS_2O3NEI23NE2OIXN_WAREHOUSE": "foo42"}
    ):
        assert cm.get("connections", "2o3nei23ne2oixn", key="warehouse") == "foo42"
