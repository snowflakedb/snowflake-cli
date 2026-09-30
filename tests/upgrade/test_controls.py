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

from __future__ import annotations

from contextlib import contextmanager

import pytest
from snowflake.cli._app.cli_app import CliAppFactory
from snowflake.cli._plugins.upgrade.controls import (
    AUTO_UPGRADE_DONE_ENV,
    NO_AUTO_UPGRADE_FLAG,
    allow_major,
    is_auto_upgrade_enabled,
    skip_due_to_cli_flag,
)
from snowflake.cli.api.config import (
    AUTO_UPGRADE_ALLOW_MAJOR_KEY,
    AUTO_UPGRADE_KEY,
    CLI_SECTION,
    IGNORE_NEW_VERSION_WARNING_KEY,
    config_init,
    get_env_variable_name,
)
from snowflake.cli.api.feature_flags import FeatureFlag

from tests.conftest import SnowCLIRunner
from tests_common.feature_flag_utils import with_feature_flags

AUTO_UPGRADE_ENV = get_env_variable_name(CLI_SECTION, key=AUTO_UPGRADE_KEY)
ALLOW_MAJOR_ENV = get_env_variable_name(CLI_SECTION, key=AUTO_UPGRADE_ALLOW_MAJOR_KEY)


@pytest.fixture
def auto_upgrade_config(config_file, monkeypatch):
    monkeypatch.delenv(AUTO_UPGRADE_DONE_ENV, raising=False)
    monkeypatch.delenv(AUTO_UPGRADE_ENV, raising=False)
    monkeypatch.delenv(ALLOW_MAJOR_ENV, raising=False)

    @contextmanager
    def _init(content: str = ""):
        with config_file(content) as cfg:
            config_init(cfg)
            yield cfg

    return _init


def test_feature_flag_off_reader_is_false_even_when_config_and_env_on(
    auto_upgrade_config, monkeypatch
):
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "1")
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_KEY} = true\n"):
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: False}):
            assert is_auto_upgrade_enabled() is False


def test_config_on_when_flag_enabled(auto_upgrade_config):
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_KEY} = true\n"):
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
            assert is_auto_upgrade_enabled() is True


def test_config_off_when_flag_enabled(auto_upgrade_config):
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_KEY} = false\n"):
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
            assert is_auto_upgrade_enabled() is False


def test_env_one_overrides_config_off(auto_upgrade_config, monkeypatch):
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "1")
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_KEY} = false\n"):
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
            assert is_auto_upgrade_enabled() is True


def test_env_zero_overrides_config_on(auto_upgrade_config, monkeypatch):
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "0")
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_KEY} = true\n"):
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
            assert is_auto_upgrade_enabled() is False


def test_already_done_env_turns_off(auto_upgrade_config, monkeypatch):
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "1")
    monkeypatch.setenv(AUTO_UPGRADE_DONE_ENV, "1")
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_KEY} = true\n"):
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
            assert is_auto_upgrade_enabled() is False


def test_ignore_new_version_warning_does_not_disable_auto_upgrade(auto_upgrade_config):
    with auto_upgrade_config(
        f"[cli]\n{AUTO_UPGRADE_KEY} = true\n{IGNORE_NEW_VERSION_WARNING_KEY} = true\n"
    ):
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
            assert is_auto_upgrade_enabled() is True


def test_allow_major_defaults_false(auto_upgrade_config):
    with auto_upgrade_config("[cli]\n"):
        assert allow_major() is False


def test_allow_major_config_true(auto_upgrade_config):
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_ALLOW_MAJOR_KEY} = true\n"):
        assert allow_major() is True


def test_allow_major_env_one_overrides_config_off(auto_upgrade_config, monkeypatch):
    monkeypatch.setenv(ALLOW_MAJOR_ENV, "1")
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_ALLOW_MAJOR_KEY} = false\n"):
        assert allow_major() is True


def test_allow_major_env_zero_overrides_config_on(auto_upgrade_config, monkeypatch):
    monkeypatch.setenv(ALLOW_MAJOR_ENV, "0")
    with auto_upgrade_config(f"[cli]\n{AUTO_UPGRADE_ALLOW_MAJOR_KEY} = true\n"):
        assert allow_major() is False


@pytest.mark.parametrize(
    "argv, expected",
    [
        ([], False),
        (["sql", "-q", "select 1"], False),
        ([NO_AUTO_UPGRADE_FLAG, "sql"], True),
        (["sql", NO_AUTO_UPGRADE_FLAG], True),
        (["--no-auto-upgrade-extra"], False),
        (["--format", "json"], False),
    ],
)
def test_skip_due_to_cli_flag(argv, expected):
    assert skip_due_to_cli_flag(argv) is expected


def test_skip_due_to_cli_flag_none_does_not_read_sys_argv(monkeypatch):
    monkeypatch.setattr(
        "sys.argv", ["snow", NO_AUTO_UPGRADE_FLAG, "sql"], raising=False
    )
    assert skip_due_to_cli_flag(None) is False
    assert skip_due_to_cli_flag() is False


def test_no_auto_upgrade_parsed_as_root_flag_without_running_a_command(runner):
    result = runner.invoke([NO_AUTO_UPGRADE_FLAG, "--help"])
    assert result.exit_code == 0, result.output
    assert "No such option" not in result.output


def test_help_hides_no_auto_upgrade_when_feature_flag_off(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert NO_AUTO_UPGRADE_FLAG not in result.output


def test_help_shows_no_auto_upgrade_when_feature_flag_on(test_snowcli_config):
    with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
        app = CliAppFactory().create_or_get_app()
        runner = SnowCLIRunner(app, str(test_snowcli_config))
        result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert NO_AUTO_UPGRADE_FLAG in result.output


def test_new_config_initialises_auto_upgrade_keys(tmp_path):
    config_path = tmp_path / "sub" / "config.toml"
    config_init(config_path)
    text = config_path.read_text(encoding="utf-8")
    assert f"{AUTO_UPGRADE_KEY} = false" in text
    assert f"{AUTO_UPGRADE_ALLOW_MAJOR_KEY} = false" in text
