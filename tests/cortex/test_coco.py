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

from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.coco.cortex_code import (
    _check_platform_supported,
    _find_cortex_code_binary,
    _get_bin_dir,
    _get_binary_path,
    _get_install_dir,
    remove_cortex_code,
    run_cortex_code,
)
from snowflake.cli.api.exceptions import CliError


class TestCortexCodePaths:
    def test_get_install_dir(self):
        install_dir = _get_install_dir()
        assert install_dir == Path.home() / ".local" / "share" / "cortex"

    def test_get_bin_dir(self):
        bin_dir = _get_bin_dir()
        assert bin_dir == Path.home() / ".local" / "bin"

    def test_get_binary_path(self):
        binary_path = _get_binary_path()
        assert binary_path == Path.home() / ".local" / "bin" / "cortex"


class TestPlatformSupport:
    @mock.patch("snowflake.cli._plugins.coco.cortex_code.platform.system")
    def test_supported_on_darwin(self, mock_system):
        mock_system.return_value = "Darwin"
        _check_platform_supported()

    @mock.patch("snowflake.cli._plugins.coco.cortex_code.platform.system")
    def test_supported_on_linux(self, mock_system):
        mock_system.return_value = "Linux"
        _check_platform_supported()

    @mock.patch("snowflake.cli._plugins.coco.cortex_code.platform.system")
    def test_not_supported_on_windows(self, mock_system):
        mock_system.return_value = "Windows"
        with pytest.raises(CliError) as exc_info:
            _check_platform_supported()
        assert "not supported on Windows" in str(exc_info.value)
        assert "Darwin" in str(exc_info.value)
        assert "Linux" in str(exc_info.value)


class TestFindCortexCodeBinary:
    @mock.patch("shutil.which")
    def test_finds_binary_in_path(self, mock_which):
        mock_which.return_value = "/usr/local/bin/cortex"
        result = _find_cortex_code_binary()
        assert result == "/usr/local/bin/cortex"
        mock_which.assert_called_once_with("cortex")

    @mock.patch("shutil.which")
    @mock.patch("pathlib.Path.exists")
    def test_finds_binary_in_local_path(self, mock_exists, mock_which):
        mock_which.return_value = None
        mock_exists.return_value = True
        result = _find_cortex_code_binary()
        assert result == str(_get_binary_path())

    @mock.patch("shutil.which")
    @mock.patch("pathlib.Path.exists")
    def test_returns_none_when_not_found(self, mock_exists, mock_which):
        mock_which.return_value = None
        mock_exists.return_value = False
        result = _find_cortex_code_binary()
        assert result is None


class TestRemoveCortexCode:
    @mock.patch("pathlib.Path.exists")
    @mock.patch("pathlib.Path.is_symlink")
    def test_raises_error_when_not_installed(self, mock_is_symlink, mock_exists):
        mock_exists.return_value = False
        mock_is_symlink.return_value = False

        with pytest.raises(CliError) as exc_info:
            remove_cortex_code()
        assert "not installed through `snow`" in str(exc_info.value)

    @mock.patch("shutil.rmtree")
    @mock.patch("pathlib.Path.unlink")
    @mock.patch("pathlib.Path.is_symlink")
    @mock.patch("pathlib.Path.exists")
    def test_removes_binary_and_install_dir(
        self, mock_exists, mock_is_symlink, mock_unlink, mock_rmtree
    ):
        mock_exists.return_value = True
        mock_is_symlink.return_value = True

        remove_cortex_code()

        mock_unlink.assert_called_once()
        mock_rmtree.assert_called_once()


class TestRunCortexCode:
    def test_remove_with_args_raises_error(self):
        with pytest.raises(CliError) as exc_info:
            run_cortex_code(["--help"], remove=True)
        assert "Cannot use --remove with args" in str(exc_info.value)

    @mock.patch("snowflake.cli._plugins.coco.cortex_code.remove_cortex_code")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code.cli_console")
    def test_remove_calls_remove_function(self, mock_console, mock_remove):
        result = run_cortex_code([], remove=True)
        assert result == 0
        mock_remove.assert_called_once()

    @mock.patch("subprocess.run")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code._find_cortex_code_binary")
    def test_runs_existing_binary_with_args(self, mock_find, mock_run):
        mock_find.return_value = "/usr/local/bin/cortex"
        mock_run.return_value = mock.Mock(returncode=0)

        result = run_cortex_code(["--help"])

        assert result == 0
        mock_run.assert_called_once_with(["/usr/local/bin/cortex", "--help"])

    @mock.patch("subprocess.run")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code._find_cortex_code_binary")
    def test_passes_through_exit_code(self, mock_find, mock_run):
        mock_find.return_value = "/usr/local/bin/cortex"
        mock_run.return_value = mock.Mock(returncode=42)

        result = run_cortex_code(["invalid-command"])

        assert result == 42

    @mock.patch("snowflake.cli._plugins.coco.cortex_code.cli_console")
    @mock.patch.dict("os.environ", {"CI": ""}, clear=False)
    @mock.patch("sys.stdin")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code._find_cortex_code_binary")
    def test_returns_error_when_not_installed_non_interactive(
        self, mock_find, mock_stdin, mock_console
    ):
        mock_find.return_value = None
        mock_stdin.isatty.return_value = False

        result = run_cortex_code([])

        assert result == 1
        mock_console.warning.assert_called_once()

    @mock.patch("snowflake.cli._plugins.coco.cortex_code.cli_console")
    @mock.patch("sys.stdin")
    @mock.patch("typer.confirm")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code._find_cortex_code_binary")
    def test_prompts_for_install_when_interactive(
        self, mock_find, mock_confirm, mock_stdin, mock_console
    ):
        mock_find.return_value = None
        mock_stdin.isatty.return_value = True
        mock_confirm.return_value = False  # User declines

        result = run_cortex_code([])

        assert result == 1
        mock_confirm.assert_called_once()

    @mock.patch("subprocess.run")
    @mock.patch("sys.stdin")
    @mock.patch("typer.confirm")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code._download_cortex_code")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code._find_cortex_code_binary")
    def test_downloads_and_runs_when_user_confirms(
        self,
        mock_find,
        mock_download,
        mock_confirm,
        mock_stdin,
        mock_run,
    ):
        mock_find.return_value = None
        mock_stdin.isatty.return_value = True
        mock_confirm.return_value = True
        mock_download.return_value = "/home/user/.local/bin/cortex"
        mock_run.return_value = mock.Mock(returncode=0)

        result = run_cortex_code(["--version"])

        assert result == 0
        mock_download.assert_called_once()
        mock_run.assert_called_once_with(["/home/user/.local/bin/cortex", "--version"])


class TestCocoCommand:
    def test_coco_command_shows_in_help(self, runner):
        result = runner.invoke(["--help"])
        assert result.exit_code == 0
        assert "coco" in result.output

    def test_coco_command_no_args_shows_help(self, runner):
        result = runner.invoke(["coco"])
        assert result.exit_code == 0
        assert "--remove" in result.output

    @mock.patch("snowflake.cli._plugins.coco.commands.run_cortex_code")
    @mock.patch("snowflake.cli._plugins.coco.commands.sys")
    def test_coco_command_with_separator_calls_run_cortex_code(
        self, mock_sys, mock_run, runner
    ):
        mock_sys.argv = ["snow", "coco", "--"]
        mock_run.return_value = 0
        result = runner.invoke(["coco", "--"])
        mock_run.assert_called_once()

    @mock.patch("snowflake.cli._plugins.coco.commands.run_cortex_code")
    def test_coco_command_passes_args(self, mock_run, runner):
        mock_run.return_value = 0
        runner.invoke(["coco", "--", "--version"])
        args, kwargs = mock_run.call_args
        assert "--version" in args[0]

    @mock.patch("snowflake.cli._plugins.coco.commands.run_cortex_code")
    def test_coco_command_remove_flag(self, mock_run, runner):
        mock_run.return_value = 0
        runner.invoke(["coco", "--remove"])
        args, kwargs = mock_run.call_args
        assert kwargs.get("remove") is True


class TestDownloadCortexCode:
    @mock.patch("subprocess.run")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code.cli_console")
    def test_download_runs_install_script(self, mock_console, mock_run):
        from snowflake.cli._plugins.coco.cortex_code import (
            INSTALL_SCRIPT_URL,
            _download_cortex_code,
        )

        mock_run.return_value = mock.Mock(returncode=0, stderr="", stdout="")

        with mock.patch("pathlib.Path.exists", return_value=True):
            result = _download_cortex_code()

        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert INSTALL_SCRIPT_URL in call_args[0][0][2]
        assert call_args[1]["env"]["NON_INTERACTIVE"] == "1"
        assert call_args[1]["env"]["SKIP_PODMAN"] == "1"
        assert call_args[1]["env"]["SKIP_PATH_PROMPT"] == "1"

    @mock.patch("subprocess.run")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code.cli_console")
    def test_download_raises_on_failure(self, mock_console, mock_run):
        from snowflake.cli._plugins.coco.cortex_code import _download_cortex_code

        mock_run.return_value = mock.Mock(
            returncode=1, stderr="Connection failed", stdout=""
        )

        with pytest.raises(CliError) as exc_info:
            _download_cortex_code()
        assert "Failed to install" in str(exc_info.value)

    @mock.patch("subprocess.run")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code.cli_console")
    def test_download_raises_when_binary_not_found(self, mock_console, mock_run):
        from snowflake.cli._plugins.coco.cortex_code import _download_cortex_code

        mock_run.return_value = mock.Mock(returncode=0, stderr="", stdout="")

        with mock.patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(CliError) as exc_info:
                _download_cortex_code()
            assert "binary not found" in str(exc_info.value)

    @mock.patch("subprocess.run")
    @mock.patch("snowflake.cli._plugins.coco.cortex_code.cli_console")
    @mock.patch.dict("os.environ", {"CORTEX_CHANNEL": "dev"}, clear=False)
    def test_download_respects_channel_env_var(self, mock_console, mock_run):
        from snowflake.cli._plugins.coco.cortex_code import _download_cortex_code

        mock_run.return_value = mock.Mock(returncode=0, stderr="", stdout="")

        with mock.patch("pathlib.Path.exists", return_value=True):
            _download_cortex_code()

        call_args = mock_run.call_args
        assert call_args[1]["env"]["CORTEX_CHANNEL"] == "dev"
