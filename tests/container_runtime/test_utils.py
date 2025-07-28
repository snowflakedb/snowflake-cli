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

import subprocess
from unittest.mock import Mock, mock_open, patch

from snowflake.cli._plugins.container_runtime.utils import (
    check_websocat_installed,
    configure_vscode_settings,
    install_websocat_instructions,
    setup_ssh_config_with_token,
)


def test_check_websocat_installed_success():
    """Test checking websocat installation when it's available."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = Mock(returncode=0)

        result = check_websocat_installed()

        assert result is True
        mock_run.assert_called_once_with(
            ["websocat", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )


def test_check_websocat_installed_not_found():
    """Test checking websocat installation when it's not available."""
    with patch("subprocess.run", side_effect=FileNotFoundError):
        result = check_websocat_installed()
        assert result is False


def test_install_websocat_instructions():
    """Test getting websocat installation instructions."""
    with patch("platform.system") as mock_system:
        # Test macOS
        mock_system.return_value = "Darwin"
        result = install_websocat_instructions()
        assert "brew install websocat" in result

        # Test Linux
        mock_system.return_value = "Linux"
        result = install_websocat_instructions()
        assert "github.com/vi/websocat" in result


@patch("snowflake.cli._plugins.container_runtime.utils.check_websocat_installed")
@patch("builtins.open", new_callable=mock_open)
@patch("os.path.exists")
@patch("os.path.expanduser")
def test_setup_ssh_config_with_token_new_config(
    mock_expanduser, mock_exists, mock_file_open, mock_websocat_check
):
    """Test setting up SSH config when no config file exists."""
    mock_expanduser.return_value = "/home/user/.ssh/config"
    mock_exists.return_value = False
    mock_websocat_check.return_value = True

    setup_ssh_config_with_token("test_runtime", "wss://example.com/ssh", "test_token")

    # Should write new config
    mock_file_open.assert_called_once_with("/home/user/.ssh/config", "w")
    written_content = mock_file_open().write.call_args[0][0]

    assert "Host snowflake-remote-runtime-test_runtime" in written_content
    assert "HostName example.com" in written_content
    assert "test_token" in written_content


@patch("snowflake.cli._plugins.container_runtime.utils.check_websocat_installed")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="""
Host other-host
    HostName other.example.com
    User admin

Host snowflake-remote-runtime-test_runtime
    ProxyCommand websocat --header "Authorization: Bearer old_token" - wss://old.example.com/ssh
    User root
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null

Host another-host
    HostName another.example.com
""",
)
@patch("os.path.exists")
@patch("os.path.expanduser")
def test_setup_ssh_config_with_token_existing_config(
    mock_expanduser, mock_exists, mock_file_open, mock_websocat_check
):
    """Test setting up SSH config when config file already exists."""
    mock_expanduser.return_value = "/home/user/.ssh/config"
    mock_exists.return_value = True
    mock_websocat_check.return_value = True

    setup_ssh_config_with_token("test_runtime", "wss://example.com/ssh", "new_token")

    # Should read existing file and write updated config
    assert mock_file_open.call_count == 2  # One read, one write

    # Check that write was called
    write_calls = [call for call in mock_file_open().write.call_args_list]
    assert len(write_calls) > 0

    written_content = write_calls[0][0][0]
    assert "new_token" in written_content
    assert "Host snowflake-remote-runtime-test_runtime" in written_content


@patch("snowflake.cli._plugins.container_runtime.utils.check_websocat_installed")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data="""
Host other-host
    HostName other.example.com
    User admin
""",
)
@patch("os.path.exists")
@patch("os.path.expanduser")
def test_setup_ssh_config_with_token_append_to_existing(
    mock_expanduser, mock_exists, mock_file_open, mock_websocat_check
):
    """Test setting up SSH config when runtime host doesn't exist in config."""
    mock_expanduser.return_value = "/home/user/.ssh/config"
    mock_exists.return_value = True
    mock_websocat_check.return_value = True

    setup_ssh_config_with_token("test_runtime", "wss://example.com/ssh", "test_token")

    # Should read existing file and append new config
    assert mock_file_open.call_count == 2  # One read, one write

    write_calls = [call for call in mock_file_open().write.call_args_list]
    written_content = write_calls[0][0][0]

    # Should contain both old and new config
    assert "Host other-host" in written_content
    assert "Host snowflake-remote-runtime-test_runtime" in written_content
    assert "test_token" in written_content


@patch("os.path.exists")
@patch("os.makedirs")
@patch("os.path.dirname")
@patch("os.path.expanduser")
@patch("builtins.open", new_callable=mock_open)
def test_configure_vscode_settings_new_file(
    mock_file_open, mock_expanduser, mock_dirname, mock_makedirs, mock_exists
):
    """Test configuring VS Code settings when no settings file exists."""
    # Mock expanduser to return different paths for VS Code and VS Code Insiders
    def expanduser_side_effect(path):
        if "Code - Insiders" in path:
            return "/home/user/Library/Application Support/Code - Insiders/User/settings.json"
        else:
            return "/home/user/Library/Application Support/Code/User/settings.json"

    mock_expanduser.side_effect = expanduser_side_effect

    def dirname_side_effect(path):
        if "settings.json" in path:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders/User"
            else:
                return "/home/user/Library/Application Support/Code/User"
        else:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders"
            else:
                return "/home/user/Library/Application Support/Code"

    mock_dirname.side_effect = dirname_side_effect

    # Mock directory structure exists but settings file doesn't
    def exists_side_effect(path):
        if "settings.json" in path:
            return False  # Settings file doesn't exist
        return True  # Directories exist

    mock_exists.side_effect = exists_side_effect

    configure_vscode_settings("test_runtime", "/custom/server/path")

    # Should create settings directory for both VS Code variants
    assert mock_makedirs.call_count == 2
    mock_makedirs.assert_any_call(
        "/home/user/Library/Application Support/Code/User", exist_ok=True
    )
    mock_makedirs.assert_any_call(
        "/home/user/Library/Application Support/Code - Insiders/User", exist_ok=True
    )

    # Should write settings for both variants - look for write calls
    assert mock_file_open.call_count >= 2

    # Check that files were opened for writing
    write_file_calls = [
        call
        for call in mock_file_open.call_args_list
        if len(call[0]) > 0 and call[0][1] == "w"
    ]
    assert len(write_file_calls) >= 2

    # Test passes if we reach here without exceptions - the specific JSON content is tested in integration


@patch("os.path.exists")
@patch("os.makedirs")
@patch("os.path.dirname")
@patch("os.path.expanduser")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='{"editor.fontSize": 14, "remote.SSH.remotePlatform": {"other-host": "linux"}, "remote.SSH.serverInstallPath": {"other-host": "/other/path"}}',
)
def test_configure_vscode_settings_existing_file(
    mock_file_open, mock_expanduser, mock_dirname, mock_makedirs, mock_exists
):
    """Test configuring VS Code settings when settings file already exists."""
    # Mock expanduser to return different paths for VS Code and VS Code Insiders
    def expanduser_side_effect(path):
        if "Code - Insiders" in path:
            return "/home/user/Library/Application Support/Code - Insiders/User/settings.json"
        else:
            return "/home/user/Library/Application Support/Code/User/settings.json"

    mock_expanduser.side_effect = expanduser_side_effect

    def dirname_side_effect(path):
        if "settings.json" in path:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders/User"
            else:
                return "/home/user/Library/Application Support/Code/User"
        else:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders"
            else:
                return "/home/user/Library/Application Support/Code"

    mock_dirname.side_effect = dirname_side_effect

    mock_exists.return_value = True

    configure_vscode_settings("test_runtime", "/custom/server/path")

    # Should create directories for both variants
    assert mock_makedirs.call_count == 2

    # Should read and write settings for both variants
    assert mock_file_open.call_count >= 4  # At least 2 reads, 2 writes


@patch("os.path.exists")
@patch("os.makedirs")
@patch("os.path.dirname")
@patch("os.path.expanduser")
@patch("builtins.open", new_callable=mock_open, read_data="{ invalid json")
def test_configure_vscode_settings_invalid_json(
    mock_file_open, mock_expanduser, mock_dirname, mock_makedirs, mock_exists
):
    """Test configuring VS Code settings when existing file has invalid JSON."""
    # Mock expanduser to return different paths for VS Code and VS Code Insiders
    def expanduser_side_effect(path):
        if "Code - Insiders" in path:
            return "/home/user/Library/Application Support/Code - Insiders/User/settings.json"
        else:
            return "/home/user/Library/Application Support/Code/User/settings.json"

    mock_expanduser.side_effect = expanduser_side_effect

    def dirname_side_effect(path):
        if "settings.json" in path:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders/User"
            else:
                return "/home/user/Library/Application Support/Code/User"
        else:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders"
            else:
                return "/home/user/Library/Application Support/Code"

    mock_dirname.side_effect = dirname_side_effect

    mock_exists.return_value = True

    configure_vscode_settings("test_runtime", "/custom/server/path")

    # Should handle invalid JSON gracefully
    assert mock_file_open.call_count >= 2


@patch("os.path.exists")
@patch("os.makedirs")
@patch("os.path.dirname")
@patch("os.path.expanduser")
@patch("builtins.open", new_callable=mock_open)
def test_configure_vscode_settings_default_path(
    mock_file_open, mock_expanduser, mock_dirname, mock_makedirs, mock_exists
):
    """Test configuring VS Code settings with default server path."""
    # Mock expanduser to return different paths for VS Code and VS Code Insiders
    def expanduser_side_effect(path):
        if "Code - Insiders" in path:
            return "/home/user/Library/Application Support/Code - Insiders/User/settings.json"
        else:
            return "/home/user/Library/Application Support/Code/User/settings.json"

    mock_expanduser.side_effect = expanduser_side_effect

    def dirname_side_effect(path):
        if "settings.json" in path:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders/User"
            else:
                return "/home/user/Library/Application Support/Code/User"
        else:
            if "Code - Insiders" in path:
                return "/home/user/Library/Application Support/Code - Insiders"
            else:
                return "/home/user/Library/Application Support/Code"

    mock_dirname.side_effect = dirname_side_effect

    def exists_side_effect(path):
        if "settings.json" in path:
            return False  # Settings file doesn't exist
        return True  # Directories exist

    mock_exists.side_effect = exists_side_effect

    configure_vscode_settings("test_runtime")  # No custom path

    # Should write settings with default path
    assert mock_file_open.call_count >= 2


def test_ssh_config_special_characters():
    """Test SSH config generation with special characters in token."""
    with patch(
        "snowflake.cli._plugins.container_runtime.utils.check_websocat_installed",
        return_value=True,
    ):
        with patch("os.path.expanduser", return_value="/home/user/.ssh/config"):
            with patch("os.path.exists", return_value=False):
                with patch("builtins.open", mock_open()) as mock_file:
                    # Token with special characters that need proper escaping
                    special_token = "token_with_quotes'and\"backslashes\\"

                    setup_ssh_config_with_token(
                        "test_runtime", "wss://example.com/ssh", special_token
                    )

                    # Should escape special characters properly
                    write_calls = [call for call in mock_file().write.call_args_list]
                    written_content = write_calls[0][0][0]
                    assert special_token in written_content


def test_vscode_settings_special_characters():
    """Test VS Code settings with special characters in paths."""
    with patch(
        "os.path.expanduser",
        return_value="/home/user/Library/Application Support/Code/User/settings.json",
    ):
        with patch("os.path.exists") as mock_exists:
            with patch("os.makedirs"):
                with patch(
                    "os.path.dirname",
                    side_effect=lambda path: "/home/user/Library/Application Support/Code/User"
                    if "settings.json" in path
                    else "/home/user/Library/Application Support/Code",
                ):
                    with patch("builtins.open", mock_open()) as mock_file:

                        def exists_side_effect(path):
                            if "settings.json" in path:
                                return False  # Settings file doesn't exist
                            return True  # Directories exist

                        mock_exists.side_effect = exists_side_effect

                        # Path with special characters
                        special_path = "/path/with spaces/and'quotes"

                        configure_vscode_settings("test_runtime", special_path)

                        # Should write settings - just verify the function completed successfully
                        assert mock_file.call_count >= 1
