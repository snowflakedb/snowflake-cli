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
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.remote.constants import ComputeResources, SnowflakeCloudType
from snowflake.cli._plugins.remote.utils import (
    ImageSpec,
    format_stage_path,
    get_current_region_id,
    get_node_resources,
    get_regions,
    validate_stage_path,
)


class TestImageSpec:
    """Test ImageSpec dataclass."""

    def test_image_spec_full_name(self):
        """Test ImageSpec full_name property."""
        resources = ComputeResources(cpu=2, memory=8, gpu=0)

        spec = ImageSpec(
            repo="/snowflake/images/snowflake_images",
            image_name="st_plat/runtime/x86/runtime_image/snowbooks",
            image_tag="1.7.1",
            resource_requests=resources,
            resource_limits=resources,
        )

        expected_full_name = "/snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:1.7.1"
        assert spec.full_name == expected_full_name


class TestRegionUtils:
    """Test region-related utility functions."""

    def test_get_regions_with_region_group(self):
        """Test get_regions with region groups."""
        mock_session = Mock()
        mock_regions = [
            Mock(
                region_group="aws-us-east-1",
                snowflake_region="us-east-1",
                cloud="aws",
                region="us-east-1",
                display_name="US East (N. Virginia)",
            ),
            Mock(
                region_group="azure-west-us-2",
                snowflake_region="west-us-2",
                cloud="azure",
                region="west-us-2",
                display_name="West US 2",
            ),
        ]
        mock_session.sql.return_value.collect.return_value = mock_regions

        with patch("snowflake.cli.api.console.cli_console") as mock_console:
            regions = get_regions(mock_session)

            assert "aws-us-east-1.us-east-1" in regions
            assert "azure-west-us-2.west-us-2" in regions

            aws_region = regions["aws-us-east-1.us-east-1"]
            assert aws_region["region_group"] == "aws-us-east-1"
            assert aws_region["snowflake_region"] == "us-east-1"
            assert aws_region["cloud"] == SnowflakeCloudType.AWS
            assert aws_region["region"] == "us-east-1"
            assert aws_region["display_name"] == "US East (N. Virginia)"

    def test_get_regions_without_region_group(self):
        """Test get_regions without region groups."""
        mock_session = Mock()
        mock_regions = [
            Mock(
                region_group=None,
                snowflake_region="us-central1",
                cloud="gcp",
                region="us-central1",
                display_name="US Central 1",
            )
        ]
        # Mock hasattr to return False for region_group
        for region in mock_regions:
            region.region_group = None

        mock_session.sql.return_value.collect.return_value = mock_regions

        with patch("snowflake.cli.api.console.cli_console") as mock_console:
            regions = get_regions(mock_session)

            assert "us-central1" in regions

            gcp_region = regions["us-central1"]
            assert gcp_region["region_group"] is None
            assert gcp_region["snowflake_region"] == "us-central1"
            assert gcp_region["cloud"] == SnowflakeCloudType.GCP
            assert gcp_region["region"] == "us-central1"
            assert gcp_region["display_name"] == "US Central 1"

    def test_get_current_region_id(self):
        """Test get_current_region_id function."""
        mock_session = Mock()
        mock_result = Mock()
        mock_result.CURRENT_REGION = "us-east-1"
        mock_session.sql.return_value.collect.return_value = [mock_result]

        region_id = get_current_region_id(mock_session)

        assert region_id == "us-east-1"
        mock_session.sql.assert_called_once_with(
            "SELECT CURRENT_REGION() AS CURRENT_REGION"
        )


class TestNodeResources:
    """Test get_node_resources function."""

    def test_get_node_resources_common_instance_family(self):
        """Test get_node_resources with common instance family."""
        mock_session = Mock()

        # Mock compute pool query
        mock_pool_result = Mock()
        mock_pool_result.__getitem__ = (
            lambda self, key: "CPU_X64_S" if key == "instance_family" else None
        )
        mock_session.sql.return_value.collect.return_value = [mock_pool_result]

        with patch(
            "snowflake.cli._plugins.remote.utils.get_regions"
        ) as mock_get_regions:
            with patch(
                "snowflake.cli._plugins.remote.utils.get_current_region_id"
            ) as mock_get_region_id:
                with patch("snowflake.cli.api.console.cli_console") as mock_console:
                    # Mock region data
                    mock_get_region_id.return_value = "us-east-1"
                    mock_get_regions.return_value = {
                        "us-east-1": {"cloud": SnowflakeCloudType.AWS}
                    }

                    resources = get_node_resources(mock_session, "test_pool")

                    # Should return common instance family resources
                    assert isinstance(resources, ComputeResources)
                    assert resources.cpu > 0
                    assert resources.memory > 0

    def test_get_node_resources_cloud_specific_instance_family(self):
        """Test get_node_resources with cloud-specific instance family."""
        mock_session = Mock()

        # Mock compute pool query with cloud-specific instance family
        mock_pool_result = Mock()
        mock_pool_result.__getitem__ = (
            lambda self, key: "GPU_NV_S" if key == "instance_family" else None
        )
        mock_session.sql.return_value.collect.return_value = [mock_pool_result]

        with patch(
            "snowflake.cli._plugins.remote.utils.get_regions"
        ) as mock_get_regions:
            with patch(
                "snowflake.cli._plugins.remote.utils.get_current_region_id"
            ) as mock_get_region_id:
                with patch("snowflake.cli.api.console.cli_console") as mock_console:
                    # Mock region data
                    mock_get_region_id.return_value = "us-east-1"
                    mock_get_regions.return_value = {
                        "us-east-1": {"cloud": SnowflakeCloudType.AWS}
                    }

                    resources = get_node_resources(mock_session, "gpu_pool")

                    # Should return cloud-specific instance family resources
                    assert isinstance(resources, ComputeResources)
                    assert resources.gpu > 0  # GPU instance should have GPU resources

    def test_get_node_resources_compute_pool_not_found(self):
        """Test get_node_resources when compute pool is not found."""
        mock_session = Mock()
        mock_session.sql.return_value.collect.return_value = []  # Empty result

        with pytest.raises(
            ValueError, match="Compute pool 'nonexistent_pool' not found"
        ):
            get_node_resources(mock_session, "nonexistent_pool")


class TestStagePathUtils:
    """Test stage path validation and formatting utilities."""

    def test_validate_stage_path_valid(self):
        """Test validate_stage_path with valid paths."""
        valid_paths = [
            "@my_stage",
            "@db.schema.stage",
            "@stage_with_underscores",
            "@stage-with-dashes",
            "@stage123",
            "@UPPERCASE_STAGE",
        ]

        for path in valid_paths:
            assert validate_stage_path(path) is True

    def test_validate_stage_path_invalid(self):
        """Test validate_stage_path with invalid paths."""
        invalid_paths = [
            "my_stage",  # Missing @
            "stage",  # Missing @
            "",  # Empty
            "   ",  # Whitespace only
            "db.schema.stage",  # Missing @
        ]

        for path in invalid_paths:
            assert validate_stage_path(path) is False

    def test_format_stage_path_add_prefix(self):
        """Test format_stage_path adds @ prefix when missing."""
        test_cases = [
            ("my_stage", "@my_stage"),
            ("db.schema.stage", "@db.schema.stage"),
            ("stage_name", "@stage_name"),
            ("UPPERCASE", "@UPPERCASE"),
        ]

        for input_path, expected in test_cases:
            assert format_stage_path(input_path) == expected

    def test_format_stage_path_remove_trailing_slash(self):
        """Test format_stage_path removes trailing slashes."""
        test_cases = [
            ("@my_stage/", "@my_stage"),
            ("@my_stage//", "@my_stage"),
            ("my_stage/", "@my_stage"),
            ("@db.schema.stage/", "@db.schema.stage"),
        ]

        for input_path, expected in test_cases:
            assert format_stage_path(input_path) == expected

    def test_format_stage_path_already_formatted(self):
        """Test format_stage_path with already properly formatted paths."""
        properly_formatted = [
            "@my_stage",
            "@db.schema.stage",
            "@stage_name",
            "@UPPERCASE_STAGE",
        ]

        for path in properly_formatted:
            assert format_stage_path(path) == path


class TestSSHUtils:
    """Test SSH-related utility functions."""

    def test_extract_hostname_from_endpoint_invalid(self):
        """Test hostname extraction from invalid URLs."""
        from snowflake.cli._plugins.remote.utils import _extract_hostname_from_endpoint

        # Test with non-wss URL
        with pytest.raises(ValueError, match="Invalid SSH endpoint URL format"):
            _extract_hostname_from_endpoint("https://example.com")

        # Test with completely invalid URL
        with pytest.raises(ValueError, match="Invalid SSH endpoint URL format"):
            _extract_hostname_from_endpoint("ftp://invalid.url")


class TestSSHKeyManagement:
    """Test SSH key generation and management."""

    @patch("subprocess.run")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.unlink")
    @patch("pathlib.Path.chmod")
    @patch("builtins.open")
    def test_generate_ssh_key_pair_success(
        self, mock_open, mock_chmod, mock_unlink, mock_exists, mock_mkdir, mock_run
    ):
        """Test successful SSH key pair generation."""
        from snowflake.cli._plugins.remote.utils import generate_ssh_key_pair

        # Setup mocks
        mock_exists.return_value = True  # Keys exist to be removed
        mock_open.return_value.__enter__.return_value.read.return_value = (
            "ssh-ed25519 AAAAB3... test@host"
        )
        mock_run.return_value = Mock()

        result = generate_ssh_key_pair("test_service")

        # Check results
        assert len(result) == 2
        private_path, public_key = result
        # Use os.path.join for cross-platform path checking
        expected_path_part = os.path.join("snowflake-remote", "test_service")
        assert expected_path_part in private_path
        assert public_key == "ssh-ed25519 AAAAB3... test@host"

        # Verify SSH key generation command
        expected_cmd = [
            "ssh-keygen",
            "-t",
            "ed25519",
            "-f",
            private_path,
            "-N",
            "",
            "-C",
            "snowflake-remote-test_service",
        ]
        mock_run.assert_called_once()
        actual_cmd = mock_run.call_args[0][0]
        assert actual_cmd[:2] == expected_cmd[:2]  # ssh-keygen -t
        assert actual_cmd[2:4] == expected_cmd[2:4]  # ed25519 -f

    @patch("subprocess.run")
    def test_generate_ssh_key_pair_command_failure(self, mock_run):
        """Test SSH key generation when ssh-keygen command fails."""
        from snowflake.cli._plugins.remote.utils import generate_ssh_key_pair

        mock_run.side_effect = subprocess.CalledProcessError(
            1, "ssh-keygen", stderr="error"
        )

        with pytest.raises(RuntimeError, match="Failed to generate SSH key pair"):
            generate_ssh_key_pair("test_service")

    @patch("subprocess.run")
    def test_generate_ssh_key_pair_command_not_found(self, mock_run):
        """Test SSH key generation when ssh-keygen is not found."""
        from snowflake.cli._plugins.remote.utils import generate_ssh_key_pair

        mock_run.side_effect = FileNotFoundError()

        with pytest.raises(RuntimeError, match="ssh-keygen command not found"):
            generate_ssh_key_pair("test_service")

    @patch("pathlib.Path.exists")
    @patch("builtins.open")
    def test_get_existing_ssh_key_success(self, mock_open, mock_exists):
        """Test getting existing SSH key when files exist."""
        from snowflake.cli._plugins.remote.utils import get_existing_ssh_key

        # Mock both private and public key files exist
        mock_exists.return_value = True
        mock_open.return_value.__enter__.return_value.read.return_value = (
            "ssh-ed25519 AAAAB3... test@host"
        )

        result = get_existing_ssh_key("test_service")

        assert result is not None
        private_path, public_key = result
        # Use os.path.join for cross-platform path checking
        expected_path_part = os.path.join("snowflake-remote", "test_service")
        assert expected_path_part in private_path
        assert public_key == "ssh-ed25519 AAAAB3... test@host"

    @patch("pathlib.Path.exists")
    def test_get_existing_ssh_key_not_found(self, mock_exists):
        """Test getting existing SSH key when files don't exist."""
        from snowflake.cli._plugins.remote.utils import get_existing_ssh_key

        mock_exists.return_value = False

        result = get_existing_ssh_key("test_service")

        assert result is None

    @patch("pathlib.Path.exists")
    @patch("builtins.open")
    def test_get_existing_ssh_key_read_error(self, mock_open, mock_exists):
        """Test getting existing SSH key when read fails."""
        from snowflake.cli._plugins.remote.utils import get_existing_ssh_key

        mock_exists.return_value = True
        mock_open.side_effect = IOError("Permission denied")

        result = get_existing_ssh_key("test_service")

        assert result is None


class TestSSHConfigManagement:
    """Test SSH configuration file management with real temporary files."""

    @patch("shutil.which")
    def test_setup_ssh_config_new_entry(self, mock_which):
        """Test setting up SSH config for new service with real file."""
        from snowflake.cli._plugins.remote.utils import setup_ssh_config_with_token

        mock_which.return_value = "/usr/bin/websocat"

        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            # Patch Path.home() to return a temporary directory with our config file
            temp_dir = Path(temp_file.name).parent
            temp_ssh_dir = temp_dir / "test_ssh"
            temp_ssh_dir.mkdir(exist_ok=True)
            temp_config = temp_ssh_dir / "config"
            temp_config.write_text("")  # Create empty config file

            with patch("pathlib.Path.home", return_value=temp_dir / "test_home"):
                # Create the .ssh directory structure
                (temp_dir / "test_home" / ".ssh").mkdir(parents=True, exist_ok=True)
                ssh_config_path = temp_dir / "test_home" / ".ssh" / "config"
                setup_ssh_config_with_token(
                    service_name="test_service",
                    ssh_endpoint_url="wss://example.snowflakecomputing.app/",
                    token="test_token_123",
                    private_key_path="/path/to/key",
                )

                # Read the actual file content
                content = ssh_config_path.read_text()

                # Verify the content contains expected SSH config
                assert "Host test_service" in content
                assert "HostName example.snowflakecomputing.app" in content
                assert (
                    "ProxyCommand /usr/bin/websocat --binary wss://example.snowflakecomputing.app/"
                    in content
                )
                assert "test_token_123" in content
                assert "IdentityFile /path/to/key" in content
                assert "PubkeyAuthentication yes" in content

    @patch("shutil.which")
    def test_setup_ssh_config_update_existing(self, mock_which):
        """Test updating existing SSH config entry with real file."""
        from snowflake.cli._plugins.remote.utils import setup_ssh_config_with_token

        mock_which.return_value = "/usr/bin/websocat"

        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            # Write initial config
            initial_config = """Host other_service
  HostName other.example.com
  Port 22

Host test_service
  HostName old.example.com
  Port 22
  User root

Host another_service
  HostName another.example.com
  Port 22"""
            temp_file.write(initial_config)
            temp_file.flush()

            # Patch Path.home() to use our temporary directory
            temp_dir = Path(temp_file.name).parent
            with patch("pathlib.Path.home", return_value=temp_dir / "test_home"):
                # Create the .ssh directory and copy our temp file as config
                ssh_dir = temp_dir / "test_home" / ".ssh"
                ssh_dir.mkdir(parents=True, exist_ok=True)
                ssh_config_path = ssh_dir / "config"
                ssh_config_path.write_text(initial_config)

                setup_ssh_config_with_token(
                    service_name="test_service",
                    ssh_endpoint_url="wss://new.snowflakecomputing.app/",
                    token="new_token_456",
                    private_key_path=None,  # No key this time
                )

                # Read the actual file content
                content = ssh_config_path.read_text()

                # Verify the old config was replaced
                assert "old.example.com" not in content
                assert "new.snowflakecomputing.app" in content
                assert "new_token_456" in content
                assert "PubkeyAuthentication no" in content  # No key authentication
                assert "other_service" in content  # Other entries preserved
                assert "another_service" in content

    @patch("shutil.which")
    def test_setup_ssh_config_websocat_not_found(self, mock_which):
        """Test setup SSH config when websocat is not installed."""
        from snowflake.cli._plugins.remote.utils import setup_ssh_config_with_token

        mock_which.return_value = None

        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            temp_dir = Path(temp_file.name).parent
            with patch("pathlib.Path.home", return_value=temp_dir / "test_home"):
                # Create the .ssh directory structure
                ssh_dir = temp_dir / "test_home" / ".ssh"
                ssh_dir.mkdir(parents=True, exist_ok=True)
                ssh_config_path = ssh_dir / "config"
                ssh_config_path.write_text("")  # Empty config

                # Should return early without modifying file
                setup_ssh_config_with_token(
                    service_name="test_service",
                    ssh_endpoint_url="wss://example.com/",
                    token="test_token",
                )

                # Verify file was not modified
                content = ssh_config_path.read_text()
                assert content == ""  # File should remain empty

    def test_cleanup_ssh_config_success(self):
        """Test successful SSH config cleanup with real file."""
        from snowflake.cli._plugins.remote.utils import cleanup_ssh_config

        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            # Write config with the service to remove
            existing_config = """Host other_service
  HostName other.com
  Port 22

Host test_service
  HostName example.com
  Port 22
  User root
  ProxyCommand websocat wss://example.com/

Host another_service
  HostName another.com
  Port 22"""
            temp_file.write(existing_config)
            temp_file.flush()

            # Patch Path.home() to use our temporary directory
            temp_dir = Path(temp_file.name).parent
            with patch("pathlib.Path.home", return_value=temp_dir / "test_home"):
                # Create the .ssh directory and copy our temp file as config
                ssh_dir = temp_dir / "test_home" / ".ssh"
                ssh_dir.mkdir(parents=True, exist_ok=True)
                ssh_config_path = ssh_dir / "config"
                ssh_config_path.write_text(existing_config)

                cleanup_ssh_config("test_service")

                # Read the actual file content
                content = ssh_config_path.read_text()

                # Verify the service section was removed
                assert "test_service" not in content
                assert "example.com" not in content
                assert "ProxyCommand websocat wss://example.com/" not in content
                # Verify other services remain
                assert "other_service" in content
                assert "another_service" in content

    def test_cleanup_ssh_config_no_file(self):
        """Test SSH config cleanup when no config file exists."""
        from snowflake.cli._plugins.remote.utils import cleanup_ssh_config

        # Use a non-existent home directory
        with patch("pathlib.Path.home", return_value=Path("/tmp/non_existent_home")):
            # Should return early without error
            cleanup_ssh_config("test_service")
