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

"""Utilities for the remote development environment plugin."""

import logging
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TypedDict, cast

# Required is not essential for runtime, so we'll use a simpler approach
from snowflake import snowpark
from snowflake.cli._plugins.remote.constants import (
    CLOUD_INSTANCE_FAMILIES,
    COMMON_INSTANCE_FAMILIES,
    SSH_CONFIG_FILENAME,
    SSH_DIR_NAME,
    SSH_KEY_SUBDIR_NAME,
    ComputeResources,
    SnowflakeCloudType,
)
from snowflake.cli.api.console import cli_console as cc

log = logging.getLogger(__name__)


class SnowflakeRegion(TypedDict):
    region_group: Optional[str]
    snowflake_region: str
    cloud: SnowflakeCloudType
    region: str
    display_name: str


@dataclass(frozen=True)
class ImageSpec:
    repo: str
    image_name: str
    image_tag: str
    resource_requests: ComputeResources
    resource_limits: ComputeResources

    @property
    def full_name(self) -> str:
        return f"{self.repo}/{self.image_name}:{self.image_tag}"


def get_regions(session: snowpark.Session) -> Dict[str, SnowflakeRegion]:
    """Get available Snowflake regions."""
    res = session.sql("SHOW REGIONS").collect()
    log.debug("Getting regions: %s", res)
    res_dict = {}
    for r in res:
        if hasattr(r, "region_group") and r.region_group:
            key = f"{r.region_group}.{r.snowflake_region}"
            res_dict[key] = SnowflakeRegion(
                region_group=r.region_group,
                snowflake_region=r.snowflake_region,
                cloud=SnowflakeCloudType.from_value(r.cloud),
                region=r.region,
                display_name=r.display_name,
            )
        else:
            key = r.snowflake_region
            res_dict[key] = SnowflakeRegion(
                region_group=None,
                snowflake_region=r.snowflake_region,
                cloud=SnowflakeCloudType.from_value(r.cloud),
                region=r.region,
                display_name=r.display_name,
            )

    return res_dict


def get_current_region_id(session: snowpark.Session) -> str:
    """Get the current Snowflake region ID."""
    res = session.sql("SELECT CURRENT_REGION() AS CURRENT_REGION").collect()[0]
    return cast(str, res.CURRENT_REGION)


def get_node_resources(
    session: snowpark.Session, compute_pool: str
) -> ComputeResources:
    """Extract resource information for the specified compute pool."""
    # Get the instance family
    rows = session.sql(f"show compute pools like '{compute_pool}'").collect()
    if not rows:
        raise ValueError(f"Compute pool '{compute_pool}' not found")

    instance_family: str = rows[0]["instance_family"]
    log.debug("get instance family %s resources", instance_family)

    # Get the cloud we're using (AWS, Azure, etc)
    region = get_regions(session)[get_current_region_id(session)]
    cloud = region["cloud"]

    log.debug("get cloud %s instance family %s resources", cloud, instance_family)

    return (
        COMMON_INSTANCE_FAMILIES.get(instance_family)
        or CLOUD_INSTANCE_FAMILIES[cloud][instance_family]
    )


def validate_stage_path(path: str) -> bool:
    """Validate if a string is a valid Snowflake stage path."""
    return path.startswith("@")


def format_stage_path(stage_path: str) -> str:
    """
    Format and normalize a stage path.

    Args:
        stage_path: Stage path to normalize

    Returns:
        Normalized stage path
    """
    # Remove trailing slashes
    path = stage_path.rstrip("/")

    # Ensure it starts with @
    if not path.startswith("@"):
        path = f"@{path}"

    return path


def get_ssh_key_paths(service_name: str) -> Tuple[Path, Path]:
    """Get the SSH key file paths for a service.

    Args:
        service_name: Name of the service

    Returns:
        Tuple of (private_key_path, public_key_path)
    """
    # Use pathlib.Path.home() for cross-platform home directory resolution
    ssh_key_dir = Path.home() / SSH_DIR_NAME / SSH_KEY_SUBDIR_NAME
    private_key_path = ssh_key_dir / service_name
    public_key_path = ssh_key_dir / f"{service_name}.pub"
    return private_key_path, public_key_path


def install_websocat_instructions() -> str:
    """Return instructions for installing websocat."""
    return "Install websocat from: https://github.com/vi/websocat/releases"


def _set_secure_file_permissions(file_path: Path, is_private_key: bool = True) -> None:
    """Set secure file permissions in a cross-platform way.

    Args:
        file_path: Path to the file
        is_private_key: True for private keys (more restrictive), False for public keys
    """
    import platform
    import stat

    if platform.system() == "Windows":
        # On Windows, ssh-keygen already sets appropriate permissions by default.
        # We only need to ensure the file isn't world-writable, which is rarely an issue.
        # Windows file permissions work differently than Unix, and OpenSSH on Windows
        # handles this correctly without additional intervention.
        try:
            # Just ensure the file isn't read-only if it's a private key we might need to delete later
            current_mode = file_path.stat().st_mode
            if current_mode & stat.S_IWRITE == 0:  # If read-only
                file_path.chmod(current_mode | stat.S_IWRITE)  # Make writable
        except (OSError, AttributeError):
            # If this fails, it's not critical - log debug message only
            log.debug(
                "Could not adjust permissions on %s (this is usually fine on Windows)",
                file_path,
            )
    else:
        # Unix-like systems: use traditional octal permissions
        if is_private_key:
            file_path.chmod(0o600)  # rw-------
        else:
            file_path.chmod(0o644)  # rw-r--r--


def _set_secure_directory_permissions(dir_path: Path) -> None:
    """Set secure directory permissions in a cross-platform way.

    Args:
        dir_path: Path to the directory
    """
    import platform

    if platform.system() == "Windows":
        # On Windows, default directory permissions are usually fine.
        # The SSH directory is typically created in the user's home directory
        # which already has appropriate access controls.
        log.debug("Using default Windows permissions for SSH directory: %s", dir_path)
    else:
        # Unix-like systems: use traditional octal permissions
        dir_path.chmod(0o700)  # rwx------


def generate_ssh_key_pair(
    service_name: str, key_type: str = "ed25519"
) -> Tuple[str, str]:
    """
    Generate SSH key pair for the remote service.

    Args:
        service_name: The name of the service
        key_type: Type of SSH key to generate (ed25519, rsa, ecdsa)

    Returns:
        Tuple of (private_key_path, public_key_content)
    """
    # Create SSH key directory if it doesn't exist
    ssh_key_dir = Path.home() / SSH_DIR_NAME / SSH_KEY_SUBDIR_NAME
    ssh_key_dir.mkdir(parents=True, exist_ok=True)

    # Set secure permissions on the directory (cross-platform)
    _set_secure_directory_permissions(ssh_key_dir)

    # Get key file paths
    private_key_path, public_key_path = get_ssh_key_paths(service_name)

    # Remove existing keys if they exist
    if private_key_path.exists():
        private_key_path.unlink()
    if public_key_path.exists():
        public_key_path.unlink()

    # Generate the SSH key pair
    try:
        cmd = [
            "ssh-keygen",
            "-t",
            key_type,
            "-f",
            str(private_key_path),
            "-N",
            "",  # No passphrase
            "-C",
            f"snowflake-remote-{service_name}",
        ]

        subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )

        # Set proper permissions (cross-platform)
        _set_secure_file_permissions(private_key_path, is_private_key=True)
        _set_secure_file_permissions(public_key_path, is_private_key=False)

        # Read the public key content
        with open(public_key_path, "r") as f:
            public_key_content = f.read().strip()

        cc.step(f"🔑 Generated SSH key pair for service '{service_name}'")
        log.debug("   Private key: %s", private_key_path)
        log.debug("   Public key: %s", public_key_path)

        return str(private_key_path), public_key_content

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to generate SSH key pair: {e.stderr}")
    except FileNotFoundError:
        import platform

        if platform.system() == "Windows":
            raise RuntimeError(
                "ssh-keygen command not found. Please install OpenSSH client for Windows:\n"
                "1. Windows 10/11: Enable 'OpenSSH Client' in Windows Features, or\n"
                "2. Install Git for Windows (includes OpenSSH), or\n"
                "3. Install OpenSSH from: https://github.com/PowerShell/Win32-OpenSSH/releases"
            )
        else:
            raise RuntimeError(
                "ssh-keygen command not found. Please install OpenSSH client."
            )


def get_existing_ssh_key(service_name: str) -> Optional[Tuple[str, str]]:
    """
    Get existing SSH key pair for the service if it exists.

    Args:
        service_name: The name of the service

    Returns:
        Tuple of (private_key_path, public_key_content) or None if not found
    """
    private_key_path, public_key_path = get_ssh_key_paths(service_name)

    if private_key_path.exists() and public_key_path.exists():
        try:
            with open(public_key_path, "r") as f:
                public_key_content = f.read().strip()
            return str(private_key_path), public_key_content
        except IOError:
            return None

    return None


def _extract_hostname_from_endpoint(ssh_endpoint_url: str) -> str:
    """Extract hostname from SSH endpoint URL."""
    # Expected format: wss://hostname.domain
    match = re.match(r"wss://([^/]+)", ssh_endpoint_url)
    if not match:
        raise ValueError(f"Invalid SSH endpoint URL format: {ssh_endpoint_url}")

    return match.group(1)


def _generate_ssh_config_lines(
    service_name: str,
    hostname: str,
    websocat_path: str,
    token: str,
    private_key_path: Optional[str],
) -> List[str]:
    """Generate SSH configuration lines for a service."""
    config_lines = [
        f"Host {service_name}",
        f"  HostName {hostname}",
        f"  Port     22",
        f"  User     root",
        f'  ProxyCommand {websocat_path} --binary wss://{hostname}/ -H "Authorization: Snowflake Token=\\"{token}\\""',
    ]

    if private_key_path:
        config_lines.extend(
            [
                f"  IdentityFile {private_key_path}",
                f"  IdentitiesOnly yes",
                f"  PubkeyAuthentication yes",
                f"  PasswordAuthentication no",
                f"  StrictHostKeyChecking no",
                f"  UserKnownHostsFile /dev/null",
            ]
        )
    else:
        config_lines.extend(
            [
                f"  PasswordAuthentication no",
                f"  PubkeyAuthentication no",
                f"  StrictHostKeyChecking no",
                f"  UserKnownHostsFile /dev/null",
            ]
        )

    return config_lines


def setup_ssh_config_with_token(
    service_name: str,
    ssh_endpoint_url: str,
    token: str,
    private_key_path: Optional[str] = None,
) -> None:
    """Setup SSH configuration for the remote service using token authentication.

    Args:
        service_name: The name of the service
        ssh_endpoint_url: The URL of the SSH endpoint
        token: The Snowflake authentication token
        private_key_path: Optional path to SSH private key for key-based authentication
    """
    # Check if websocat is installed and get its path
    websocat_path = shutil.which("websocat")
    if not websocat_path:
        cc.step("Please install websocat manually and run this command again.")
        cc.step(install_websocat_instructions())
        return

    # Extract hostname from endpoint URL
    hostname = _extract_hostname_from_endpoint(ssh_endpoint_url)

    # Generate SSH configuration lines
    config_lines = _generate_ssh_config_lines(
        service_name, hostname, websocat_path, token, private_key_path
    )

    config_content = "\n" + "\n".join(config_lines) + "\n"

    # Use pathlib for cross-platform SSH config path handling
    ssh_config_path = Path.home() / SSH_DIR_NAME / SSH_CONFIG_FILENAME

    # Check if the config already exists for this host
    existing_config = ""
    host_pattern = re.compile(f"^Host {re.escape(service_name)}$", re.MULTILINE)

    if ssh_config_path.exists():
        existing_config = ssh_config_path.read_text()

    if host_pattern.search(existing_config):
        # Config already exists, update it
        lines = existing_config.splitlines()
        new_lines = []
        skip_until_next_host = False

        for line in lines:
            if line.strip().startswith(f"Host {service_name}"):
                skip_until_next_host = True
                continue
            elif skip_until_next_host and line.strip().startswith("Host "):
                skip_until_next_host = False

            if not skip_until_next_host:
                new_lines.append(line)

        new_config = "\n".join(new_lines) + config_content
    else:
        # Append new config
        new_config = (
            existing_config + config_content if existing_config else config_content
        )

    # Write the updated config using pathlib
    ssh_config_path.write_text(new_config)

    # Log successful file operation
    log.debug("SSH configuration written to %s", ssh_config_path)


def cleanup_ssh_config(service_name: str) -> None:
    """Remove SSH configuration for a service from ~/.ssh/config.

    Args:
        service_name: The name of the service to remove from SSH config
    """
    ssh_config_path = Path.home() / SSH_DIR_NAME / SSH_CONFIG_FILENAME

    if not ssh_config_path.exists():
        return  # Nothing to clean up

    try:
        config_content = ssh_config_path.read_text()

        # Find and remove the host section for this service
        lines = config_content.split("\n")
        new_lines = []
        skip_section = False

        for line in lines:
            # Check if this is the start of our service's host section
            if line.strip() == f"Host {service_name}":
                skip_section = True
                continue

            # Check if this is the start of a different host section
            if line.strip().startswith("Host ") and skip_section:
                skip_section = False
                # Don't skip this line - it's a new host section
                new_lines.append(line)
                continue

            # Skip lines that are part of our service's section
            if skip_section:
                continue

            new_lines.append(line)

        # Write the updated config back using pathlib
        ssh_config_path.write_text("\n".join(new_lines))

        log.debug("Removed SSH configuration for %s", service_name)

    except Exception as e:
        log.warning("Failed to clean up SSH config for %s: %s", service_name, e)
