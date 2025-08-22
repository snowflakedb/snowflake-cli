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
import platform
import re
import shlex
import shutil
import stat
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, cast

import requests

# Required is not essential for runtime, so we'll use a simpler approach
from snowflake import snowpark
from snowflake.cli._plugins.remote.constants import (
    CLOUD_INSTANCE_FAMILIES,
    COMMON_INSTANCE_FAMILIES,
    DEFAULT_ENDPOINT_TIMEOUT_MINUTES,
    ENDPOINT_CHECK_INTERVAL_SECONDS,
    ENDPOINT_REQUEST_TIMEOUT_SECONDS,
    SSH_CONFIG_FILENAME,
    SSH_DEFAULT_PORT,
    SSH_DEFAULT_USER,
    SSH_DIR_NAME,
    SSH_KEY_SUBDIR_NAME,
    ComputeResources,
    SnowflakeCloudType,
)
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import CliError
from typing_extensions import TypedDict

log = logging.getLogger(__name__)


def validate_service_name(service_name: str) -> None:
    """
    Validate service name according to Snowflake CREATE SERVICE requirements.

    Based on Snowflake documentation (https://docs.snowflake.com/en/sql-reference/sql/create-service#required-parameters):
    - Quoted names for special characters or case-sensitive names are not supported
    - Service names must be valid SQL identifiers without quotes

    Args:
        service_name: The service name to validate

    Raises:
        CliError: If the service name is invalid
    """
    if not service_name:
        raise CliError("Service name cannot be empty")

    # Check for quotes (not allowed)
    if '"' in service_name or "'" in service_name or "`" in service_name:
        raise CliError(
            f"Invalid service name '{service_name}': quoted names are not supported. "
            "Service names cannot contain quotes."
        )

    # Check for invalid characters (only alphanumeric, underscores allowed)
    # Snowflake identifiers can contain letters, digits, and underscores
    if not re.match(r"^[A-Za-z0-9_]+$", service_name):
        raise CliError(
            f"Invalid service name '{service_name}': only alphanumeric characters and underscores are allowed. "
            "Special characters and spaces are not supported."
        )

    # Check if it starts with a letter or underscore (SQL identifier requirement)
    if not re.match(r"^[A-Za-z_]", service_name):
        raise CliError(
            f"Invalid service name '{service_name}': service name must start with a letter or underscore."
        )

    # Check length (Snowflake identifier limit is 255 characters)
    if len(service_name) > 255:
        raise CliError(
            f"Invalid service name '{service_name}': service name cannot exceed 255 characters. "
            f"Current length: {len(service_name)}"
        )


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
        raise CliError(f"Compute pool '{compute_pool}' not found")

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


def format_stage_path(stage_path: str) -> str:
    """
    Format and normalize a stage path.

    Handles both @ prefixed paths and SnowURL paths (snow://).
    Adds @ prefix if missing, removes trailing slashes.

    TODO: Consider adding validation for invalid stage path formats (e.g., invalid
    Snowflake identifiers) in the future if necessary. Currently we rely on
    Snowflake's backend to handle detailed identifier validation.

    Args:
        stage_path: Stage path to normalize

    Returns:
        Normalized stage path

    Raises:
        ValueError: If the stage path is invalid
    """
    if not stage_path or not stage_path.strip():
        raise CliError("Stage path cannot be empty")

    # Remove whitespace first
    path = stage_path.strip()

    # Handle SnowURL paths - check prefix before removing trailing slashes
    if path.startswith("snow://"):
        if len(path) <= len("snow://"):
            raise CliError(
                f"Invalid SnowURL stage path: '{stage_path}' - missing content after snow://"
            )
        return path.rstrip("/")

    # Remove trailing slashes for non-SnowURL paths
    path = path.rstrip("/")

    # Handle @ prefixed paths
    if path.startswith("@"):
        if len(path) <= 1:
            raise CliError(
                f"Invalid @ prefixed stage path: '{stage_path}' - missing stage name after @"
            )
        return path

    # For paths without prefix, add @ prefix
    if not path:
        raise CliError("Stage path cannot be empty")

    return f"@{path}"


def parse_image_string(image_string: str) -> tuple[str, str, str]:
    """
    Parse an image string to extract repository, image name, and tag.

    Docker image naming rules:
    - Repository names can contain lowercase letters, digits, and separators (., -, _)
    - Tags can contain lowercase/uppercase letters, digits, underscores, periods, and dashes
    - Colons (:) are only valid as tag separators, not within names
    - Forward slashes (/) are only valid as repository separators

    Args:
        image_string: Either a full image path (repo/image:tag) or just a tag

    Returns:
        Tuple of (repo, image_name, tag)

    Examples:
        parse_image_string("1.7.1") -> ("", "", "1.7.1")  # Just a tag
        parse_image_string("myimage:latest") -> ("", "myimage", "latest")
        parse_image_string("myrepo/myimage:v1.0") -> ("myrepo", "myimage", "v1.0")
        parse_image_string("registry.com/myrepo/myimage:v1.0") -> ("registry.com/myrepo", "myimage", "v1.0")
    """
    # Split on the last colon to separate tag (if present)
    if ":" in image_string:
        image_path, tag = image_string.rsplit(":", 1)
    else:
        image_path, tag = image_string, ""

    # Split on the last slash to separate repo from image name (if present)
    if "/" in image_path:
        repo, image_name = image_path.rsplit("/", 1)
    else:
        repo, image_name = "", image_path

    # Special case: if we have no repo, no slash, and no colon, treat as just a tag
    # This handles cases like "1.7.1" which should be treated as a tag, not an image name
    if not repo and not "/" in image_string and not ":" in image_string:
        return "", "", image_string

    return repo, image_name, tag


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


def _set_secure_file_permissions(file_path: Path, is_private_key: bool = True) -> None:
    """Set secure file permissions in a cross-platform way.

    Args:
        file_path: Path to the file
        is_private_key: True for private keys (more restrictive), False for public keys
    """

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
        raise CliError(f"Failed to generate SSH key pair: {e.stderr}")
    except FileNotFoundError:
        if platform.system() == "Windows":
            raise CliError(
                "ssh-keygen command not found. Please install OpenSSH client for Windows:\n"
                "1. Windows 10/11: Enable 'OpenSSH Client' in Windows Features, or\n"
                "2. Install Git for Windows (includes OpenSSH), or\n"
                "3. Install OpenSSH from: https://github.com/PowerShell/Win32-OpenSSH/releases"
            )
        else:
            raise CliError(
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


def _generate_ssh_config_lines(
    service_name: str,
    hostname: str,
    websocat_path: str,
    token: str,
    private_key_path: Optional[str],
) -> List[str]:
    """Generate SSH configuration lines for a service."""
    # Properly escape values to prevent command injection
    escaped_websocat_path = shlex.quote(websocat_path)

    # For the token, we need to escape it for shell safety but also ensure it's properly quoted
    # within the Authorization header. We escape the token and then add quotes around it.
    escaped_token = shlex.quote(token)

    config_lines = [
        f"Host {service_name}",
        f"  HostName {hostname}",
        f"  Port     {SSH_DEFAULT_PORT}",
        f"  User     {SSH_DEFAULT_USER}",
        f'  ProxyCommand {escaped_websocat_path} --binary wss://{hostname}/ -H "Authorization: Snowflake Token=\\"{escaped_token}\\""',
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
    ssh_hostname: str,
    token: str,
    private_key_path: Optional[str] = None,
) -> None:
    """Setup SSH configuration for the remote service using token authentication.

    Args:
        service_name: The name of the service
        ssh_hostname: The hostname of the SSH endpoint
        token: The Snowflake authentication token
        private_key_path: Optional path to SSH private key for key-based authentication
    """
    # Check if websocat is installed and get its path
    websocat_path = shutil.which("websocat")
    if not websocat_path:
        raise CliError(
            "websocat is required for SSH connections but is not installed. "
            "Install websocat from: https://github.com/vi/websocat/releases"
        )

    # Generate SSH configuration lines
    config_lines = _generate_ssh_config_lines(
        service_name, ssh_hostname, websocat_path, token, private_key_path
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
            if line.strip() == f"Host {service_name}":
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


def ensure_binary_exists(binary_name: str) -> None:
    """Check if a binary exists in PATH and raise CliError if not found.

    Args:
        binary_name: Name of the binary to check

    Raises:
        CliError: If binary is not found in PATH
    """
    if shutil.which(binary_name) is None:
        raise CliError(
            f"'{binary_name}' is not installed or not found in PATH. "
            "Please install it and try again."
        )


def launch_ide(binary_name: str, service_name: str, remote_path: str) -> None:
    """Launch an IDE connected to the remote service over SSH.

    Args:
        binary_name: IDE binary name ("code" or "cursor")
        service_name: Name of the remote service
        remote_path: Remote path to open in the IDE

    Raises:
        CliError: If IDE binary is not found or launch fails
    """
    ensure_binary_exists(binary_name)

    folder_uri = f"vscode-remote://ssh-remote+{service_name}{remote_path}"
    try:
        result = subprocess.run([binary_name, "--folder-uri", folder_uri], check=False)
        if result.returncode != 0:
            raise CliError(
                f"Failed to launch {binary_name}, exit code: {result.returncode}"
            )
    except CliError:
        # Re-raise our own CliErrors without wrapping them
        raise
    except Exception as e:
        # Only catch unexpected exceptions (file not found, permission errors, etc.)
        raise CliError(f"Failed to launch {binary_name}: {e}")


def validate_endpoint_ready(
    endpoint_url: str,
    auth_token: str,
    endpoint_name: str = "unknown",
    timeout_minutes: int = DEFAULT_ENDPOINT_TIMEOUT_MINUTES,
) -> None:
    """
    Validate that an endpoint is ready and responding to HTTP requests with authentication.

    This utility function checks that an endpoint is not only available but also ready to serve requests
    by making authenticated HTTP GET requests until the endpoint responds successfully.

    Args:
        endpoint_url: Full URL of the endpoint to validate (will be converted to https if needed)
        auth_token: Authentication token for the request
        endpoint_name: Name of the endpoint for logging purposes
        timeout_minutes: Maximum time to wait for endpoint readiness

    Raises:
        CliError: If endpoint doesn't become ready within timeout or authentication fails
    """
    log.debug("Validating endpoint readiness for %s at %s", endpoint_name, endpoint_url)

    timeout_seconds = timeout_minutes * 60
    start_time = time.time()
    attempts = 0

    while time.time() - start_time < timeout_seconds:
        attempts += 1
        try:
            # Make authenticated HTTP GET request to the endpoint
            headers = {
                "Authorization": f'Snowflake Token="{auth_token}"',
                "User-Agent": "snowflake-cli-remote-plugin",
            }

            log.debug(
                "Attempt %d: Checking endpoint readiness at %s", attempts, endpoint_url
            )

            response = requests.get(
                f"https://{endpoint_url}",
                headers=headers,
                timeout=ENDPOINT_REQUEST_TIMEOUT_SECONDS,
            )

            # Check if we got a successful response (2xx status codes)
            if response.status_code >= 200 and response.status_code < 300:
                log.debug(
                    "✓ Endpoint %s is ready! Status: %d, Response size: %d bytes",
                    endpoint_name,
                    response.status_code,
                    len(response.content),
                )
                return

            log.debug(
                "Endpoint %s not ready yet. Status: %d, retrying in %d seconds...",
                endpoint_name,
                response.status_code,
                ENDPOINT_CHECK_INTERVAL_SECONDS,
            )

        except Exception as e:
            log.debug(
                "Error to %s endpoint (attempt %d): %s, retrying...",
                endpoint_name,
                attempts,
                str(e),
            )

        # Wait before next attempt
        time.sleep(ENDPOINT_CHECK_INTERVAL_SECONDS)

    # If we get here, we've timed out
    elapsed_minutes = (time.time() - start_time) / 60
    raise CliError(
        f"Endpoint {endpoint_name} did not become ready within {timeout_minutes} minutes "
        f"(tried for {elapsed_minutes:.1f} minutes with {attempts} attempts). "
        f"The service container may still be initializing. Try `snow remote list` to get the status of the service."
    )
