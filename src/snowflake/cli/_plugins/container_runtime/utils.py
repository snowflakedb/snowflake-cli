import enum
import os
import platform
import re
import subprocess
from dataclasses import dataclass
from pathlib import PurePath
from typing import (
    Dict,
    List,
    Optional,
    TypedDict,
    Union,
    cast,
)

from snowflake.cli.api.console import cli_console as cc
from snowflake.snowpark import session
from typing_extensions import NotRequired, Required

# Constants for SSH configuration
SSH_CONFIG_PATH = "~/.ssh/config"
SSH_HOST_PREFIX = "snowflake-remote-runtime-"


def check_websocat_installed() -> bool:
    """Check if websocat is installed on the system."""
    try:
        subprocess.run(
            ["websocat", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        return True
    except FileNotFoundError:
        return False


def install_websocat_instructions() -> str:
    """Return instructions for installing websocat based on the OS."""
    system = platform.system().lower()
    if system == "darwin":
        return "Install websocat with Homebrew: brew install websocat"
    elif system == "linux":
        return "Install websocat: https://github.com/vi/websocat/releases"
    elif system == "windows":
        return "Install websocat: https://github.com/vi/websocat/releases"
    else:
        return "Install websocat from: https://github.com/vi/websocat/releases"


def install_websocat_macos() -> bool:
    """Install websocat on macOS using Homebrew."""
    try:
        cc.step("Installing websocat via Homebrew...")
        result = subprocess.run(
            ["brew", "install", "websocat"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )
        cc.step("✓ websocat installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        cc.step(f"Failed to install websocat: {e.stderr}")
        return False
    except FileNotFoundError:
        cc.step("Homebrew not found. Please install websocat manually.")
        return False


def setup_ssh_config_with_token(
    service_name: str, ssh_endpoint_url: str, token: str
) -> None:
    """
    Setup SSH configuration for the remote runtime service using token authentication.

    Args:
        service_name: The name of the service
        ssh_endpoint_url: The URL of the SSH endpoint
        token: The Snowflake authentication token
    """
    # Check if websocat is installed
    if not check_websocat_installed():
        cc.step("⚠️  websocat is required for SSH connection but not found.")

        # Try to install websocat on macOS
        if platform.system().lower() == "darwin":
            if install_websocat_macos():
                cc.step("✓ websocat installed successfully")
            else:
                cc.step("Please install websocat manually and run this command again.")
                cc.step(install_websocat_instructions())
                return
        else:
            cc.step(install_websocat_instructions())
            cc.step("Please install websocat and run this command again.")
            return

    # Parse the endpoint URL to extract hostname
    # Expected format: wss://hostname.domain or https://hostname.domain
    match = re.match(r"wss://([^/]+)", ssh_endpoint_url)
    if not match:
        # Try https format
        match = re.match(r"https://([^/]+)", ssh_endpoint_url)

    if not match:
        raise ValueError(f"Invalid SSH endpoint URL format: {ssh_endpoint_url}")

    hostname = match.group(1)

    # Prepare SSH config content - format matches the user's example
    host_name = f"{SSH_HOST_PREFIX}{service_name}"
    config_content = f"""
# Snowflake Remote Runtime - {service_name}
Host {host_name}
  HostName {hostname}
  Port     22
  User     root
  ProxyCommand websocat --binary wss://{hostname}/ -H "Authorization: Snowflake Token=\\"{token}\\""
  StrictHostKeyChecking no
"""

    # Expand the SSH config path
    ssh_config_path = os.path.expanduser(SSH_CONFIG_PATH)

    # Check if the config already exists for this host
    existing_config = ""
    host_pattern = re.compile(f"^Host {re.escape(host_name)}$", re.MULTILINE)

    if os.path.exists(ssh_config_path):
        with open(ssh_config_path, "r") as f:
            existing_config = f.read()

    if host_pattern.search(existing_config):
        # Config already exists, update it
        lines = existing_config.splitlines()
        new_lines = []
        skip_until_next_host = False

        for line in lines:
            if line.strip().startswith(f"Host {host_name}"):
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

    # Write the updated config
    with open(ssh_config_path, "w") as f:
        f.write(new_config)

    # Provide concise feedback - detailed status is shown in the command
    cc.step(f"📝 SSH configuration updated in {ssh_config_path}")


def setup_ssh_config(
    service_name: str, ssh_endpoint_url: str, ssh_key_path: str
) -> None:
    """
    Setup SSH configuration for the remote runtime service.

    Args:
        service_name: The name of the service
        ssh_endpoint_url: The URL of the SSH endpoint
        ssh_key_path: Path to the SSH private key
    """
    # Check if websocat is installed
    if not check_websocat_installed():
        cc.step("⚠️  websocat is required for SSH connection but not found.")
        cc.step(install_websocat_instructions())
        cc.step("Please install websocat and run this command again.")
        return

    # Parse the endpoint URL to extract hostname and domain
    match = re.match(r"wss://(.*?)(?:-ssh)?\.(.*)", ssh_endpoint_url)
    if not match:
        raise ValueError(f"Invalid SSH endpoint URL format: {ssh_endpoint_url}")

    hostname, domain = match.groups()

    # Prepare SSH config content
    host_name = f"{SSH_HOST_PREFIX}{service_name}"
    config_content = f"""
# Snowflake Remote Runtime - {service_name}
Host {host_name}
  HostName {hostname}.{domain}
  ProxyCommand websocat - --binary wss://{hostname}-ssh.{domain}
  User root
  IdentityFile {ssh_key_path}
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
"""

    # Expand the SSH config path
    ssh_config_path = os.path.expanduser(SSH_CONFIG_PATH)

    # Check if the config already exists for this host
    existing_config = ""
    host_pattern = re.compile(f"^Host {re.escape(host_name)}$", re.MULTILINE)

    if os.path.exists(ssh_config_path):
        with open(ssh_config_path, "r") as f:
            existing_config = f.read()

    if host_pattern.search(existing_config):
        # Config already exists, update it
        lines = existing_config.splitlines()
        new_lines = []
        skip_until_next_host = False

        for line in lines:
            if line.strip().startswith(f"Host {host_name}"):
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

    # Write the updated config
    with open(ssh_config_path, "w") as f:
        f.write(new_config)

    cc.step(f"SSH configuration added to {ssh_config_path}")


def validate_stage_path(path: str) -> bool:
    """Validate if a string is a valid Snowflake stage path."""
    return path.startswith("@")


def validate_git_repo(url: str) -> bool:
    """Validate if a string is a valid Git repository URL."""
    return (
        url.startswith("git://") or url.startswith("https://") or url.startswith("git@")
    )


def parse_source_uri(uri: str) -> Dict[str, str]:
    """
    Parse a source URI and determine its type.

    Args:
        uri: Source URI string

    Returns:
        Dictionary with source type and path
    """
    if validate_stage_path(uri):
        return {"type": "stage", "path": uri}
    elif validate_git_repo(uri):
        return {"type": "git", "url": uri}
    else:
        raise ValueError(
            f"Invalid source URI format: {uri}. Must be a stage path (@stage/path) or Git repository URL."
        )


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


class SnowflakeCloudType(enum.Enum):
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"

    @classmethod
    def from_value(cls, value: str) -> "SnowflakeCloudType":
        assert value
        for k in cls:
            if k.value == value.lower():
                return k
        else:
            raise ValueError(f"'{cls.__name__}' enum not found for '{value}'")


@dataclass(frozen=True)
class PayloadEntrypoint:
    file_path: PurePath
    main_func: Optional[str]


@dataclass(frozen=True)
class UploadedPayload:
    # TODO: Include manifest of payload files for validation
    stage_path: PurePath
    entrypoint: List[Union[str, PurePath]]


@dataclass(frozen=True)
class ComputeResources:
    cpu: float  # Number of vCPU cores
    memory: float  # Memory in GiB
    gpu: int = 0  # Number of GPUs
    gpu_type: Optional[str] = None


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


class SnowflakeRegion(TypedDict):
    region_group: NotRequired[str]
    snowflake_region: Required[str]
    cloud: Required[SnowflakeCloudType]
    region: Required[str]
    display_name: Required[str]


def get_regions(
    sess: session.Session,
) -> Dict[str, SnowflakeRegion]:

    res = sess.sql("SHOW REGIONS").collect()
    cc.step(f"Getting regions: {res}")
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
                snowflake_region=r.snowflake_region,
                cloud=SnowflakeCloudType.from_value(r.cloud),
                region=r.region,
                display_name=r.display_name,
            )

    return res_dict


def get_current_region_id(sess: session.Session) -> str:
    res = sess.sql("SELECT CURRENT_REGION() AS CURRENT_REGION").collect()[0]

    return cast(str, res.CURRENT_REGION)
