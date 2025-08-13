import enum
import json
import os
import platform
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
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
SSH_KEY_DIR = "~/.ssh/snowflake-container-runtime"


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


def generate_ssh_key_pair(
    service_name: str, key_type: str = "ed25519"
) -> Tuple[str, str]:
    """
    Generate SSH key pair for the container runtime service.

    Args:
        service_name: The name of the service
        key_type: Type of SSH key to generate (ed25519, rsa, ecdsa)

    Returns:
        Tuple of (private_key_path, public_key_content)
    """
    # Create SSH key directory if it doesn't exist
    ssh_key_dir = Path(os.path.expanduser(SSH_KEY_DIR))
    ssh_key_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

    # Define key file paths
    private_key_path = ssh_key_dir / f"{service_name}"
    public_key_path = ssh_key_dir / f"{service_name}.pub"

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
            f"snowflake-container-runtime-{service_name}",
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )

        # Set proper permissions
        private_key_path.chmod(0o600)
        public_key_path.chmod(0o644)

        # Read the public key content
        with open(public_key_path, "r") as f:
            public_key_content = f.read().strip()

        cc.step(f"🔑 Generated SSH key pair for service '{service_name}'")
        cc.step(f"   Private key: {private_key_path}")
        cc.step(f"   Public key: {public_key_path}")

        return str(private_key_path), public_key_content

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to generate SSH key pair: {e.stderr}")
    except FileNotFoundError:
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
    ssh_key_dir = Path(os.path.expanduser(SSH_KEY_DIR))
    private_key_path = ssh_key_dir / f"{service_name}"
    public_key_path = ssh_key_dir / f"{service_name}.pub"

    if private_key_path.exists() and public_key_path.exists():
        try:
            with open(public_key_path, "r") as f:
                public_key_content = f.read().strip()
            return str(private_key_path), public_key_content
        except IOError:
            return None

    return None


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
    service_name: str,
    ssh_endpoint_url: str,
    token: str,
    private_key_path: Optional[str] = None,
) -> None:
    """
    Setup SSH configuration for the remote runtime service using token authentication.

    Args:
        service_name: The name of the service
        ssh_endpoint_url: The URL of the SSH endpoint
        token: The Snowflake authentication token
        private_key_path: Optional path to SSH private key for key-based authentication
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

    # Build SSH config with optional key authentication
    config_lines = [
        f"Host {host_name}",
        f"  HostName {hostname}",
        f"  Port     22",
        f"  User     root",
        f'  ProxyCommand websocat --binary wss://{hostname}/ -H "Authorization: Snowflake Token=\\"{token}\\""',
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
                f"  StrictHostKeyChecking no",
                f"  UserKnownHostsFile /dev/null",
            ]
        )

    config_content = "\n" + "\n".join(config_lines) + "\n"

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


def configure_vscode_settings(
    service_name: str, server_install_path: str = "/root/user-default"
) -> None:
    """
    Configure VS Code Remote SSH settings to specify server install path.

    Args:
        service_name: The name of the service
        server_install_path: Path where VS Code server should be installed on remote
    """
    host_identifier = f"{SSH_HOST_PREFIX}{service_name}"

    # Configure both VS Code and VS Code Insiders
    vscode_variants = [
        {
            "name": "VS Code",
            "windows": "~/AppData/Roaming/Code/User/settings.json",
            "darwin": "~/Library/Application Support/Code/User/settings.json",
            "linux": "~/.config/Code/User/settings.json",
        },
        {
            "name": "VS Code Insiders",
            "windows": "~/AppData/Roaming/Code - Insiders/User/settings.json",
            "darwin": "~/Library/Application Support/Code - Insiders/User/settings.json",
            "linux": "~/.config/Code - Insiders/User/settings.json",
        },
    ]

    system = platform.system().lower()
    configured_count = 0

    for variant in vscode_variants:
        # Determine settings path based on OS
        if system == "windows":
            settings_path = os.path.expanduser(variant["windows"])
        elif system == "darwin":
            settings_path = os.path.expanduser(variant["darwin"])
        else:  # Linux
            settings_path = os.path.expanduser(variant["linux"])

        # Only configure if the VS Code variant directory exists (indicating it's installed)
        settings_dir = os.path.dirname(settings_path)
        if not os.path.exists(os.path.dirname(settings_dir)):
            continue  # Skip this variant if not installed

        # Create settings directory if it doesn't exist
        os.makedirs(settings_dir, exist_ok=True)

        # Load existing settings or create new ones
        settings = {}
        if os.path.exists(settings_path):
            try:
                with open(settings_path, "r") as f:
                    settings = json.load(f)
            except (json.JSONDecodeError, IOError):
                cc.step(
                    f"⚠️  Could not read existing {variant['name']} settings, creating new settings"
                )
                settings = {}

        # Configure Remote SSH server install path
        if "remote.SSH.serverInstallPath" not in settings:
            settings["remote.SSH.serverInstallPath"] = {}

        # Set the server install path for this specific host
        settings["remote.SSH.serverInstallPath"][host_identifier] = server_install_path

        # Also configure lockfiles to use tmp (important for AFS and similar filesystems)
        settings["remote.SSH.lockfilesInTmp"] = True

        # Write settings back to file
        try:
            with open(settings_path, "w") as f:
                json.dump(settings, f, indent=2)
            cc.step(f"⚙️  Configured {variant['name']} settings: {settings_path}")
            configured_count += 1
        except IOError as e:
            cc.step(f"⚠️  Warning: Could not update {variant['name']} settings: {e}")

    if configured_count > 0:
        cc.step(f"📁 VS Code server install path configured: {server_install_path}")
        cc.step(f"🔧 Configured {configured_count} VS Code variant(s)")
    else:
        cc.step(
            f"⚠️  No VS Code installations found. Please install VS Code and run this command again."
        )
        cc.step(
            f"💡 Alternatively, manually set 'remote.SSH.serverInstallPath.{host_identifier}' to '{server_install_path}' in VS Code settings"
        )


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
