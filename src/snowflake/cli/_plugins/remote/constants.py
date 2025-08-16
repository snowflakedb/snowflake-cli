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

"""Constants for the remote development environment plugin."""

import enum
from dataclasses import dataclass
from typing import Optional


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
class ComputeResources:
    cpu: float  # Number of vCPU cores
    memory: float  # Memory in GiB
    gpu: int = 0  # Number of GPUs
    gpu_type: Optional[str] = None


# SPCS specification constants
DEFAULT_CONTAINER_NAME = "main"
ENABLE_REMOTE_DEV_ENV_VAR = "IS_REMOTE_DEV"
MEMORY_VOLUME_NAME = "dshm"
USER_WORKSPACE_VOLUME_NAME = "user-workspace"
USER_WORKSPACE_VOLUME_MOUNT_PATH = "/root/workspace"
USER_VSCODE_DATA_VOLUME_NAME = "user-vscode-data"
USER_VSCODE_DATA_VOLUME_MOUNT_PATH = "/root/.vscode-server"

# Service naming constants
SERVICE_NAME_PREFIX = "SNOW_REMOTE"

# Service status constants
SERVICE_STATUS_READY = "READY"
SERVICE_STATUS_SUSPENDED = "SUSPENDED"
SERVICE_STATUS_SUSPENDING = "SUSPENDING"
SERVICE_STATUS_PENDING = "PENDING"
SERVICE_STATUS_STARTING = "STARTING"
SERVICE_STATUS_TERMINATING = "TERMINATING"
SERVICE_STATUS_FAILED = "FAILED"
SERVICE_STATUS_ERROR = "ERROR"
SERVICE_STATUS_UNKNOWN = "UNKNOWN"

# Service operation result constants
SERVICE_RESULT_CREATED = "created"
SERVICE_RESULT_RESUMED = "resumed"
SERVICE_RESULT_RUNNING = "running"

# Default timeout for service operations
DEFAULT_SERVICE_TIMEOUT_MINUTES = 10
STATUS_CHECK_INTERVAL_SECONDS = 10

# Default container image information
DEFAULT_IMAGE_REPO = "/snowflake/images/snowflake_images"
DEFAULT_IMAGE_CPU = "st_plat/runtime/x86/runtime_image/snowbooks"
DEFAULT_IMAGE_GPU = "st_plat/runtime/x86/generic_gpu/runtime_image/snowbooks"
DEFAULT_IMAGE_TAG = "1.7.1"

# Percent of container memory to allocate for /dev/shm volume
MEMORY_VOLUME_SIZE = 0.3

# Default ports
DEFAULT_SERVER_PORT = 12020
DEFAULT_WEBSOCKET_PORT = 12021

# Endpoint names
SERVER_UI_ENDPOINT_NAME = "server-ui"
WEBSOCKET_SSH_ENDPOINT_NAME = "websocket-ssh"
RAY_DASHBOARD_ENDPOINT_NAME = "ray-dashboard"

# ML runtime health check settings
ML_RUNTIME_HEALTH_CHECK_PORT = "5001"
ENABLE_HEALTH_CHECKS = "false"

# Ray environment variables
RAY_ENV_VARS = {
    "HEAD_CLIENT_SERVER_PORT": "10001",
    "HEAD_GCS_PORT": "12001",
    "HEAD_DASHBOARD_GRPC_PORT": "12002",
    "HEAD_DASHBOARD_PORT": "12003",
    "OBJECT_MANAGER_PORT": "12011",
    "NODE_MANAGER_PORT": "12012",
    "RUNTIME_ENV_AGENT_PORT": "12013",
    "DASHBOARD_AGENT_GRPC_PORT": "12014",
    "DASHBOARD_AGENT_LISTEN_PORT": "12015",
    "MIN_WORKER_PORT": "12031",
    "MAX_WORKER_PORT": "13000",
}

# Ray endpoint configurations
RAY_ENDPOINTS = [
    {"name": "ray-client-server-endpoint", "port": 10001, "protocol": "TCP"},
    {"name": "ray-gcs-endpoint", "port": 12001, "protocol": "TCP"},
    {"name": "ray-dashboard-grpc-endpoint", "port": 12002, "protocol": "TCP"},
    {"name": "ray-object-manager-endpoint", "port": 12011, "protocol": "TCP"},
    {"name": "ray-node-manager-endpoint", "port": 12012, "protocol": "TCP"},
    {"name": "ray-runtime-agent-endpoint", "port": 12013, "protocol": "TCP"},
    {"name": "ray-dashboard-agent-grpc-endpoint", "port": 12014, "protocol": "TCP"},
    {"name": "ephemeral-port-range", "portRange": "32768-60999", "protocol": "TCP"},
    {"name": "ray-worker-port-range", "portRange": "12031-13000", "protocol": "TCP"},
]

# Compute pool resource information
COMMON_INSTANCE_FAMILIES = {
    "CPU_X64_XS": ComputeResources(cpu=1, memory=6),
    "CPU_X64_S": ComputeResources(cpu=3, memory=13),
    "CPU_X64_M": ComputeResources(cpu=6, memory=28),
    "CPU_X64_L": ComputeResources(cpu=28, memory=116),
    "HIGHMEM_X64_S": ComputeResources(cpu=6, memory=58),
}

AWS_INSTANCE_FAMILIES = {
    "HIGHMEM_X64_M": ComputeResources(cpu=28, memory=240),
    "HIGHMEM_X64_L": ComputeResources(cpu=124, memory=984),
    "GPU_NV_S": ComputeResources(cpu=6, memory=27, gpu=1, gpu_type="A10G"),
    "GPU_NV_M": ComputeResources(cpu=44, memory=178, gpu=4, gpu_type="A10G"),
    "GPU_NV_L": ComputeResources(cpu=92, memory=1112, gpu=8, gpu_type="A100"),
}

AZURE_INSTANCE_FAMILIES = {
    "HIGHMEM_X64_M": ComputeResources(cpu=28, memory=244),
    "HIGHMEM_X64_L": ComputeResources(cpu=92, memory=654),
    "GPU_NV_XS": ComputeResources(cpu=3, memory=26, gpu=1, gpu_type="T4"),
    "GPU_NV_SM": ComputeResources(cpu=32, memory=424, gpu=1, gpu_type="A10"),
    "GPU_NV_2M": ComputeResources(cpu=68, memory=858, gpu=2, gpu_type="A10"),
    "GPU_NV_3M": ComputeResources(cpu=44, memory=424, gpu=2, gpu_type="A100"),
    "GPU_NV_SL": ComputeResources(cpu=92, memory=858, gpu=4, gpu_type="A100"),
}

CLOUD_INSTANCE_FAMILIES = {
    SnowflakeCloudType.AWS: AWS_INSTANCE_FAMILIES,
    SnowflakeCloudType.AZURE: AZURE_INSTANCE_FAMILIES,
}
