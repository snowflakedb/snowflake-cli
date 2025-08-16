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
from dataclasses import dataclass
from typing import Dict, Optional, TypedDict, cast

# Required is not essential for runtime, so we'll use a simpler approach
from snowflake import snowpark
from snowflake.cli._plugins.remote.constants import (
    CLOUD_INSTANCE_FAMILIES,
    COMMON_INSTANCE_FAMILIES,
    ComputeResources,
    SnowflakeCloudType,
)

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
        raise ValueError("Stage path cannot be empty")

    # Remove whitespace first
    path = stage_path.strip()

    # Handle SnowURL paths - check prefix before removing trailing slashes
    if path.startswith("snow://"):
        if len(path) <= len("snow://"):
            raise ValueError(
                f"Invalid SnowURL stage path: '{stage_path}' - missing content after snow://"
            )
        return path.rstrip("/")

    # Remove trailing slashes for non-SnowURL paths
    path = path.rstrip("/")

    # Handle @ prefixed paths
    if path.startswith("@"):
        if len(path) <= 1:
            raise ValueError(
                f"Invalid @ prefixed stage path: '{stage_path}' - missing stage name after @"
            )
        return path

    # For paths without prefix, add @ prefix
    if not path:
        raise ValueError("Stage path cannot be empty")

    return f"@{path}"
