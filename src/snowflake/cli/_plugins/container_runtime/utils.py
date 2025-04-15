import enum
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

from packaging import version
from snowflake.cli.api.console import cli_console as cc
from snowflake.snowpark import session
from typing_extensions import NotRequired, Required


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


def get_current_snowflake_version(sess: session.Session) -> version.Version:
    """Get Snowflake Version as a version.Version object follow PEP way of versioning, that is to say:
        "7.44.2 b202312132139364eb71238" to <Version('7.44.2+b202312132139364eb71238')>

    Args:
        sess: Snowpark Session.
        statement_params: Statement params. Defaults to None.

    Returns:
        The version of Snowflake Version.
    """
    res = sess.sql("SELECT CURRENT_VERSION() AS CURRENT_VERSION").collect()[0]

    version_str = res.CURRENT_VERSION
    assert isinstance(version_str, str)

    version_str = "+".join(version_str.split())
    return version.parse(version_str)
