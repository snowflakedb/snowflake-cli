import enum
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Union, cast

from packaging import version
from snowflake.snowpark import session


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


def get_current_region_id(sess: session.Session) -> str:
    res = session.sql(sess, "SELECT CURRENT_REGION() AS CURRENT_REGION").collect()[0]

    return cast(str, res.CURRENT_REGION)


def get_current_snowflake_version(
    sess: session.Session, *, statement_params: Optional[Dict[str, Any]] = None
) -> version.Version:
    """Get Snowflake Version as a version.Version object follow PEP way of versioning, that is to say:
        "7.44.2 b202312132139364eb71238" to <Version('7.44.2+b202312132139364eb71238')>

    Args:
        sess: Snowpark Session.
        statement_params: Statement params. Defaults to None.

    Returns:
        The version of Snowflake Version.
    """
    res = session.sql(
        sess,
        "SELECT CURRENT_VERSION() AS CURRENT_VERSION",
        statement_params=statement_params,
    ).collect()[0]

    version_str = res.CURRENT_VERSION
    assert isinstance(version_str, str)

    version_str = "+".join(version_str.split())
    return version.parse(version_str)
