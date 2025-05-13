from enum import Enum


class StageEncryptionType(str, Enum):
    SNOWFLAKE_FULL = "SNOWFLAKE_FULL"
    SNOWFLAKE_SSE = "SNOWFLAKE_SSE"
    AWS_CSE = "AWS_CSE"
    AWS_SSE_S3 = "AWS_SSE_S3"
    AWS_SSE_KMS = "AWS_SSE_KMS"
    GCS_SSE_KMS = "GCS_SSE_KMS"
    AZURE_CSE = "AZURE_CSE"
    NONE = "NONE"
