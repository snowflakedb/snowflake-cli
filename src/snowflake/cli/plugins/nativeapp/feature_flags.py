from enum import unique

from snowflake.cli.api.feature_flags import BooleanFlag, FeatureFlagMixin


@unique
class FeatureFlag(FeatureFlagMixin):
    ENABLE_SETUP_SCRIPT_GENERATION = BooleanFlag(
        "ENABLE_SETUP_SCRIPT_GENERATION", False
    )
