from enum import Enum, unique
from typing import NamedTuple

from snowflake.cli.api.config import (
    FEATURE_FLAGS_SECTION_PATH,
    get_config_bool_value,
    get_env_variable_name,
)


class BooleanFlag(NamedTuple):
    name: str
    default: bool = False


@unique
class FeatureFlagMixin(Enum):
    def is_enabled(self) -> bool:
        return get_config_bool_value(
            *FEATURE_FLAGS_SECTION_PATH,
            key=self.value.name.lower(),
            default=self.value.default,
        )

    def is_disabled(self):
        return not self.is_enabled()

    def env_variable(self):
        return get_env_variable_name(*FEATURE_FLAGS_SECTION_PATH, key=self.value.name)


@unique
class FeatureFlag(FeatureFlagMixin):
    ENABLE_STREAMLIT_EMBEDDED_STAGE = BooleanFlag(
        "ENABLE_STREAMLIT_EMBEDDED_STAGE", False
    )
    ENABLE_NOTEBOOKS = BooleanFlag("ENABLE_NOTEBOOKS", False)
