from enum import Enum, unique
from typing import NamedTuple

from snowflake.cli.api.config import (
    FEATURE_FLAGS_SECTION_PATH,
    check_if_value_is_set,
    get_config_bool_value,
    get_env_variable_name,
)


class _NotSet(str):
    pass


NOT_SET = _NotSet("NotSet")


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

    def state(self):
        is_set = check_if_value_is_set(
            *FEATURE_FLAGS_SECTION_PATH,
            key=self.value.name.lower(),
        )
        return {
            "enabled": self.is_enabled(),
            "configured": is_set,
            "default": self.value.default,
        }

    def env_variable(self):
        return get_env_variable_name(*FEATURE_FLAGS_SECTION_PATH, key=self.value.name)


@unique
class FeatureFlag(FeatureFlagMixin):
    ENABLE_STREAMLIT_EMBEDDED_STAGE = BooleanFlag(
        "ENABLE_STREAMLIT_EMBEDDED_STAGE", False
    )
    ENABLE_NOTEBOOKS = BooleanFlag("ENABLE_NOTEBOOKS", False)
