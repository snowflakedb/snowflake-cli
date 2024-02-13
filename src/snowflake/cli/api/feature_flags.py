from enum import Enum, unique
from typing import NamedTuple

from snowflake.cli.api.config import FEATURE_FLAGS_SECTION_PATH, get_config_bool_value


class _Flag(NamedTuple):
    name: str
    default: bool = False


@unique
class FeatureFlag(Enum):
    ENABLE_STREAMLIT_EMBED_STAGE = _Flag("ENABLE_STREAMLIT_EMBED_STAGE", False)

    def is_enabled(self) -> bool:
        return get_config_bool_value(
            *FEATURE_FLAGS_SECTION_PATH,
            key=self.value.name.lower(),
            default=self.value.default,
        )

    def is_disable(self):
        return not self.is_enabled()

    def env_variable(self):
        return self.get_env_value(*FEATURE_FLAGS_SECTION_PATH, key=self.value.name)
