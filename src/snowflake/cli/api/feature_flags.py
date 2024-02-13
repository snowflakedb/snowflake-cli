from enum import Enum, unique

from snowflake.cli.api.config import FEATURE_FLAGS_SECTION_PATH, get_config_bool_value


@unique
class FeatureFlag(Enum):
    ENABLE_EXPERIMENTAL = ("ENABLE_EXPERIMENTAL", False)

    def is_enabled(self) -> bool:
        flag_name, default_value = self.value
        return get_config_bool_value(
            *FEATURE_FLAGS_SECTION_PATH, key=flag_name.lower(), default=default_value
        )

    def is_disable(self):
        return not self.is_enabled()
