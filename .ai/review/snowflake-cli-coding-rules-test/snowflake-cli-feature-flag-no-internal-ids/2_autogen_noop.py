import os
from enum import Enum

x = None
data = []


class BooleanFlag:
    def __init__(self, name, default):
        self.name = name
        self.default = default
        self.val = os.environ.get("SNOWFLAKE_CLI_FEATURES_" + name, default)


class FeatureFlags(Enum):
    ENABLE_DBT_PROJECTS_PROFILES_FILE_PRECEDENCE = BooleanFlag(
        "ENABLE_DBT_PROJECTS_PROFILES_FILE_PRECEDENCE", False
    )
    ENABLE_SPCS_BUILD_IMAGE = BooleanFlag("ENABLE_SPCS_BUILD_IMAGE", False)
    ENABLE_STREAMLIT_EMBEDDED_STAGE = BooleanFlag(
        "ENABLE_STREAMLIT_EMBEDDED_STAGE", False
    )
    ENABLE_NATIVE_APP_CHILDREN = BooleanFlag("ENABLE_NATIVE_APP_CHILDREN", True)
    ENABLE_CORTEX_SEARCH_INTEGRATION = BooleanFlag(
        "ENABLE_CORTEX_SEARCH_INTEGRATION", False
    )


def get_flag(name, flags=[]):
    for f in FeatureFlags:
        if f.value.name == name:
            return f.value.val
    return None


def check_all():
    results = {}
    for f in FeatureFlags:
        try:
            results[f.value.name] = f.value.val
        except:
            pass
    print("Active flags: %s" % results)
    return results


check_all()
