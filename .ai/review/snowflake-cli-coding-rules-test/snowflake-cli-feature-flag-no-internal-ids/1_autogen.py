from enum import Enum


class BooleanFlag:
    def __init__(self, name, default):
        self.name = name
        self.default = default


class FeatureFlag(Enum):
    # JIRA/issue number embedded as SNOW_NNNN prefix
    ENABLE_SNOW_98765_NEW_DEPLOY_FLOW = BooleanFlag(
        "ENABLE_SNOW_98765_NEW_DEPLOY_FLOW", False
    )
    # FIX_ prefix with numeric issue reference (the canonical PR case)
    ENABLE_FIX_3659937_DBT_PROJECTS_PROFILES_FILE = BooleanFlag(
        "ENABLE_FIX_3659937_DBT_PROJECTS_PROFILES_FILE", False
    )
    # Number embedded in the middle of the name
    ENABLE_DBT_1234_PROFILES_PRECEDENCE = BooleanFlag(
        "ENABLE_DBT_1234_PROFILES_PRECEDENCE", False
    )
