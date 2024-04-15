import warnings

from snowflake.connector.compat import IS_WINDOWS


def ignore_unuseful_warnings():
    if IS_WINDOWS:  # This warning does not work correctly on Windows.
        warnings.filterwarnings(
            action="ignore",
            message="Bad owner or permissions.*",
            module="snowflake.connector.config_manager",
        )
