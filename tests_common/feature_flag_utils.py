import os
from contextlib import contextmanager
from unittest import mock

from snowflake.cli.api.feature_flags import FeatureFlagMixin


@contextmanager
def with_feature_flags(flags: dict[FeatureFlagMixin, bool]):
    with mock.patch.dict(
        "os.environ",
        os.environ.copy()
        | {flag.env_variable(): str(value) for flag, value in flags.items()},
    ):
        yield
