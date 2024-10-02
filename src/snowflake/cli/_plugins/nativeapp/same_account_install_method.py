from typing import Optional

from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationCreatedExternallyError,
)

# from snowflake.cli._plugins.nativeapp.project_model import NativeAppProjectModel
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.entities.utils import ensure_correct_owner


class SameAccountInstallMethod:
    _requires_created_by_cli: bool
    _from_release_directive: bool
    version: Optional[str]
    patch: Optional[int]

    def __init__(
        self,
        requires_created_by_cli: bool,
        version: Optional[str] = None,
        patch: Optional[int] = None,
        from_release_directive: bool = False,
    ):
        self._requires_created_by_cli = requires_created_by_cli
        self.version = version
        self.patch = patch
        self._from_release_directive = from_release_directive

    @classmethod
    def unversioned_dev(cls):
        """aka. stage dev aka loose files"""
        return cls(True)

    @classmethod
    def versioned_dev(cls, version: str, patch: Optional[int] = None):
        return cls(False, version, patch)

    @classmethod
    def release_directive(cls):
        return cls(False, from_release_directive=True)

    @property
    def is_dev_mode(self) -> bool:
        return not self._from_release_directive

    def using_clause(
        self,
        stage_fqn: str,
    ) -> str:
        if self._from_release_directive:
            return ""

        if self.version:
            patch_clause = f"patch {self.patch}" if self.patch else ""
            return f"using version {self.version} {patch_clause}"

        stage_name = StageManager.quote_stage_name(stage_fqn)
        return f"using {stage_name}"

    def ensure_app_usable(self, app_name: str, app_role: str, show_app_row: dict):
        """Raise an exception if we cannot proceed with install given the pre-existing application object"""

        if self._requires_created_by_cli:
            if show_app_row[COMMENT_COL] not in ALLOWED_SPECIAL_COMMENTS:
                # this application object was not created by this tooling
                raise ApplicationCreatedExternallyError(app_name)

        # expected owner
        ensure_correct_owner(row=show_app_row, role=app_role, obj_name=app_name)
