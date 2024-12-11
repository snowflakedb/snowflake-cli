from dataclasses import dataclass
from typing import Optional

from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationCreatedExternallyError,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.project.util import to_identifier


@dataclass
class SameAccountInstallMethod:
    _requires_created_by_cli: bool
    version: Optional[str] = None
    patch: Optional[int] = None
    _from_release_directive: bool = False

    @classmethod
    def unversioned_dev(cls):
        """aka. stage dev aka loose files"""
        return cls(True)

    @classmethod
    def versioned_dev(cls, version: str, patch: Optional[int] = None):
        return cls(False, version, patch)

    @classmethod
    def release_directive(cls):
        return cls(False, _from_release_directive=True)

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
            version_clause = f"version {to_identifier(self.version)}"
            patch_clause = f"patch {self.patch}" if self.patch else ""
            return f"using {version_clause} {patch_clause}"

        stage_name = StageManager.quote_stage_name(stage_fqn)
        return f"using {stage_name}"

    def ensure_app_usable(self, app_name: str, app_role: str, show_app_row: dict):
        """Raise an exception if we cannot proceed with install given the pre-existing application object"""

        if self._requires_created_by_cli:
            if show_app_row[COMMENT_COL] not in ALLOWED_SPECIAL_COMMENTS:
                # this application object was not created by this tooling
                raise ApplicationCreatedExternallyError(app_name)
