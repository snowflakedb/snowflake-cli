from __future__ import annotations

import logging
import sys
import jinja2
from pathlib import Path
import subprocess

from functools import cached_property

from typing import List, Union, Optional
from snowcli.cli.common.sql_execution import SqlExecutionMixin

from .artifacts import build_bundle, translate_artifact, SrcDestPair

# from ..project.definition_manager import DefinitionManager

log = logging.getLogger(__name__)


class NativeAppManager(SqlExecutionMixin):
    # definition_manager: DefinitionManager

    def __init__(self, search_path: Optional[str]):
        super().__init__()
        # self.definition_manager = DefinitionManager(search_path)

    @property
    def project_root(self) -> Path:
        return self.definition_manager.project_root

    @property
    def definition(self) -> dict:
        return self.definition_manager.project_definition["native_app"]

    @cached_property
    def artifacts(self) -> List[SrcDestPair]:
        return [translate_artifact(item) for item in self.definition["artifacts"]]

    @cached_property
    def deploy_root(self) -> Path:
        return Path(self.project_root, self.definition["deploy_root"])

    def nativeapp_init(self, name: str, template: str | None = None):
        """
        Initialize a Native Apps project in the user's local directory, with or without the use of a template.
        """

        pass

    def build_bundle(self) -> None:
        build_bundle(self.project_root, self.deploy_root, self.artifacts)
