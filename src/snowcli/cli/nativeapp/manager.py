from __future__ import annotations

import logging
import sys
import jinja2
from pathlib import Path
import subprocess

from snowcli.cli.common.sql_execution import SqlExecutionMixin

log = logging.getLogger(__name__)


class NativeAppManager(SqlExecutionMixin):
    def nativeapp_init(self, name: str, template: str | None = None):
        """
        Initialize a Native Apps project in the user's local directory, with or without the use of a template.
        """

        pass
