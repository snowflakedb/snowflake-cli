# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import Field, model_validator
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class ScriptModel(UpdatableModel):
    """Model for a single script definition in snowflake.yml."""

    cmd: Optional[str] = Field(
        default=None,
        title="Command to execute",
    )
    run: Optional[List[str]] = Field(
        default=None,
        title="List of script names to run in sequence (composite script)",
    )
    description: Optional[str] = Field(
        default=None,
        title="Human-readable description of the script",
    )
    shell: bool = Field(
        default=False,
        title="Whether to run through shell (required for pipes, redirects, globs)",
    )
    cwd: Optional[str] = Field(
        default=None,
        title="Working directory for the script (relative to project root)",
    )
    env: Optional[Dict[str, str]] = Field(
        default=None,
        title="Environment variables to set for the script",
    )

    @model_validator(mode="after")
    def validate_cmd_or_run(self):
        if self.cmd is None and self.run is None:
            raise ValueError("Script must have either 'cmd' or 'run' defined")
        if self.cmd is not None and self.run is not None:
            raise ValueError("Script cannot have both 'cmd' and 'run' defined")
        if self.run is not None and len(self.run) == 0:
            raise ValueError("'run' field cannot be an empty list")
        return self
