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

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ProjectEnvironment:
    """
    This class handles retrieval of project env variables.
    These env variables can be accessed through templating, as ctx.env.<var_name>

    This class checks for env values in the following order:
    - Check for overrides values from the command line. Use these values first.
    - Check if these variables are available as environment variables and return them if found.
    - Check for default values from the project definition file.
    """

    override_env: Dict[str, Any]
    default_env: Dict[str, Any]

    def __init__(
        self,
        default_env: Optional[Dict[str, Any]] = None,
        override_env: Optional[Dict[str, Any]] = None,
    ):
        self.override_env = override_env or {}
        self.default_env = default_env or {}

    def __getitem__(self, item):
        if item in self.override_env:
            return self.override_env.get(item)
        if item in os.environ:
            return os.environ[item]
        return self.default_env[item]

    def get(self, item, default=None):
        try:
            return self[item]
        except KeyError:
            return default

    def __contains__(self, item) -> bool:
        try:
            self[item]
            return True
        except KeyError:
            return False
