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

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class PluginConfig:
    is_plugin_enabled: bool
    internal_config: Dict[str, Any]


class PluginConfigProvider:
    def get_enabled_plugin_names(self) -> List[str]:
        raise NotImplementedError()

    def get_config(self, plugin_name: str) -> PluginConfig:
        raise NotImplementedError()
