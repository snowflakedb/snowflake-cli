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

from typing import Dict, Optional

_FEATURES_PREFIX = "features"
_APP_PREFIX = "app"


class CLICounterField:
    TEMPLATES_PROCESSOR = f"{_FEATURES_PREFIX}.templates_processor"
    SQL_TEMPLATES = f"{_FEATURES_PREFIX}.sql_templates"
    PDF_TEMPLATES = f"{_FEATURES_PREFIX}.pdf_templates"
    SNOWPARK_PROCESSOR = f"{_FEATURES_PREFIX}.{_APP_PREFIX}.snowpark_processor"
    POST_DEPLOY_SCRIPTS = f"{_FEATURES_PREFIX}.{_APP_PREFIX}.post_deploy_scripts"


class CLIMetrics:
    """
    Class to track various metrics across the execution of a command
    """

    def __init__(self):
        self._counters: Dict[str, int] = {}

    def __eq__(self, other):
        if isinstance(other, CLIMetrics):
            return self._counters == other._counters
        return False

    def get_counter(self, name: str) -> Optional[int]:
        return self._counters.get(name)

    def set_counter(self, name: str, value: int) -> None:
        self._counters[name] = value

    def increment_counter(self, name: str, value: int = 1) -> None:
        if name not in self._counters:
            self.set_counter(name, value)
        else:
            self._counters[name] += value

    @property
    def counters(self) -> Dict[str, int]:
        # return a copy of the original dict to avoid mutating the original
        return self._counters.copy()
