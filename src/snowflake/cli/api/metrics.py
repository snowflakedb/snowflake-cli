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


class _TypePrefix:
    FEATURES = "features"


class _DomainPrefix:
    GLOBAL = "global"
    APP = "app"
    SQL = "sql"


class CLICounterField:
    """
    for each counter field we're adopting a convention of
    <type>.<domain>.<name>
    for example, if we're tracking a global feature, then the field name would be
    features.global.feature_name

    The metrics API is implemented to be generic, but we are adopting a convention
    for feature tracking with the following model for a given command execution:
    * counter not present -> feature is not available
    * counter == 0 -> feature is available, but not used
    * counter == 1 -> feature is used
    this makes it easy to compute percentages for feature dashboards in Snowsight
    """

    TEMPLATES_PROCESSOR = (
        f"{_TypePrefix.FEATURES}.{_DomainPrefix.GLOBAL}.templates_processor"
    )
    SQL_TEMPLATES = f"{_TypePrefix.FEATURES}.{_DomainPrefix.SQL}.sql_templates"
    PDF_TEMPLATES = f"{_TypePrefix.FEATURES}.{_DomainPrefix.GLOBAL}.pdf_templates"
    SNOWPARK_PROCESSOR = (
        f"{_TypePrefix.FEATURES}.{_DomainPrefix.APP}.snowpark_processor"
    )
    POST_DEPLOY_SCRIPTS = (
        f"{_TypePrefix.FEATURES}.{_DomainPrefix.APP}.post_deploy_scripts"
    )
    PACKAGE_SCRIPTS = f"{_TypePrefix.FEATURES}.{_DomainPrefix.APP}.package_scripts"


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

    def set_counter_default(self, name: str, value: int) -> None:
        """
        sets the counter if it does not already exist
        """
        if name not in self._counters:
            self.set_counter(name, value)

    def increment_counter(self, name: str, value: int = 1) -> None:
        if name not in self._counters:
            self.set_counter(name, value)
        else:
            self._counters[name] += value

    @property
    def counters(self) -> Dict[str, int]:
        # return a copy of the original dict to avoid mutating the original
        return self._counters.copy()
