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

from dataclasses import dataclass
from enum import Enum


@dataclass
class _ExternalLink:
    text: str
    link: str

    def get_link_text(self) -> str:
        """Returns the name and link in specific format."""
        return f"`{self.text} <{self.link}>`_"


class NativeAppReference(Enum):
    AUTOMATIC_SQL_CODE_GENERATION = _ExternalLink(
        text="Automatic SQL code generation",
        link="https://docs.snowflakze.com/en/developer-guide/snowflake-cli-v2/native-apps/bundle-app#label-cli-nativeapp-bundle-codegen",
    )

    SNOW_APP_BUNDLE = _ExternalLink(
        text="snow app bundle",
        link="https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/command-reference/native-apps-commands/bundle-app",
    )

    PROJECT_DEFINITION_OVERRIDES = _ExternalLink(
        text="Project definition overrides",
        link="https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/native-apps/project-definitions#project-definition-overrides",
    )
