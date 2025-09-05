# Copyright (c) 2025 Snowflake Inc.
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

from pydantic import BaseModel, Field, ValidationError
from pydantic_core import SchemaError
from snowflake.cli.api.artifacts.common import ArtifactError


class RegexResolver:
    def __init__(self):
        self._pattern_classes = {}

    def does_match(self, pattern: str, text: str) -> bool:
        """
        Check if text matches pattern.
        """
        if len(pattern) > 1000:
            raise ArtifactError(
                f"Regex pattern too long ({len(pattern)} chars, max 1000): "
                "potentially unsafe for performance"
            )
        if pattern not in self._pattern_classes:
            self._pattern_classes[pattern] = self._generate_pattern_class(pattern)

        pattern_class = self._pattern_classes[pattern]
        try:
            pattern_class(test_field=text)
            return True
        except ValidationError:
            return False

    @staticmethod
    def _generate_pattern_class(pattern: str) -> type:
        try:

            class _RegexTestModel(BaseModel):
                test_field: str = Field(pattern=pattern)

            return _RegexTestModel
        except SchemaError as e:
            raise ArtifactError(f"Invalid regex pattern: {e}") from e
