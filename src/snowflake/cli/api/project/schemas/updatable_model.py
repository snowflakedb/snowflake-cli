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

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field
from snowflake.cli.api.project.util import IDENTIFIER_NO_LENGTH


class UpdatableModel(BaseModel):
    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)

    def update_from_dict(self, update_values: Dict[str, Any]):
        """
        Takes a dictionary with values to override.
        If the field type is subclass of a UpdatableModel, its update_from_dict() method is called with
        the value to be set.
        If not, we use simple setattr to set new value.
        Values provided are validated against original restrictions, so it's impossible to overwrite string field with
        integer value etc.
        """
        for field, value in update_values.items():
            if field in self.model_fields.keys():
                if (
                    hasattr(getattr(self, field), "update_from_dict")
                    and field in self.model_fields_set
                ):
                    getattr(self, field).update_from_dict(value)
                else:
                    setattr(self, field, value)
        return self


def IdentifierField(*args, **kwargs):  # noqa
    return Field(max_length=254, pattern=IDENTIFIER_NO_LENGTH, *args, **kwargs)
