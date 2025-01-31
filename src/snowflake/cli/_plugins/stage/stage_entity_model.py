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

from enum import Enum, unique
from typing import Literal, Optional, Union

from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField
from snowflake.core.stage import Stage


@unique
class KindType(Enum):
    PERMANENT = "PERMANENT"
    TEMPORARY = "TEMPORARY"


class StageEntityModel(EntityModelBase):
    type: Literal["stage"] = DiscriminatorField(default="stage")  # noqa: A003
    snowapi_model: Optional[Stage] = None

    class Meta:
        supported_fields = ["name", "kind", "comment"]

    def __init__(self, /, _model: Union[Stage, dict], **data) -> None:
        super().__init__(**data)
        if isinstance(_model, Stage):
            self.snowapi_model = _model
        else:
            api_model_kwargs = {
                key: value
                for key, value in _model.items()
                if key in self.Meta.supported_fields
            }
            self.snowapi_model = Stage(**api_model_kwargs)

    def __getattr__(self, name):
        if name in self.Meta.supported_fields:
            return getattr(self.snowapi_model, name)
        return super().__getattr__(name)

    # @property
    # def kind(self) -> str:
    #     return self.snowapi_model.kind
    #
    # @property
    # def comment(self) -> Optional[str]:
    #     return self.snowapi_model.comment
    #
    # @property
    # def name(self) -> str:
    #     return self.snowapi_model.name

    def to_dict(self) -> dict:
        if self.snowapi_model is None:
            return {}
        return {
            key: value
            for key, value in self.snowapi_model.to_dict().items()
            if key in self.Meta.supported_fields
        }
