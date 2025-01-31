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

from pydantic import PrivateAttr
from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField
from snowflake.core.stage import Stage


@unique
class KindType(Enum):
    PERMANENT = "PERMANENT"
    TEMPORARY = "TEMPORARY"


class StageEntityModel(EntityModelBase):
    type: Literal["stage"] = DiscriminatorField(default="stage")  # noqa: A003
    _snowapi_model: Optional[Stage] = PrivateAttr(default=None)

    class Meta:
        supported_fields = ["name", "kind", "comment", "has_encryption_key"]

    def __init__(self, /, _model: Union[Stage, dict] = None, **data) -> None:
        super().__init__(**data)
        if _model is None:
            _model = dict()
        if isinstance(_model, Stage):
            self._snowapi_model = _model
        else:
            api_model_kwargs = {
                key: value
                for key, value in _model.items()
                if key in self.Meta.supported_fields
            }
            # api_model_kwargs["has_encryption_key"] = 123
            # TODO: discuss: some classes have more required fields. If we're not operating on an existing entity
            #       it might be beneficial to not run validations yet, and do that lazy just before creating/updating
            #       that resource
            self._snowapi_model = Stage.model_construct(**api_model_kwargs)
            # self.snowapi_model = Stage(**api_model_kwargs)

    def __getattr__(self, name):
        if name in self.Meta.supported_fields:
            return getattr(self._snowapi_model, name)
        return super().__getattr__(name)

    def revalidate(self):
        # TODO: there should be better ways to validate model instance
        self._snowapi_model = Stage(**self._snowapi_model.to_dict())

    def to_dict(self) -> dict:
        if self._snowapi_model is None:
            return {}
        return {
            key: value
            for key, value in self._snowapi_model.to_dict().items()
            if key in self.Meta.supported_fields
        }

    @property
    def snowapi_model(self) -> Stage:
        return self._snowapi_model

    @snowapi_model.setter
    def snowapi_model(self, value: Stage):
        self._snowapi_model = value
