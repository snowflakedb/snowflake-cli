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
import inspect
from dataclasses import dataclass
from typing import Annotated, Type

import typer

InteractiveAnnotation = []


def to_parameter(x: Annotated) -> inspect.Parameter:
    pass


@dataclass
class ActionParameter:
    """
    Parameter metadata that is used to generate typer commands as well as to
    provide defaults to action invocations made via EntityBase.
    """

    name: str
    python_type: Type
    typer_param: typer.Argument | typer.Option
    """
    Defaults live on the typer argument / option, rather than in this dataclass directly.
    """

    # TODO: non-typer specific stuff in this dataclass, generate the typer from it

    def as_typer_param(self):
        ...

    def as_annotated_param(self):
        return Annotated[self.python_type, self.typer_param]
