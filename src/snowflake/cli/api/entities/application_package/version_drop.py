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

from typing import Annotated, Optional

from snowflake.cli._plugins.workspace.manager import ActionContext
from snowflake.cli.api.entities.actions import EntityActions
from snowflake.cli.api.entities.actions.lib import HelpText
from snowflake.cli.api.entities.application_package.entity import (
    ApplicationPackageEntity,
)


@ApplicationPackageEntity.implements(EntityActions.VERSION_DROP)
def action_version_drop(
    self,
    ctx: ActionContext,
    version: Annotated[
        Optional[str],
        HelpText(""),
    ] = None,
):
    """
    Drops a version defined in your application package.
    Versions can either be passed in as an argument to the command or read from the `manifest.yml` file.
    Dropping patches is not allowed.
    """
    pass
