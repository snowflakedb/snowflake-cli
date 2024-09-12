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
# limitations under the License.from typing import Annotated, Optional

from typing import Annotated, Optional

from snowflake.cli._plugins.workspace.manager import ActionContext
from snowflake.cli.api.commands.flags import is_tty_interactive
from snowflake.cli.api.entities.actions import EntityActions
from snowflake.cli.api.entities.actions.lib import HelpText
from snowflake.cli.api.entities.application_package.entity import (
    ApplicationPackageEntity,
    ForceBool,
    InteractiveBool,
)

ApplicationPackageEntity.implements(EntityActions.VERSION_CREATE)


def action_version_create(
    entity: "ApplicationPackageEntity",
    ctx: ActionContext,
    version: Annotated[
        Optional[str],
        HelpText(
            """
            Version to define in your application package.
            If the version already exists, an auto-incremented patch is added to the version instead.
            Defaults to the version specified in the `manifest.yml` file.
        """
        ),
    ],
    patch: Annotated[
        Optional[int],
        HelpText(
            """
            The patch number you want to create for an existing version.
            Defaults to undefined if it is not set, which means the Snowflake CLI either uses
            the patch specified in the `manifest.yml` file or automatically generates a new patch number.
        """
        ),
    ],
    interactive: InteractiveBool = is_tty_interactive(),
    force: ForceBool = False,
):
    """
    Adds a new patch to the provided version defined in your application package.
    If the version does not exist, creates a version with patch 0.
    """
    pass
