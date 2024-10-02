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

from pathlib import Path
from typing import Dict, Optional

from snowflake.cli._plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
)
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.application_entity import (
    ApplicationEntity,
)
from snowflake.cli.api.entities.application_package_entity import (
    ApplicationPackageEntity,
)


class NativeAppTeardownProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: Dict, project_root: Path):
        super().__init__(project_definition, project_root)

    def drop_application(
        self, auto_yes: bool, interactive: bool = False, cascade: Optional[bool] = None
    ):
        return ApplicationEntity.drop(
            console=cc,
            app_name=self.app_name,
            app_role=self.app_role,
            auto_yes=auto_yes,
            interactive=interactive,
            cascade=cascade,
        )

    def drop_package(self, auto_yes: bool):
        return ApplicationPackageEntity.drop(
            console=cc,
            package_name=self.package_name,
            package_role=self.package_role,
            force_drop=auto_yes,
        )

    def process(
        self,
        interactive: bool,
        force_drop: bool = False,
        cascade: Optional[bool] = None,
        *args,
        **kwargs,
    ):

        # Drop the application object
        self.drop_application(
            auto_yes=force_drop, interactive=interactive, cascade=cascade
        )

        # Drop the application package
        self.drop_package(auto_yes=force_drop)
