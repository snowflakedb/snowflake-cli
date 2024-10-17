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
from typing import Optional

from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
)
from snowflake.cli._plugins.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
)
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.v1.native_app.native_app import NativeApp


class NativeAppVersionDropProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, project_definition: NativeApp, project_root: Path):
        super().__init__(project_definition, project_root)

    def process(
        self,
        version: Optional[str],
        force: bool,
        interactive: bool,
        *args,
        **kwargs,
    ):
        return ApplicationPackageEntity.version_drop(
            console=cc,
            project_root=self.project_root,
            deploy_root=self.deploy_root,
            bundle_root=self.bundle_root,
            generated_root=self.generated_root,
            artifacts=self.artifacts,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
            version=version,
            force=force,
            interactive=interactive,
        )
