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

from functools import cached_property
from pathlib import Path
from typing import List, Optional

from snowflake.cli._plugins.nativeapp.artifacts import resolve_without_follow
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.entities.common import get_sql_executor
from snowflake.cli.api.project.definition import (
    default_app_package,
    default_application,
    default_role,
)
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.util import (
    append_test_resource_suffix,
    extract_schema,
    to_identifier,
)


class NativeAppProjectModel:
    """
    Exposes properties of a native app project defined in a Snowflake Project Definition file. Whenever
    appropriate, APIs defined in this class provide suitable defaults or fallback logic to access properties
    of the project.
    """

    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
    ):
        self._project_definition = project_definition
        self._project_root = resolve_without_follow(project_root)

    @property
    def project_root(self) -> Path:
        return self._project_root

    @property
    def definition(self) -> NativeApp:
        return self._project_definition

    @cached_property
    def artifacts(self) -> List[PathMapping]:
        return self.definition.artifacts

    @cached_property
    def bundle_root(self) -> Path:
        return self.project_root / self.definition.bundle_root

    @cached_property
    def deploy_root(self) -> Path:
        return self.project_root / self.definition.deploy_root

    @cached_property
    def generated_root(self) -> Path:
        return self.deploy_root / self.definition.generated_root

    @cached_property
    def package_scripts(self) -> List[str]:
        """
        Relative paths to package scripts from the project root.
        """
        if self.definition.package and self.definition.package.scripts:
            return self.definition.package.scripts
        else:
            return []

    @cached_property
    def stage_fqn(self) -> str:
        return f"{self.package_name}.{self.definition.source_stage}"

    @cached_property
    def scratch_stage_fqn(self) -> str:
        return f"{self.package_name}.{self.definition.scratch_stage}"

    @cached_property
    def stage_schema(self) -> Optional[str]:
        return extract_schema(self.stage_fqn)

    @cached_property
    def package_warehouse(self) -> Optional[str]:
        if self.definition.package and self.definition.package.warehouse:
            return to_identifier(self.definition.package.warehouse)
        else:
            cli_context = get_cli_context()
            if cli_context.connection.warehouse:
                return to_identifier(cli_context.connection.warehouse)
            return None

    @cached_property
    def application_warehouse(self) -> Optional[str]:
        if self.definition.application and self.definition.application.warehouse:
            return to_identifier(self.definition.application.warehouse)
        else:
            cli_context = get_cli_context()
            if cli_context.connection.warehouse:
                return to_identifier(cli_context.connection.warehouse)
            return None

    @cached_property
    def project_identifier(self) -> str:
        # name is expected to be a valid Snowflake identifier, but YAML parsers will
        # sometimes strip out double quotes, so we try to get them back here.
        return to_identifier(self.definition.name)

    @property
    def package_name(self) -> str:
        if self.definition.package and self.definition.package.name:
            return to_identifier(self.definition.package.name)
        else:
            # V1.0 PDF doesn't support templating, so if the identifier isn't
            # explicitly specified, the default is generated here,
            # so we have to append the test resource suffix here
            name = default_app_package(self.project_identifier)
            return to_identifier(append_test_resource_suffix(name))

    @cached_property
    def package_role(self) -> str:
        if self.definition.package and self.definition.package.role:
            return self.definition.package.role
        else:
            return self._default_role

    @cached_property
    def package_distribution(self) -> str:
        if self.definition.package and self.definition.package.distribution:
            return self.definition.package.distribution.lower()
        else:
            return "internal"

    @property
    def app_name(self) -> str:
        if self.definition.application and self.definition.application.name:
            return to_identifier(self.definition.application.name)
        else:
            # V1.0 PDF doesn't support templating, so if the identifier isn't
            # explicitly specified, the default is generated here,
            # so we have to append the test resource suffix here
            name = default_application(self.project_identifier)
            return to_identifier(append_test_resource_suffix(name))

    @cached_property
    def app_role(self) -> str:
        if self.definition.application and self.definition.application.role:
            return self.definition.application.role
        else:
            return self._default_role

    @cached_property
    def app_post_deploy_hooks(self) -> Optional[List[PostDeployHook]]:
        """
        List of application instance post deploy hooks.
        """
        if self.definition.application and self.definition.application.post_deploy:
            return self.definition.application.post_deploy
        else:
            return None

    @cached_property
    def package_post_deploy_hooks(self) -> Optional[List[PostDeployHook]]:
        """
        List of application package post deploy hooks.
        """
        if self.definition.package and self.definition.package.post_deploy:
            return self.definition.package.post_deploy
        else:
            return None

    @cached_property
    def _default_role(self) -> str:
        role = default_role()
        if role is None:
            role = get_sql_executor().current_role()
        return role

    @cached_property
    def debug_mode(self) -> Optional[bool]:
        if self.definition.application:
            return self.definition.application.debug
        return None

    def get_bundle_context(self) -> BundleContext:
        return BundleContext(
            package_name=self.package_name,
            artifacts=self.artifacts,
            project_root=self.project_root,
            bundle_root=self.bundle_root,
            deploy_root=self.deploy_root,
            generated_root=self.generated_root,
        )
