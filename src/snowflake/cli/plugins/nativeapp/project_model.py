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
from typing import Callable, List, Optional

from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.project.definition import (
    default_app_package,
    default_application,
    default_role,
)
from snowflake.cli.api.project.schemas.native_app.application import (
    ApplicationPostDeployHook,
)
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.util import extract_schema, to_identifier
from snowflake.cli.plugins.nativeapp.artifacts import resolve_without_follow


def default_role_fallback_cb() -> str:
    raise ClickException("Not role available")


class NativeAppProjectModel:
    """
    Represents information related to a native app package entity as part of a Snowflake CLI project.
    """

    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
        role_fallback_cb: Callable[[], str] | None = None,
    ):
        self._project_definition = project_definition
        self._project_root = resolve_without_follow(project_root)
        self._role_fallback_cb = role_fallback_cb or default_role_fallback_cb

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
        return Path(self.project_root, self.definition.bundle_root)

    @cached_property
    def deploy_root(self) -> Path:
        return Path(self.project_root, self.definition.deploy_root)

    @cached_property
    def generated_root(self) -> Path:
        return Path(self.deploy_root, self.definition.generated_root)

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
            return self.definition.package.warehouse
        elif cli_context.connection:
            return cli_context.connection.warehouse
        else:
            return None

    @cached_property
    def application_warehouse(self) -> Optional[str]:
        if self.definition.application and self.definition.application.warehouse:
            return self.definition.application.warehouse
        elif cli_context.connection:
            return cli_context.connection.warehouse
        else:
            return None

    @cached_property
    def project_identifier(self) -> str:
        # name is expected to be a valid Snowflake identifier, but YAML parsers will
        # sometimes strip out double quotes, so we try to get them back here.
        return to_identifier(self.definition.name)

    @cached_property
    def package_name(self) -> str:
        if self.definition.package and self.definition.package.name:
            return to_identifier(self.definition.package.name)
        else:
            return to_identifier(default_app_package(self.project_identifier))

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

    @cached_property
    def app_name(self) -> str:
        if self.definition.application and self.definition.application.name:
            return to_identifier(self.definition.application.name)
        else:
            return to_identifier(default_application(self.project_identifier))

    @cached_property
    def app_role(self) -> str:
        if self.definition.application and self.definition.application.role:
            return self.definition.application.role
        else:
            return self._default_role

    @cached_property
    def app_post_deploy_hooks(self) -> Optional[List[ApplicationPostDeployHook]]:
        """
        List of application post deploy hooks.
        """
        if self.definition.application and self.definition.application.post_deploy:
            return self.definition.application.post_deploy
        else:
            return None

    @cached_property
    def _default_role(self) -> str:
        role = default_role()
        if role is None:
            role = self._role_fallback_cb()
        return role

    @cached_property
    def debug_mode(self) -> bool:
        if self.definition.application:
            return self.definition.application.debug
        else:
            return True
