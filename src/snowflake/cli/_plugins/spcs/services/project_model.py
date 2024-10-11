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
from typing import List

from snowflake.cli._plugins.nativeapp.artifacts import resolve_without_follow
from snowflake.cli.api.project.schemas.v1.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.schemas.v1.spcs.service import Service
from snowflake.cli.api.project.util import (
    to_identifier,
)


class ServiceProjectModel:
    def __init__(
        self,
        project_definition: Service,
        project_root: Path,
    ):
        self._project_definition = project_definition
        self._project_root = resolve_without_follow(project_root)

    @property
    def project_root(self) -> Path:
        return self._project_root

    @property
    def definition(self) -> Service:
        return self._project_definition

    @cached_property
    def service_name(self) -> str:
        return self._project_definition.name

    @cached_property
    def spec(self) -> PathMapping:
        return self.definition.spec

    @cached_property
    def images(self) -> List[PathMapping]:
        return self.definition.images

    @cached_property
    def image_sources(self) -> List[PathMapping]:
        source_image_paths = []
        for image in self.images:
            source_image_paths.append(PathMapping(src=image.src))
        return source_image_paths

    @cached_property
    def source_repo_path(self) -> str:
        return self.definition.source_repo

    @cached_property
    def source_repo_fqn(self) -> str:
        repo_path = self.definition.source_repo
        return repo_path.strip("/").replace("/", ".")

    @cached_property
    def source_stage_fqn(self) -> str:
        return self.definition.source_stage

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
    def project_identifier(self) -> str:
        return to_identifier(self.definition.name)

    @cached_property
    def query_warehouse(self) -> str:
        return to_identifier(self.definition.query_warehouse)

    @cached_property
    def compute_pool(self) -> str:
        return to_identifier(self.definition.compute_pool)

    @cached_property
    def min_instances(self) -> int:
        return self.definition.min_instances

    @cached_property
    def max_instances(self) -> int:
        return self.definition.max_instances

    @cached_property
    def comment(self) -> str:
        return self.definition.comment

    # def get_bundle_context(self) -> BundleContext:
    #     return BundleContext(
    #         package_name=self.package_name,
    #         artifacts=self.artifacts,
    #         project_root=self.project_root,
    #         bundle_root=self.bundle_root,
    #         deploy_root=self.deploy_root,
    #         generated_root=self.generated_root,
    #     )
