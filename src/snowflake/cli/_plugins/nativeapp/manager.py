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

from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Generator, List, Optional

from snowflake.cli._plugins.nativeapp.artifacts import (
    BundleMap,
)
from snowflake.cli._plugins.nativeapp.entities.application import (
    ApplicationEntity,
    ApplicationOwnedObject,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
)
from snowflake.cli._plugins.nativeapp.policy import AllowAlwaysPolicy, PolicyBase
from snowflake.cli._plugins.nativeapp.project_model import (
    NativeAppProjectModel,
)
from snowflake.cli._plugins.stage.diff import (
    DiffResult,
)
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import (
    execute_post_deploy_hooks,
    sync_deploy_root_with_stage,
)
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.schemas.v1.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.v1.native_app.path_mapping import PathMapping


class NativeAppCommandProcessor(ABC):
    @abstractmethod
    def process(self, *args, **kwargs):
        pass


class NativeAppManager:
    """
    Base class with frequently used functionality already implemented and ready to be used by related subclasses.
    """

    def __init__(self, project_definition: NativeApp, project_root: Path):
        super().__init__()
        self._na_project = NativeAppProjectModel(
            project_definition=project_definition,
            project_root=project_root,
        )

    @property
    def na_project(self) -> NativeAppProjectModel:
        return self._na_project

    @property
    def project_root(self) -> Path:
        return self.na_project.project_root

    @property
    def definition(self) -> NativeApp:
        return self.na_project.definition

    @property
    def artifacts(self) -> List[PathMapping]:
        return self.na_project.artifacts

    @property
    def bundle_root(self) -> Path:
        return self.na_project.bundle_root

    @property
    def deploy_root(self) -> Path:
        return self.na_project.deploy_root

    @property
    def generated_root(self) -> Path:
        return self.na_project.generated_root

    @property
    def package_scripts(self) -> List[str]:
        return self.na_project.package_scripts

    @property
    def stage_fqn(self) -> str:
        return self.na_project.stage_fqn

    @property
    def scratch_stage_fqn(self) -> str:
        return self.na_project.scratch_stage_fqn

    @property
    def stage_schema(self) -> Optional[str]:
        return self.na_project.stage_schema

    @property
    def package_warehouse(self) -> Optional[str]:
        return self.na_project.package_warehouse

    def use_package_warehouse(self):
        return ApplicationPackageEntity.use_package_warehouse(
            self.package_warehouse,
        )

    @property
    def application_warehouse(self) -> Optional[str]:
        return self.na_project.application_warehouse

    @property
    def project_identifier(self) -> str:
        return self.na_project.project_identifier

    @property
    def package_name(self) -> str:
        return self.na_project.package_name

    @property
    def package_role(self) -> str:
        return self.na_project.package_role

    @property
    def package_distribution(self) -> str:
        return self.na_project.package_distribution

    @property
    def app_name(self) -> str:
        return self.na_project.app_name

    @property
    def app_role(self) -> str:
        return self.na_project.app_role

    @property
    def app_post_deploy_hooks(self) -> Optional[List[PostDeployHook]]:
        return self.na_project.app_post_deploy_hooks

    @property
    def package_post_deploy_hooks(self) -> Optional[List[PostDeployHook]]:
        return self.na_project.package_post_deploy_hooks

    @property
    def debug_mode(self) -> bool:
        return self.na_project.debug_mode

    @cached_property
    def get_app_pkg_distribution_in_snowflake(self) -> str:
        return ApplicationPackageEntity.get_app_pkg_distribution_in_snowflake(
            self.package_name, self.package_role
        )

    @cached_property
    def account_event_table(self) -> str:
        return ApplicationEntity.get_account_event_table()

    def verify_project_distribution(
        self, expected_distribution: Optional[str] = None
    ) -> bool:
        return ApplicationPackageEntity.verify_project_distribution(
            console=cc,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
            expected_distribution=expected_distribution,
        )

    def build_bundle(self) -> BundleMap:
        """
        Populates the local deploy root from artifact sources.
        """
        return ApplicationPackageEntity.bundle(
            project_root=self.project_root,
            deploy_root=self.deploy_root,
            bundle_root=self.bundle_root,
            generated_root=self.generated_root,
            package_name=self.package_name,
            artifacts=self.artifacts,
        )

    def sync_deploy_root_with_stage(
        self,
        bundle_map: BundleMap,
        role: str,
        prune: bool,
        recursive: bool,
        stage_fqn: str,
        local_paths_to_sync: List[Path] | None = None,
        print_diff: bool = True,
    ) -> DiffResult:
        return sync_deploy_root_with_stage(
            console=cc,
            deploy_root=self.deploy_root,
            package_name=self.package_name,
            stage_schema=self.stage_schema,
            bundle_map=bundle_map,
            role=role,
            prune=prune,
            recursive=recursive,
            stage_fqn=stage_fqn,
            local_paths_to_sync=local_paths_to_sync,
            print_diff=print_diff,
        )

    def get_existing_app_info(self) -> Optional[dict]:
        return ApplicationEntity.get_existing_app_info_static(
            app_name=self.app_name,
            app_role=self.app_role,
        )

    def get_existing_app_pkg_info(self) -> Optional[dict]:
        return ApplicationPackageEntity.get_existing_app_pkg_info(
            package_name=self.package_name,
            package_role=self.package_role,
        )

    def get_objects_owned_by_application(self):
        return ApplicationEntity.get_objects_owned_by_application(
            app_name=self.app_name,
            app_role=self.app_role,
        )

    def _application_objects_to_str(
        self, application_objects: list[ApplicationOwnedObject]
    ) -> str:
        return ApplicationEntity.application_objects_to_str(application_objects)

    def _application_object_to_str(self, obj: ApplicationOwnedObject):
        return ApplicationEntity.application_object_to_str(obj)

    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        return ApplicationEntity.get_snowsight_url_static(
            self.app_name, self.application_warehouse
        )

    def create_app_package(self) -> None:
        return ApplicationPackageEntity.create_app_package(
            console=cc,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
        )

    def execute_app_post_deploy_hooks(self) -> None:
        execute_post_deploy_hooks(
            console=cc,
            project_root=self.project_root,
            post_deploy_hooks=self.app_post_deploy_hooks,
            deployed_object_type="application",
            database_name=self.app_name,
        )

    def deploy(
        self,
        bundle_map: BundleMap,
        prune: bool,
        recursive: bool,
        policy: PolicyBase,
        stage_fqn: Optional[str] = None,
        local_paths_to_sync: List[Path] | None = None,
        validate: bool = True,
        print_diff: bool = True,
    ) -> DiffResult:
        return ApplicationPackageEntity.deploy(
            console=cc,
            project_root=self.project_root,
            deploy_root=self.deploy_root,
            bundle_root=self.bundle_root,
            generated_root=self.generated_root,
            artifacts=self.artifacts,
            bundle_map=bundle_map,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
            prune=prune,
            recursive=recursive,
            paths=local_paths_to_sync,
            print_diff=print_diff,
            validate=validate,
            stage_fqn=stage_fqn or self.stage_fqn,
            package_warehouse=self.package_warehouse,
            post_deploy_hooks=self.package_post_deploy_hooks,
            package_scripts=self.package_scripts,
            policy=policy,
        )

    def validate(self, use_scratch_stage: bool = False):
        return ApplicationPackageEntity.validate_setup_script(
            console=cc,
            project_root=self.project_root,
            deploy_root=self.deploy_root,
            bundle_root=self.bundle_root,
            generated_root=self.generated_root,
            artifacts=self.artifacts,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
            prune=True,
            recursive=True,
            paths=[],
            stage_fqn=self.stage_fqn,
            package_warehouse=self.package_warehouse,
            policy=AllowAlwaysPolicy(),
            use_scratch_stage=use_scratch_stage,
            scratch_stage_fqn=self.scratch_stage_fqn,
        )

    def get_validation_result(self, use_scratch_stage: bool = False):
        return ApplicationPackageEntity.get_validation_result_static(
            console=cc,
            project_root=self.project_root,
            deploy_root=self.deploy_root,
            bundle_root=self.bundle_root,
            generated_root=self.generated_root,
            artifacts=self.artifacts,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
            prune=True,
            recursive=True,
            paths=[],
            stage_fqn=self.stage_fqn,
            package_warehouse=self.package_warehouse,
            policy=AllowAlwaysPolicy(),
            use_scratch_stage=use_scratch_stage,
            scratch_stage_fqn=self.scratch_stage_fqn,
        )

    def get_events(  # type: ignore
        self,
        since: str | datetime | None = None,
        until: str | datetime | None = None,
        record_types: list[str] | None = None,
        scopes: list[str] | None = None,
        consumer_org: str = "",
        consumer_account: str = "",
        consumer_app_hash: str = "",
        first: int = -1,
        last: int = -1,
    ) -> list[dict]:
        return ApplicationEntity.get_events(
            app_name=self.app_name,
            package_name=self.package_name,
            since=since,
            until=until,
            record_types=record_types,
            scopes=scopes,
            consumer_org=consumer_org,
            consumer_account=consumer_account,
            consumer_app_hash=consumer_app_hash,
            first=first,
            last=last,
        )

    def stream_events(
        self,
        interval_seconds: int,
        since: str | datetime | None = None,
        record_types: list[str] | None = None,
        scopes: list[str] | None = None,
        consumer_org: str = "",
        consumer_account: str = "",
        consumer_app_hash: str = "",
        last: int = -1,
    ) -> Generator[dict, None, None]:
        return ApplicationEntity.stream_events(
            app_name=self.app_name,
            package_name=self.package_name,
            interval_seconds=interval_seconds,
            since=since,
            record_types=record_types,
            scopes=scopes,
            consumer_org=consumer_org,
            consumer_account=consumer_account,
            consumer_app_hash=consumer_app_hash,
            last=last,
        )
