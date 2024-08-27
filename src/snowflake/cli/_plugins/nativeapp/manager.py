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

import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import Generator, List, Optional, TypedDict

from click import ClickException
from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.nativeapp.artifacts import (
    BundleMap,
    build_bundle,
)
from snowflake.cli._plugins.nativeapp.codegen.compiler import (
    NativeAppCompiler,
)
from snowflake.cli._plugins.nativeapp.constants import (
    NAME_COL,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    NoEventTableForAccount,
)
from snowflake.cli._plugins.nativeapp.project_model import (
    NativeAppProjectModel,
)
from snowflake.cli._plugins.stage.diff import (
    DiffResult,
)
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.api.entities.utils import (
    execute_post_deploy_hooks,
    generic_sql_error_handler,
    sync_deploy_root_with_stage,
)
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.util import (
    identifier_for_url,
    unquote_identifier,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import DictCursor, ProgrammingError

ApplicationOwnedObject = TypedDict("ApplicationOwnedObject", {"name": str, "type": str})


class NativeAppCommandProcessor(ABC):
    @abstractmethod
    def process(self, *args, **kwargs):
        pass


class NativeAppManager(SqlExecutionMixin):
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

    @contextmanager
    def use_application_warehouse(self):
        if self.application_warehouse:
            with self.use_warehouse(self.application_warehouse):
                yield
        else:
            raise ClickException(
                dedent(
                    f"""\
                Application warehouse cannot be empty.
                Please provide a value for it in your connection information or your project definition file.
                """
                )
            )

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
        query = "show parameters like 'event_table' in account"
        results = self._execute_query(query, cursor_class=DictCursor)
        return next((r["value"] for r in results if r["key"] == "EVENT_TABLE"), "")

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
        bundle_map = build_bundle(self.project_root, self.deploy_root, self.artifacts)
        compiler = NativeAppCompiler(self.na_project.get_bundle_context())
        compiler.compile_artifacts()
        return bundle_map

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
        """
        Check for an existing application object by the same name as in project definition, in account.
        It executes a 'show applications like' query and returns the result as single row, if one exists.
        """
        with self.use_role(self.app_role):
            return self.show_specific_object(
                "applications", self.app_name, name_col=NAME_COL
            )

    def get_existing_app_pkg_info(self) -> Optional[dict]:
        return ApplicationPackageEntity.get_existing_app_pkg_info(
            package_name=self.package_name,
            package_role=self.package_role,
        )

    def get_objects_owned_by_application(self) -> List[ApplicationOwnedObject]:
        """
        Returns all application objects owned by this application.
        """
        with self.use_role(self.app_role):
            results = self._execute_query(
                f"show objects owned by application {self.app_name}"
            ).fetchall()
            return [{"name": row[1], "type": row[2]} for row in results]

    def _application_objects_to_str(
        self, application_objects: list[ApplicationOwnedObject]
    ) -> str:
        """
        Returns a list in an "(Object Type) Object Name" format. Database-level and schema-level object names are fully qualified:
        (COMPUTE_POOL) POOL_NAME
        (DATABASE) DB_NAME
        (SCHEMA) DB_NAME.PUBLIC
        ...
        """
        return "\n".join(
            [self._application_object_to_str(obj) for obj in application_objects]
        )

    def _application_object_to_str(self, obj: ApplicationOwnedObject) -> str:
        return f"({obj['type']}) {obj['name']}"

    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        name = identifier_for_url(self.app_name)
        with self.use_application_warehouse():
            return make_snowsight_url(self._conn, f"/#/apps/application/{name}")

    def create_app_package(self) -> None:
        return ApplicationPackageEntity.create_app_package(
            console=cc,
            package_name=self.package_name,
            package_role=self.package_role,
            package_distribution=self.package_distribution,
        )

    def _apply_package_scripts(self) -> None:
        return ApplicationPackageEntity.apply_package_scripts(
            console=cc,
            package_scripts=self.package_scripts,
            package_warehouse=self.package_warehouse,
            project_root=self.project_root,
            package_role=self.package_role,
            package_name=self.package_name,
        )

    def execute_package_post_deploy_hooks(self) -> None:
        execute_post_deploy_hooks(
            console=cc,
            project_root=self.project_root,
            post_deploy_hooks=self.package_post_deploy_hooks,
            deployed_object_type="application package",
            database_name=self.package_name,
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
        stage_fqn: Optional[str] = None,
        local_paths_to_sync: List[Path] | None = None,
        validate: bool = True,
        print_diff: bool = True,
    ) -> DiffResult:
        """app deploy process"""

        # 1. Create an empty application package, if none exists
        self.create_app_package()

        with self.use_role(self.package_role):
            # 2. now that the application package exists, create shared data
            self._apply_package_scripts()

            # 3. Upload files from deploy root local folder to the above stage
            stage_fqn = stage_fqn or self.stage_fqn
            diff = self.sync_deploy_root_with_stage(
                bundle_map=bundle_map,
                role=self.package_role,
                prune=prune,
                recursive=recursive,
                stage_fqn=stage_fqn,
                local_paths_to_sync=local_paths_to_sync,
                print_diff=print_diff,
            )

            # 4. Execute post-deploy hooks
            with self.use_package_warehouse():
                self.execute_package_post_deploy_hooks()

        if validate:
            self.validate(use_scratch_stage=False)

        return diff

    def deploy_to_scratch_stage_fn(self):
        bundle_map = self.build_bundle()
        self.deploy(
            bundle_map=bundle_map,
            prune=True,
            recursive=True,
            stage_fqn=self.scratch_stage_fqn,
            validate=False,
            print_diff=False,
        )

    def validate(self, use_scratch_stage: bool = False):
        return ApplicationPackageEntity.validate_setup_script(
            console=cc,
            package_name=self.package_name,
            package_role=self.package_role,
            stage_fqn=self.stage_fqn,
            use_scratch_stage=use_scratch_stage,
            scratch_stage_fqn=self.scratch_stage_fqn,
            deploy_to_scratch_stage_fn=self.deploy_to_scratch_stage_fn,
        )

    def get_validation_result(self, use_scratch_stage: bool):
        return ApplicationPackageEntity.get_validation_result(
            console=cc,
            package_name=self.package_name,
            package_role=self.package_role,
            stage_fqn=self.stage_fqn,
            use_scratch_stage=use_scratch_stage,
            scratch_stage_fqn=self.scratch_stage_fqn,
            deploy_to_scratch_stage_fn=self.deploy_to_scratch_stage_fn,
        )

    def get_events(  # type: ignore [return]
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
        record_types = record_types or []
        scopes = scopes or []

        if first >= 0 and last >= 0:
            raise ValueError("first and last cannot be used together")

        if not self.account_event_table:
            raise NoEventTableForAccount()

        # resource_attributes uses the unquoted/uppercase app and package name
        app_name = unquote_identifier(self.app_name)
        package_name = unquote_identifier(self.package_name)
        org_name = unquote_identifier(consumer_org)
        account_name = unquote_identifier(consumer_account)

        # Filter on record attributes
        if consumer_org and consumer_account:
            # Look for events shared from a consumer account
            app_clause = (
                f"resource_attributes:\"snow.application.package.name\" = '{package_name}' "
                f"and resource_attributes:\"snow.application.consumer.organization\" = '{org_name}' "
                f"and resource_attributes:\"snow.application.consumer.name\" = '{account_name}'"
            )
            if consumer_app_hash:
                # If the user has specified a hash of a specific app installation
                # in the consumer account, filter events to that installation only
                app_clause += f" and resource_attributes:\"snow.database.hash\" = '{consumer_app_hash.lower()}'"
        else:
            # Otherwise look for events from an app installed in the same account as the package
            app_clause = f"resource_attributes:\"snow.database.name\" = '{app_name}'"

        # Filter on event time
        if isinstance(since, datetime):
            since_clause = f"and timestamp >= '{since}'"
        elif isinstance(since, str) and since:
            since_clause = f"and timestamp >= sysdate() - interval '{since}'"
        else:
            since_clause = ""
        if isinstance(until, datetime):
            until_clause = f"and timestamp <= '{until}'"
        elif isinstance(until, str) and until:
            until_clause = f"and timestamp <= sysdate() - interval '{until}'"
        else:
            until_clause = ""

        # Filter on event type (log, span, span_event)
        type_in_values = ",".join(f"'{v}'" for v in record_types)
        types_clause = (
            f"and record_type in ({type_in_values})" if type_in_values else ""
        )

        # Filter on event scope (e.g. the logger name)
        scope_in_values = ",".join(f"'{v}'" for v in scopes)
        scopes_clause = (
            f"and scope:name in ({scope_in_values})" if scope_in_values else ""
        )

        # Limit event count
        first_clause = f"limit {first}" if first >= 0 else ""
        last_clause = f"limit {last}" if last >= 0 else ""

        query = dedent(
            f"""\
            select * from (
                select timestamp, value::varchar value
                from {self.account_event_table}
                where ({app_clause})
                {since_clause}
                {until_clause}
                {types_clause}
                {scopes_clause}
                order by timestamp desc
                {last_clause}
            ) order by timestamp asc
            {first_clause}
            """
        )
        try:
            return self._execute_query(query, cursor_class=DictCursor).fetchall()
        except ProgrammingError as err:
            generic_sql_error_handler(err)

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
        try:
            events = self.get_events(
                since=since,
                record_types=record_types,
                scopes=scopes,
                consumer_org=consumer_org,
                consumer_account=consumer_account,
                consumer_app_hash=consumer_app_hash,
                last=last,
            )
            yield from events  # Yield the initial batch of events
            last_event_time = events[-1]["TIMESTAMP"] if events else None

            while True:  # Then infinite poll for new events
                time.sleep(interval_seconds)
                previous_events = events
                events = self.get_events(
                    since=last_event_time,
                    record_types=record_types,
                    scopes=scopes,
                    consumer_org=consumer_org,
                    consumer_account=consumer_account,
                    consumer_app_hash=consumer_app_hash,
                )
                if not events:
                    continue

                yield from _new_events_only(previous_events, events)
                last_event_time = events[-1]["TIMESTAMP"]
        except KeyboardInterrupt:
            return


def _new_events_only(previous_events: list[dict], new_events: list[dict]) -> list[dict]:
    # The timestamp that overlaps between both sets of events
    overlap_time = new_events[0]["TIMESTAMP"]

    # Remove all the events from the new result set
    # if they were already printed. We iterate and remove
    # instead of filtering in order to handle duplicates
    # (i.e. if an event is present 3 times in new_events
    # but only once in previous_events, it should still
    # appear twice in new_events at the end
    new_events = new_events.copy()
    for event in reversed(previous_events):
        if event["TIMESTAMP"] < overlap_time:
            break
        # No need to handle ValueError here since we know
        # that events that pass the above if check will
        # either be in both lists or in new_events only
        new_events.remove(event)
    return new_events


def _validation_item_to_str(item: dict[str, str | int]):
    s = item["message"]
    if item["errorCode"]:
        s = f"{s} (error code {item['errorCode']})"
    return s
