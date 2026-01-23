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

import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import yaml
from snowflake.cli._plugins.connection.util import get_account
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.spcs.common import (
    EVENT_COLUMN_NAMES,
    NoPropertiesProvidedError,
    SPCSEventTableError,
    build_db_and_schema_clause,
    build_resource_clause,
    build_time_clauses,
    filter_log_timestamp,
    format_event_row,
    format_metric_row,
    handle_object_already_exists,
    new_logs_only,
    strip_empty_lines,
)
from snowflake.cli._plugins.spcs.services.service_project_paths import (
    ServiceProjectPaths,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.artifacts.utils import bundle_artifacts
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB, ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import Artifacts
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


class ServiceManager(SqlExecutionMixin):
    def create(
        self,
        service_name: str,
        compute_pool: str,
        spec_path: Path,
        min_instances: int,
        max_instances: int,
        auto_resume: bool,
        external_access_integrations: Optional[List[str]],
        query_warehouse: Optional[str],
        tags: Optional[List[Tag]],
        comment: Optional[str],
        if_not_exists: bool,
    ) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)
        create_statement = "CREATE SERVICE"
        if if_not_exists:
            create_statement = f"{create_statement} IF NOT EXISTS"
        query = f"""\
            {create_statement} {service_name}
            IN COMPUTE POOL {compute_pool}
            FROM SPECIFICATION $$
            {spec}
            $$
            MIN_INSTANCES = {min_instances}
            MAX_INSTANCES = {max_instances}
            AUTO_RESUME = {auto_resume}
            """.splitlines()

        if external_access_integrations:
            external_access_integration_list = ",".join(
                f"{e}" for e in external_access_integrations
            )
            query.append(
                f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_list})"
            )

        if query_warehouse:
            query.append(f"QUERY_WAREHOUSE = {query_warehouse}")

        if comment:
            query.append(f"COMMENT = {comment}")

        if tags:
            tag_list = ",".join(f"{t.name}={t.value_string_literal()}" for t in tags)
            query.append(f"WITH TAG ({tag_list})")

        try:
            return self.execute_query(strip_empty_lines(query))
        except ProgrammingError as e:
            handle_object_already_exists(e, ObjectType.SERVICE, service_name)

    def deploy(
        self,
        service_name: str,
        stage: str,
        artifacts: List[str],
        compute_pool: str,
        spec_path: Path,
        min_instances: int,
        max_instances: int,
        auto_resume: bool,
        auto_suspend_secs: Optional[int],
        external_access_integrations: Optional[List[str]],
        query_warehouse: Optional[str],
        tags: Optional[List[Tag]],
        comment: Optional[str],
        service_project_paths: ServiceProjectPaths,
        upgrade: bool,
    ) -> SnowflakeCursor:
        stage_manager = StageManager()
        stage_manager.create(fqn=FQN.from_stage(stage))

        stage = stage_manager.get_standard_stage_prefix(stage)
        self._upload_artifacts(
            stage_manager=stage_manager,
            service_project_paths=service_project_paths,
            artifacts=artifacts,
            stage=stage,
        )

        if upgrade:
            self.set_property(
                service_name=service_name,
                min_instances=min_instances,
                max_instances=max_instances,
                query_warehouse=query_warehouse,
                auto_resume=auto_resume,
                auto_suspend_secs=auto_suspend_secs,
                external_access_integrations=external_access_integrations,
                comment=comment,
            )
            query = [
                f"ALTER SERVICE {service_name}",
                f"FROM {stage}",
                f"SPECIFICATION_FILE = '{spec_path}'",
            ]
            return self.execute_query(strip_empty_lines(query))
        else:
            query = [
                f"CREATE SERVICE {service_name}",
                f"IN COMPUTE POOL {compute_pool}",
                f"FROM {stage}",
                f"SPECIFICATION_FILE = '{spec_path}'",
                f"AUTO_RESUME = {auto_resume}",
            ]

            if min_instances:
                query.append(f"MIN_INSTANCES = {min_instances}")

            if max_instances:
                query.append(f"MAX_INSTANCES = {max_instances}")

            if auto_suspend_secs is not None:
                query.append(f"AUTO_SUSPEND_SECS = {auto_suspend_secs}")

            if query_warehouse:
                query.append(f"QUERY_WAREHOUSE = {query_warehouse}")

            if external_access_integrations:
                external_access_integration_list = ",".join(
                    f"{e}" for e in external_access_integrations
                )
                query.append(
                    f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_list})"
                )

            if comment:
                query.append(f"COMMENT = {comment}")

            if tags:
                tag_list = ",".join(
                    f"{t.name}={t.value_string_literal()}" for t in tags
                )
                query.append(f"WITH TAG ({tag_list})")

            try:
                return self.execute_query(strip_empty_lines(query))
            except ProgrammingError as e:
                handle_object_already_exists(e, ObjectType.SERVICE, service_name)

    @staticmethod
    def _upload_artifacts(
        stage_manager: StageManager,
        service_project_paths: ServiceProjectPaths,
        artifacts: Artifacts,
        stage: str,
    ):
        if not artifacts:
            raise ValueError("Service needs to have artifacts to deploy")

        service_project_paths.remove_up_bundle_root()
        SecurePath(service_project_paths.bundle_root).mkdir(parents=True, exist_ok=True)
        bundle_map = bundle_artifacts(service_project_paths, artifacts)
        for absolute_src, absolute_dest in bundle_map.all_mappings(
            absolute=True, expand_directories=True
        ):
            # We treat the bundle/service root as deploy root
            stage_path = StagePath.from_stage_str(stage) / (
                absolute_dest.relative_to(service_project_paths.bundle_root).parent
            )
            stage_manager.put(
                local_path=absolute_dest, stage_path=stage_path, overwrite=True
            )
        service_project_paths.clean_up_output()

    def _execute_job_service(
        self,
        job_service_name: str,
        compute_pool: str,
        spec_json: str,
        external_access_integrations: Optional[List[str]] = None,
        query_warehouse: Optional[str] = None,
        comment: Optional[str] = None,
        async_mode: Optional[bool] = None,
        replicas: Optional[int] = None,
    ) -> SnowflakeCursor:
        """
        Common method to execute a job service with the given specification.

        Args:
            job_service_name: Name for the job service
            compute_pool: Compute pool to run the job
            spec_json: Job specification as a JSON string
            external_access_integrations: List of EAI names
            query_warehouse: Query warehouse to use
            comment: Comment for the job
            async_mode: Whether to run asynchronously (if None, parameter is omitted)
            replicas: Number of job replicas to run (if None, parameter is omitted)
        """

        query = f"""\
                EXECUTE JOB SERVICE
                IN COMPUTE POOL {compute_pool}
                FROM SPECIFICATION $$
                {spec_json}
                $$
                NAME = {job_service_name}
                """.splitlines()

        if async_mode is not None:
            query.append(f"ASYNC = {str(async_mode).upper()}")

        if replicas is not None:
            query.append(f"REPLICAS = {replicas}")

        if external_access_integrations:
            external_access_integration_list = ",".join(
                f"{e}" for e in external_access_integrations
            )
            query.append(
                f"EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_list})"
            )

        if query_warehouse:
            query.append(f"QUERY_WAREHOUSE = {query_warehouse}")

        if comment:
            query.append(f"COMMENT = {comment}")

        try:
            return self.execute_query(strip_empty_lines(query))
        except ProgrammingError as e:
            handle_object_already_exists(e, ObjectType.SERVICE, job_service_name)

    def execute_job(
        self,
        job_service_name: str,
        compute_pool: str,
        spec_path: Path,
        external_access_integrations: Optional[List[str]],
        query_warehouse: Optional[str],
        comment: Optional[str],
        async_mode: bool = False,
        replicas: Optional[int] = None,
    ) -> SnowflakeCursor:
        spec = self._read_yaml(spec_path)
        return self._execute_job_service(
            job_service_name=job_service_name,
            compute_pool=compute_pool,
            spec_json=spec,
            external_access_integrations=external_access_integrations,
            query_warehouse=query_warehouse,
            comment=comment,
            async_mode=async_mode
            if async_mode
            else None,  # Only pass if True to maintain backward compatibility
            replicas=replicas,
        )

    def build_image(
        self,
        job_service_name: str,
        compute_pool: str,
        image_repository: str,
        image_name: str,
        image_tag: str,
        stage: str,
        build_context_path: str,
        external_access_integrations: Optional[List[str]],
        async_mode: bool = True,
    ) -> SnowflakeCursor:
        """
        Builds a container image using EXECUTE JOB SERVICE with the Snowflake image builder.

        Args:
            job_service_name: Name for the build job service
            compute_pool: Compute pool to run the build job
            image_repository: Repository path in format [db.][schema.]repo_name
            image_name: Name for the built image
            image_tag: Tag for the built image
            stage: Stage where the build context is uploaded
            build_context_path: Path on stage where build context is located
            external_access_integrations: List of EAI names
            async_mode: Whether to run the build asynchronously
        """
        # Get organization name
        # Using execute_string (same as get_account) for consistency
        *_, org_cursor = self._conn.execute_string(
            "SELECT CURRENT_ORGANIZATION_NAME()", cursor_class=DictCursor
        )
        org_name = org_cursor.fetchone()["CURRENT_ORGANIZATION_NAME()"].lower()

        # Get account name using existing utility function
        account_name = get_account(self._conn)

        # Parse the image repository using FQN utility
        # This handles [db.][schema.]repo_name format automatically
        repo_fqn = FQN.from_string(image_repository).using_connection(self._conn)

        # Validate that we have all required parts
        # Note: FQN parsing gives us: "db.schema.repo" → both present,
        # "schema.repo" → only schema, "repo" → neither
        # It's impossible to have database without schema
        if not repo_fqn.database or not repo_fqn.schema:
            missing_parts = []
            if not repo_fqn.database:
                missing_parts.append("database")
            if not repo_fqn.schema:
                missing_parts.append("schema")
            raise ValueError(
                f"Image repository requires database and schema. "
                f"Missing: {', '.join(missing_parts)}. "
                f"Either provide a fully qualified name 'database.schema.repository' "
                f"or set the missing parts in your connection context. "
                f"Provided: '{image_repository}'"
            )

        # Build the image registry URL
        # Format: <org>-<account-name>.registry-local.snowflakecomputing.com/<db>/<schema>/<repo>
        image_registry_url = (
            f"{org_name}-{account_name}.registry-local.snowflakecomputing.com/"
            f"{repo_fqn.database.lower()}/{repo_fqn.schema.lower()}/{repo_fqn.name.lower()}"
        )

        # Create the specification for the image build job
        spec = {
            "spec": {
                "containers": [
                    {
                        "name": "main",
                        "image": "/snowflake/images/snowflake_images/sf-image-build:0.0.1",
                        "env": {
                            "IMAGE_REGISTRY_URL": image_registry_url,
                            "IMAGE_NAME": image_name,
                            "IMAGE_TAG": image_tag,
                            "BUILD_CONTEXT": "/app",
                        },
                        "volumeMounts": [
                            {
                                "name": "code-volume",
                                "mountPath": "/app",
                            }
                        ],
                    }
                ],
                "volumes": [
                    {
                        "name": "code-volume",
                        "source": f"@{stage}/{build_context_path}",
                        "uid": 65532,
                    }
                ],
            }
        }

        spec_json = json.dumps(spec)

        return self._execute_job_service(
            job_service_name=job_service_name,
            compute_pool=compute_pool,
            spec_json=spec_json,
            external_access_integrations=external_access_integrations,
            async_mode=async_mode,
        )

    def _read_yaml(self, path: Path) -> str:
        # TODO(aivanou): Add validation towards schema
        with SecurePath(path).open("r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fh:
            data = yaml.safe_load(fh)
        return json.dumps(data)

    def status(self, service_name: str) -> SnowflakeCursor:
        return self.execute_query(f"CALL SYSTEM$GET_SERVICE_STATUS('{service_name}')")

    def logs(
        self,
        service_name: str,
        instance_id: str,
        container_name: str,
        num_lines: int,
        previous_logs: bool = False,
        since_timestamp: str = "",
        include_timestamps: bool = False,
    ):
        cursor = self.execute_query(
            f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '{instance_id}', '{container_name}', "
            f"{num_lines}, {previous_logs}, '{since_timestamp}', {include_timestamps});"
        )

        for log in cursor.fetchall():
            yield log[0] if isinstance(log, tuple) else log

    def stream_logs(
        self,
        service_name: str,
        instance_id: str,
        container_name: str,
        num_lines: int,
        since_timestamp: str,
        include_timestamps: bool,
        interval_seconds: int,
        check_terminal_status: bool = False,
    ):
        """
        Stream logs from a service container with optional terminal status checking.

        Args:
            check_terminal_status: If True, will check service status and stop streaming
                                  when a terminal status (DONE, FAILED, CANCELLED) is reached.
                                  The final status will be yielded as the last item.
        """
        try:
            prev_timestamp = since_timestamp
            prev_log_records: List[str] = []

            while True:
                raw_log_blocks = [
                    log
                    for log in self.logs(
                        service_name=service_name,
                        instance_id=instance_id,
                        container_name=container_name,
                        num_lines=num_lines,
                        since_timestamp=prev_timestamp,
                        include_timestamps=True,
                    )
                ]

                new_log_records = []
                for block in raw_log_blocks:
                    new_log_records.extend(block.split("\n"))

                new_log_records = [line for line in new_log_records if line.strip()]

                if new_log_records:
                    dedup_log_records = new_logs_only(prev_log_records, new_log_records)
                    if dedup_log_records:
                        for log in dedup_log_records:
                            yield filter_log_timestamp(log, include_timestamps)

                        prev_timestamp = dedup_log_records[-1].split(" ", 1)[0]
                        prev_log_records = dedup_log_records

                # Check for terminal status if requested
                if check_terminal_status:
                    # Use describe API to get service properties including status
                    describe_result = ObjectManager().describe(
                        object_type="service",
                        fqn=FQN.from_string(service_name),
                        cursor_class=DictCursor,
                    )

                    # Get the status from describe output (single row with status column)
                    result_row = describe_result.fetchone()
                    job_status = result_row.get("status") if result_row else None

                    if job_status and job_status in ["DONE", "FAILED", "CANCELLED"]:
                        # Yield terminal status as a tuple to distinguish from log lines
                        yield ("__TERMINAL_STATUS__", job_status)
                        return

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            return

    def get_account_event_table(self):
        query = "show parameters like 'event_table' in account"
        results = self.execute_query(query, cursor_class=DictCursor)
        event_table = next(
            (r["value"] for r in results if r["key"] == "EVENT_TABLE"), ""
        )
        if not event_table:
            raise SPCSEventTableError("No SPCS event table configured in the account.")
        return event_table

    def get_events(
        self,
        service_name: str,
        instance_id: str,
        container_name: str,
        since: str | datetime | None = None,
        until: str | datetime | None = None,
        first: Optional[int] = None,
        last: Optional[int] = None,
        show_all_columns: bool = False,
    ):

        account_event_table = self.get_account_event_table()
        resource_clause = build_resource_clause(
            service_name, instance_id, container_name
        )
        since_clause, until_clause = build_time_clauses(since, until)

        first_clause = f"limit {first}" if first is not None else ""
        last_clause = f"limit {last}" if last is not None else ""

        query = f"""\
                     select *
                    from (
                        select *
                        from {account_event_table}
                        where (
                            {resource_clause}
                            {since_clause}
                            {until_clause}
                        )
                        and record_type = 'LOG'
                        and scope['name'] = 'snow.spcs.platform'
                        order by timestamp desc
                        {last_clause}
                    )
                    order by timestamp asc
                    {first_clause}
                """

        cursor = self.execute_query(query)
        raw_events = cursor.fetchall()
        if not raw_events:
            return []

        if show_all_columns:
            return [dict(zip(EVENT_COLUMN_NAMES, event)) for event in raw_events]

        formatted_events = []
        for raw_event in raw_events:
            event_dict = dict(zip(EVENT_COLUMN_NAMES, raw_event))
            formatted = format_event_row(event_dict)
            formatted_events.append(formatted)

        return formatted_events

    def get_all_metrics(
        self,
        service_name: FQN | str,
        instance_id: str,
        container_name: str,
        since: str | datetime | None = None,
        until: str | datetime | None = None,
        show_all_columns: bool = False,
    ):
        service_name, database, schema = parse_service_details(service_name)

        account_event_table = self.get_account_event_table()
        resource_clause = build_resource_clause(
            service_name, instance_id, container_name
        )
        since_clause, until_clause = build_time_clauses(since, until)

        db_and_schema_clause = ""
        if database:
            db_and_schema_clause = build_db_and_schema_clause(
                database_name=database, schema_name=schema
            )

        query = f"""\
                    select *
                    from {account_event_table}
                    where (
                        {resource_clause}
                        {db_and_schema_clause}
                        {since_clause}
                        {until_clause}
                    )
                    and record_type = 'METRIC'
                    and scope['name'] = 'snow.spcs.platform'
                    order by timestamp desc
                """

        cursor = self.execute_query(query)
        raw_metrics = cursor.fetchall()
        if not raw_metrics:
            return []

        if show_all_columns:
            return [dict(zip(EVENT_COLUMN_NAMES, metric)) for metric in raw_metrics]

        formatted_metrics = []
        for raw_metric in raw_metrics:
            metric_dict = dict(zip(EVENT_COLUMN_NAMES, raw_metric))
            formatted = format_metric_row(metric_dict)
            formatted_metrics.append(formatted)

        return formatted_metrics

    def get_latest_metrics(
        self,
        service_name: str,
        instance_id: str,
        container_name: str,
        show_all_columns: bool = False,
    ):
        service_name, database, schema = parse_service_details(service_name)

        account_event_table = self.get_account_event_table()
        resource_clause = build_resource_clause(
            service_name, instance_id, container_name
        )

        db_and_schema_clause = ""
        if database:
            db_and_schema_clause = build_db_and_schema_clause(
                database_name=database, schema_name=schema
            )

        query = f"""
            with rankedmetrics as (
                select
                    *,
                    row_number() over (
                        partition by record['metric']['name']
                        order by timestamp desc
                    ) as rank
                from {account_event_table}
                where
                    record_type = 'METRIC'
                    and scope['name'] = 'snow.spcs.platform'
                    and {resource_clause}
                    {db_and_schema_clause}
                    and timestamp > dateadd('hour', -1, current_timestamp)
            )
            select *
            from rankedmetrics
            where rank = 1
            order by timestamp desc;
        """

        cursor = self.execute_query(query)
        raw_metrics = cursor.fetchall()
        if not raw_metrics:
            return []

        if show_all_columns:
            return [dict(zip(EVENT_COLUMN_NAMES, metric)) for metric in raw_metrics]

        formatted_metrics = []
        for raw_metric in raw_metrics:
            metric_dict = dict(zip(EVENT_COLUMN_NAMES, raw_metric))
            formatted = format_metric_row(metric_dict)
            formatted_metrics.append(formatted)

        return formatted_metrics

    def upgrade_spec(self, service_name: str, spec_path: Path):
        spec = self._read_yaml(spec_path)
        query = f"alter service {service_name} from specification $$ {spec} $$"
        return self.execute_query(query)

    def list_endpoints(self, service_name: str) -> SnowflakeCursor:
        return self.execute_query(f"show endpoints in service {service_name}")

    def list_instances(self, service_name: str) -> SnowflakeCursor:
        return self.execute_query(f"show service instances in service {service_name}")

    def list_containers(self, service_name: str) -> SnowflakeCursor:
        return self.execute_query(f"show service containers in service {service_name}")

    def list_roles(self, service_name: str) -> SnowflakeCursor:
        return self.execute_query(f"show roles in service {service_name}")

    def suspend(self, service_name: str):
        return self.execute_query(f"alter service {service_name} suspend")

    def resume(self, service_name: str):
        return self.execute_query(f"alter service {service_name} resume")

    def set_property(
        self,
        service_name: str,
        min_instances: Optional[int],
        max_instances: Optional[int],
        query_warehouse: Optional[str],
        auto_resume: Optional[bool],
        auto_suspend_secs: Optional[int],
        external_access_integrations: Optional[List[str]],
        comment: Optional[str],
    ):
        property_pairs = [
            ("min_instances", min_instances),
            ("max_instances", max_instances),
            ("query_warehouse", query_warehouse),
            ("auto_resume", auto_resume),
            ("auto_suspend_secs", auto_suspend_secs),
            ("external_access_integrations", external_access_integrations),
            ("comment", comment),
        ]

        # Check if all provided properties are set to None (no properties are being set)
        if all([value is None for property_name, value in property_pairs]):
            raise NoPropertiesProvidedError(
                f"No properties specified for service '{service_name}'. Please provide at least one property to set."
            )
        query: List[str] = [f"alter service {service_name} set "]

        if min_instances is not None:
            query.append(f" min_instances = {min_instances}")

        if max_instances is not None:
            query.append(f" max_instances = {max_instances}")

        if query_warehouse is not None:
            query.append(f" query_warehouse = {query_warehouse}")

        if auto_resume is not None:
            query.append(f" auto_resume = {auto_resume}")

        if auto_suspend_secs is not None:
            query.append(f" auto_suspend_secs = {auto_suspend_secs}")

        if external_access_integrations is not None:
            external_access_integration_list = ",".join(
                f"{e}" for e in external_access_integrations
            )
            query.append(
                f"external_access_integrations = ({external_access_integration_list})"
            )

        if comment is not None:
            query.append(f" comment = {comment}")

        return self.execute_query(strip_empty_lines(query))

    def unset_property(
        self,
        service_name: str,
        min_instances: bool,
        max_instances: bool,
        query_warehouse: bool,
        auto_resume: bool,
        auto_suspend_secs: bool,
        comment: bool,
    ):
        property_pairs = [
            ("min_instances", min_instances),
            ("max_instances", max_instances),
            ("query_warehouse", query_warehouse),
            ("auto_resume", auto_resume),
            ("auto_suspend_secs", auto_suspend_secs),
            ("comment", comment),
        ]

        # Check if all properties provided are False (no properties are being unset)
        if not any([value for property_name, value in property_pairs]):
            raise NoPropertiesProvidedError(
                f"No properties specified for service '{service_name}'. Please provide at least one property to reset to its default value."
            )
        unset_list = [property_name for property_name, value in property_pairs if value]
        query = f"alter service {service_name} unset {','.join(unset_list)}"
        return self.execute_query(query)


def parse_service_details(
    service_identifier: str | FQN,
) -> tuple[str, str | None, str | None]:
    if isinstance(service_identifier, FQN):
        name = service_identifier.name
        database = service_identifier.database
        schema = service_identifier.schema
    else:
        name = service_identifier
        database = None
        schema = None

    return name, database, schema
