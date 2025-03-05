from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Dict, Generator, List, Literal, Optional, TypedDict

import typer
from click import ClickException, UsageError
from pydantic import Field, field_validator
from snowflake.cli._plugins.connection.util import (
    UIParameter,
    make_snowsight_url,
)
from snowflake.cli._plugins.nativeapp.artifacts import (
    find_events_definitions_in_manifest_file,
)
from snowflake.cli._plugins.nativeapp.common_flags import (
    ForceOption,
    InteractiveOption,
    ValidateOption,
)
from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    OWNER_COL,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.entities.models.event_sharing_telemetry import (
    EventSharingTelemetry,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
    NoEventTableForAccount,
)
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
    PolicyBase,
)
from snowflake.cli._plugins.nativeapp.same_account_install_method import (
    SameAccountInstallMethod,
)
from snowflake.cli._plugins.nativeapp.sf_facade import get_snowflake_facade
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    UpgradeApplicationRestrictionError,
)
from snowflake.cli._plugins.nativeapp.utils import needs_confirmation
from snowflake.cli._plugins.stage.manager import DefaultStagePathParts
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.cli_global_context import get_cli_context, span
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.entities.common import (
    EntityBase,
    attach_spans_to_entity_actions,
)
from snowflake.cli.api.entities.utils import (
    drop_generic_object,
    execute_post_deploy_hooks,
    generic_sql_error_handler,
    get_sql_executor,
    print_messages,
)
from snowflake.cli.api.errno import (
    APPLICATION_NO_LONGER_AVAILABLE,
    APPLICATION_OWNS_EXTERNAL_OBJECTS,
    DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
)
from snowflake.cli.api.metrics import CLICounterField
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBase,
    Identifier,
    PostDeployHook,
    TargetField,
)
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField
from snowflake.cli.api.project.util import (
    append_test_resource_suffix,
    identifier_for_url,
    to_identifier,
    unquote_identifier,
)
from snowflake.connector import DictCursor, ProgrammingError

log = logging.getLogger(__name__)

ApplicationOwnedObject = TypedDict("ApplicationOwnedObject", {"name": str, "type": str})


class EventSharingHandler:
    """
    Handles the logic around event sharing for applications.
    This class is used to determine whether event sharing should be authorized or not, and which events should be shared.
    """

    def __init__(
        self,
        *,
        telemetry_definition: Optional[EventSharingTelemetry],
        deploy_root: Path,
        install_method: SameAccountInstallMethod,
        console: AbstractConsole,
    ):
        """
        Initializes the event sharing handler.
        If telemetry_definition is not present or share_mandatory_events is not set,
        we will default to sharing events if mandatory events are present in the manifest file and we are in dev mode.

        :param telemetry_definition: The telemetry configuration for this application from the PDF if present.
        :param deploy_root: The root directory of the application package.
        :param install_method: The install method of the application.
        :param console: The console to use for logging.
        """

        self._is_dev_mode = install_method.is_dev_mode
        self._metrics = get_cli_context().metrics
        self._console = console

        self._event_sharing_enabled = get_snowflake_facade().get_ui_parameter(
            UIParameter.NA_EVENT_SHARING_V2, True
        )
        self._event_sharing_enforced = get_snowflake_facade().get_ui_parameter(
            UIParameter.NA_ENFORCE_MANDATORY_FILTERS, True
        )

        self._share_mandatory_events = (
            telemetry_definition and telemetry_definition.share_mandatory_events
        )
        self._optional_shared_events = (
            telemetry_definition and telemetry_definition.optional_shared_events
        ) or []

        self._metrics.set_counter(
            CLICounterField.EVENT_SHARING, int(self._share_mandatory_events or False)
        )
        self._metrics.set_counter(CLICounterField.EVENT_SHARING_WARNING, 0)
        self._metrics.set_counter(CLICounterField.EVENT_SHARING_ERROR, 0)

        if not self._event_sharing_enabled:
            # We cannot set AUTHORIZE_TELEMETRY_EVENT_SHARING to True or False if event sharing is not enabled,
            # so we will ignore the field in both cases, but warn only if it is set to True
            if self._share_mandatory_events or self._optional_shared_events:
                self.event_sharing_warning(
                    "Same-account event sharing is not enabled in your account, therefore, application telemetry section will be ignored."
                )
            self._share_mandatory_events = None
            self._optional_shared_events = []
            return

        if install_method.is_dev_mode:
            mandatory_events_in_manifest = self._contains_mandatory_events(
                find_events_definitions_in_manifest_file(deploy_root)
            )

            if mandatory_events_in_manifest and self._event_sharing_enforced:
                if self._share_mandatory_events is None:
                    self.event_sharing_warning(
                        "Mandatory events are present in the manifest file. Automatically authorizing event sharing in dev mode. To suppress this warning, please add 'share_mandatory_events: true' in the application telemetry section."
                    )
                    self._share_mandatory_events = True

    def _contains_mandatory_events(self, events_definitions: List[Dict[str, str]]):
        return any(event["sharing"] == "MANDATORY" for event in events_definitions)

    def should_authorize_event_sharing(
        self,
    ) -> Optional[bool]:
        """
        Determines whether event sharing should be authorized.

        Outputs:
        - None: Event sharing should not be updated or explicitly set.
        - True: Event sharing should be authorized.
        - False: Event sharing should be disabled.
        """

        if not self._event_sharing_enabled:
            return None

        return self._share_mandatory_events

    def event_sharing_warning(self, message: str):
        """
        Logs a warning message about event sharing, and emits an event sharing warning metric.
        """

        self._metrics.set_counter(CLICounterField.EVENT_SHARING_WARNING, 1)
        self._console.warning(f"WARNING: {message}")

    def event_sharing_error(self, message: str, err: Exception):
        """
        Raises an error about event sharing, and emits an event sharing error metric.
        """

        self._metrics.set_counter(CLICounterField.EVENT_SHARING_ERROR, 1)
        raise ClickException(message) from err

    def events_to_share(
        self, events_definitions: List[Dict[str, str]]
    ) -> Optional[List[str]]:
        """
        Determines which events should be shared based on the application events definitions.
        It also combines the mandatory events with the optional shared events.

        :param events_definitions: The events definitions of the application.

        Outputs:
        - None: No events should be updated.
        - List[str]: The names of the events that should be shared.
        """

        # events definition has this format: [{'name': 'SNOWFLAKE$ERRORS_AND_WARNINGS', 'type': 'ERRORS_AND_WARNINGS', 'sharing': 'MANDATORY'...}]
        events_map = {event["type"]: event for event in events_definitions}
        events_names = []
        for event_type in self._optional_shared_events:
            if event_type not in events_map:
                raise ClickException(
                    f"Event '{event_type}' from application 'telemetry' section in the project definition file could not be found in the application."
                )
            else:
                events_names.append(events_map[event_type]["name"])

        # add mandatory events to event_names list:
        for event in events_definitions:
            if event["sharing"] == "MANDATORY":
                events_names.append(event["name"])

        if not self._share_mandatory_events:
            if (
                self._contains_mandatory_events(events_definitions)
                and not self._event_sharing_enforced
            ):
                self.event_sharing_warning(
                    "Mandatory events are present in the application, but event sharing is not authorized in the application telemetry field. This will soon be required to set in order to deploy this application."
                )
            return None

        return sorted(list(set(events_names)))


class ApplicationEntityModel(EntityModelBase):
    type: Literal["application"] = DiscriminatorField()  # noqa A003
    from_: TargetField[ApplicationPackageEntityModel] = Field(
        alias="from",
        title="An application package this entity should be created from",
    )
    debug: Optional[bool] = Field(
        title="Whether to enable debug mode when using a named stage to create an application object",
        default=None,
    )
    telemetry: Optional[EventSharingTelemetry] = Field(
        title="Telemetry configuration for the application",
        default=None,
    )

    @field_validator("identifier")
    @classmethod
    def append_test_resource_suffix_to_identifier(
        cls, input_value: Identifier | str
    ) -> Identifier | str:
        identifier = (
            input_value.name if isinstance(input_value, Identifier) else input_value
        )
        with_suffix = append_test_resource_suffix(identifier)
        if isinstance(input_value, Identifier):
            return input_value.model_copy(update=dict(name=with_suffix))
        return with_suffix


@attach_spans_to_entity_actions(entity_name="app")
class ApplicationEntity(EntityBase[ApplicationEntityModel]):
    """
    A Native App application object, created from an application package.
    """

    @property
    def project_root(self) -> Path:
        return self._workspace_ctx.project_root

    @property
    def package_entity_id(self) -> str:
        return self._entity_model.from_.target

    @property
    def name(self) -> str:
        return to_identifier(self._entity_model.fqn.name)

    @property
    def role(self) -> str:
        model = self._entity_model
        return (model.meta and model.meta.role) or self._workspace_ctx.default_role

    @property
    def warehouse(self) -> str:
        model = self._entity_model
        return (
            model.meta and model.meta.warehouse and to_identifier(model.meta.warehouse)
        ) or to_identifier(self._workspace_ctx.default_warehouse)

    @property
    def post_deploy_hooks(self) -> list[PostDeployHook] | None:
        model = self._entity_model
        return model.meta and model.meta.post_deploy

    @property
    def console(self) -> AbstractConsole:
        return self._workspace_ctx.console

    @property
    def debug(self) -> bool | None:
        return self._entity_model.debug

    @property
    def telemetry(self) -> EventSharingTelemetry | None:
        return self._entity_model.telemetry

    def action_deploy(
        self,
        action_ctx: ActionContext,
        from_release_directive: bool,
        prune: bool,
        recursive: bool,
        paths: List[Path],
        release_channel: Optional[str] = None,
        validate: bool = ValidateOption,
        interactive: bool = InteractiveOption,
        version: Optional[str] = None,
        patch: Optional[int] = None,
        force: Optional[bool] = ForceOption,
        *args,
        **kwargs,
    ):
        """
        Create or upgrade the application object using the given strategy
        (unversioned dev, versioned dev, or same-account release directive).
        """
        package_entity: ApplicationPackageEntity = action_ctx.get_entity(
            self.package_entity_id
        )

        stage_path = package_entity.stage_path

        if force:
            policy = AllowAlwaysPolicy()
        elif interactive:
            policy = AskAlwaysPolicy()
        else:
            policy = DenyAlwaysPolicy()

        # same-account release directive
        if from_release_directive:
            release_channel = package_entity.get_sanitized_release_channel(
                release_channel
            )

            self.create_or_upgrade_app(
                package=package_entity,
                stage_path=stage_path,
                install_method=SameAccountInstallMethod.release_directive(),
                release_channel=release_channel,
                policy=policy,
                interactive=interactive,
            )
            return

        if release_channel:
            raise UsageError(
                f"Release channel is only supported when --from-release-directive is used."
            )

        # versioned dev
        if version:
            try:
                version_exists = package_entity.get_existing_version_info(version)
                if not version_exists:
                    raise UsageError(
                        f"Application package {package_entity.name} does not have any version {version} defined. Use 'snow app version create' to define a version in the application package first."
                    )
            except ApplicationPackageDoesNotExistError as app_err:
                raise UsageError(
                    f"Application package {package_entity.name} does not exist. Use 'snow app version create' to first create an application package and then define a version in it."
                )

            self.create_or_upgrade_app(
                package=package_entity,
                stage_path=stage_path,
                install_method=SameAccountInstallMethod.versioned_dev(version, patch),
                policy=policy,
                interactive=interactive,
            )
            return

        # unversioned dev
        package_entity.action_deploy(
            action_ctx=action_ctx,
            prune=True,
            recursive=True,
            paths=[],
            validate=validate,
            interactive=interactive,
            force=force,
        )
        self.create_or_upgrade_app(
            package=package_entity,
            stage_path=stage_path,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            policy=policy,
            interactive=interactive,
        )

    def action_drop(
        self,
        action_ctx: ActionContext,
        interactive: bool,
        force_drop: bool = False,
        cascade: Optional[bool] = None,
        *args,
        **kwargs,
    ):
        """
        Attempts to drop the application object if all validations and user prompts allow so.
        """
        needs_confirm = True

        # 1. If existing application is not found, exit gracefully
        show_obj_row = get_snowflake_facade().get_existing_app_info(
            self.name, self.role
        )
        if show_obj_row is None:
            self.console.warning(
                f"Role {self.role} does not own any application object with the name {self.name}, or the application object does not exist."
            )
            return

        # 2. Check if created by the Snowflake CLI
        row_comment = show_obj_row[COMMENT_COL]
        if row_comment not in ALLOWED_SPECIAL_COMMENTS and needs_confirmation(
            needs_confirm, force_drop
        ):
            should_drop_object = typer.confirm(
                dedent(
                    f"""\
                        Application object {self.name} was not created by Snowflake CLI.
                        Application object details:
                        Name: {self.name}
                        Created on: {show_obj_row["created_on"]}
                        Source: {show_obj_row["source"]}
                        Owner: {show_obj_row[OWNER_COL]}
                        Comment: {show_obj_row[COMMENT_COL]}
                        Version: {show_obj_row["version"]}
                        Patch: {show_obj_row["patch"]}
                        Are you sure you want to drop it?
                    """
                )
            )
            if not should_drop_object:
                self.console.message(f"Did not drop application object {self.name}.")
                # The user desires to keep the app, therefore we can't proceed since it would
                # leave behind an orphan app when we get to dropping the package
                raise typer.Abort()

        # 3. Check for application objects owned by the application
        # This query will fail if the application package has already been dropped, so handle this case gracefully
        has_objects_to_drop = False
        message_prefix = ""
        cascade_true_message = ""
        cascade_false_message = ""
        interactive_prompt = ""
        non_interactive_abort = ""
        try:
            if application_objects := self.get_objects_owned_by_application():
                has_objects_to_drop = True
                message_prefix = (
                    f"The following objects are owned by application {self.name}"
                )
                cascade_true_message = f"{message_prefix} and will be dropped:"
                cascade_false_message = f"{message_prefix} and will NOT be dropped:"
                interactive_prompt = "Would you like to drop these objects in addition to the application? [y/n/ABORT]"
                non_interactive_abort = "Re-run teardown again with --cascade or --no-cascade to specify whether these objects should be dropped along with the application"
        except ProgrammingError as e:
            if e.errno != APPLICATION_NO_LONGER_AVAILABLE:
                raise
            application_objects = []
            message_prefix = f"Could not determine which objects are owned by application {self.name}"
            has_objects_to_drop = True  # potentially, but we don't know what they are
            cascade_true_message = (
                f"{message_prefix}, an unknown number of objects will be dropped."
            )
            cascade_false_message = f"{message_prefix}, they will NOT be dropped."
            interactive_prompt = f"Would you like to drop an unknown set of objects in addition to the application? [y/n/ABORT]"
            non_interactive_abort = f"Re-run teardown again with --cascade or --no-cascade to specify whether any objects should be dropped along with the application."

        if has_objects_to_drop:
            if cascade is True:
                # If the user explicitly passed the --cascade flag
                self.console.message(cascade_true_message)
                with self.console.indented():
                    for obj in application_objects:
                        self.console.message(_application_object_to_str(obj))
            elif cascade is False:
                # If the user explicitly passed the --no-cascade flag
                self.console.message(cascade_false_message)
                with self.console.indented():
                    for obj in application_objects:
                        self.console.message(_application_object_to_str(obj))
            elif interactive:
                # If the user didn't pass any cascade flag and the session is interactive
                self.console.message(message_prefix)
                with self.console.indented():
                    for obj in application_objects:
                        self.console.message(_application_object_to_str(obj))
                user_response = typer.prompt(
                    interactive_prompt,
                    show_default=False,
                    default="ABORT",
                ).lower()
                if user_response in ["y", "yes"]:
                    cascade = True
                elif user_response in ["n", "no"]:
                    cascade = False
                else:
                    raise typer.Abort()
            else:
                # Else abort since we don't know what to do and can't ask the user
                self.console.message(message_prefix)
                with self.console.indented():
                    for obj in application_objects:
                        self.console.message(_application_object_to_str(obj))
                self.console.message(non_interactive_abort)
                raise typer.Abort()
        elif cascade is None:
            # If there's nothing to drop, set cascade to an explicit False value
            cascade = False

        # 4. All validations have passed, drop object
        drop_generic_object(
            console=self.console,
            object_type="application",
            object_name=self.name,
            role=self.role,
            cascade=cascade,
        )

    def action_events(
        self,
        action_ctx: ActionContext,
        since: str | datetime | None = None,
        until: str | datetime | None = None,
        record_types: list[str] | None = None,
        scopes: list[str] | None = None,
        consumer_org: str = "",
        consumer_account: str = "",
        consumer_app_hash: str = "",
        first: int = -1,
        last: int = -1,
        follow: bool = False,
        interval_seconds: int = 10,
        *args,
        **kwargs,
    ):
        package_entity: ApplicationPackageEntity = action_ctx.get_entity(
            self.package_entity_id
        )
        if follow:
            return self.stream_events(
                package_name=package_entity.name,
                interval_seconds=interval_seconds,
                since=since,
                record_types=record_types,
                scopes=scopes,
                consumer_org=consumer_org,
                consumer_account=consumer_account,
                consumer_app_hash=consumer_app_hash,
                last=last,
            )
        else:
            return self.get_events(
                package_name=package_entity.name,
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

    def get_objects_owned_by_application(self) -> List[ApplicationOwnedObject]:
        """
        Returns all application objects owned by this application.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(self.role):
            results = sql_executor.execute_query(
                f"show objects owned by application {self.name}"
            ).fetchall()
            return [{"name": row[1], "type": row[2]} for row in results]

    def _upgrade_app(
        self,
        stage_path: DefaultStagePathParts,
        install_method: SameAccountInstallMethod,
        event_sharing: EventSharingHandler,
        policy: PolicyBase,
        interactive: bool,
        release_channel: Optional[str] = None,
    ) -> list[tuple[str]] | None:
        self.console.step(f"Upgrading existing application object {self.name}.")

        try:
            return get_snowflake_facade().upgrade_application(
                name=self.name,
                install_method=install_method,
                path_to_version_directory=stage_path.full_path,
                debug_mode=self.debug,
                should_authorize_event_sharing=event_sharing.should_authorize_event_sharing(),
                release_channel=release_channel,
                role=self.role,
                warehouse=self.warehouse,
            )
        except UpgradeApplicationRestrictionError as err:
            self.console.warning(err.message)
            self.drop_application_before_upgrade(policy=policy, interactive=interactive)
            return None

    def _create_app(
        self,
        stage_path: DefaultStagePathParts,
        install_method: SameAccountInstallMethod,
        event_sharing: EventSharingHandler,
        package: ApplicationPackageEntity,
        release_channel: Optional[str] = None,
    ) -> list[tuple[str]]:
        self.console.step(f"Creating new application object {self.name} in account.")

        if package.role != self.role:
            get_snowflake_facade().grant_privileges_to_role(
                privileges=["install", "develop"],
                object_type=ObjectType.APPLICATION_PACKAGE,
                object_identifier=package.name,
                role_to_grant=self.role,
                role_to_use=package.role,
            )

            stage_schema = stage_path.schema
            get_snowflake_facade().grant_privileges_to_role(
                privileges=["usage"],
                object_type=ObjectType.SCHEMA,
                object_identifier=f"{package.name}.{stage_schema}",
                role_to_grant=self.role,
                role_to_use=package.role,
            )

            get_snowflake_facade().grant_privileges_to_role(
                privileges=["read"],
                object_type=ObjectType.STAGE,
                object_identifier=stage_path.stage,
                role_to_grant=self.role,
                role_to_use=package.role,
            )

        create_app_result, warnings = get_snowflake_facade().create_application(
            name=self.name,
            package_name=package.name,
            install_method=install_method,
            path_to_version_directory=stage_path.full_path,
            debug_mode=self.debug,
            should_authorize_event_sharing=event_sharing.should_authorize_event_sharing(),
            role=self.role,
            warehouse=self.warehouse,
            release_channel=release_channel,
        )
        for warning in warnings:
            self.console.warning(warning)
        return create_app_result

    @span("update_app_object")
    def create_or_upgrade_app(
        self,
        package: ApplicationPackageEntity,
        stage_path: DefaultStagePathParts,
        install_method: SameAccountInstallMethod,
        policy: PolicyBase,
        interactive: bool,
        release_channel: Optional[str] = None,
    ):
        event_sharing = EventSharingHandler(
            telemetry_definition=self.telemetry,
            deploy_root=package.deploy_root,
            install_method=install_method,
            console=self.console,
        )

        # 1. Check for an existing application by the same name
        show_app_row = get_snowflake_facade().get_existing_app_info(
            self.name, self.role
        )

        # 2. If existing application is found, try to upgrade the application object.
        create_or_upgrade_result = None
        if show_app_row:
            create_or_upgrade_result = self._upgrade_app(
                stage_path=stage_path,
                install_method=install_method,
                event_sharing=event_sharing,
                policy=policy,
                interactive=interactive,
                release_channel=release_channel,
            )

        # 3. If no existing application found, or we performed a drop before the upgrade, we proceed to create
        if create_or_upgrade_result is None:
            create_or_upgrade_result = self._create_app(
                stage_path=stage_path,
                install_method=install_method,
                event_sharing=event_sharing,
                package=package,
                release_channel=release_channel,
            )

        print_messages(self.console, create_or_upgrade_result)

        events_definitions = get_snowflake_facade().get_event_definitions(
            self.name, self.role
        )

        events_to_share = event_sharing.events_to_share(events_definitions)
        if events_to_share is not None:
            get_snowflake_facade().share_telemetry_events(
                self.name, events_to_share, self.role
            )

        # hooks always executed after a create or upgrade
        self.execute_post_deploy_hooks()

    def execute_post_deploy_hooks(self):
        execute_post_deploy_hooks(
            console=self.console,
            project_root=self.project_root,
            post_deploy_hooks=self.post_deploy_hooks,
            deployed_object_type="application",
            role_name=self.role,
            warehouse_name=self.warehouse,
            database_name=self.name,
        )

    @contextmanager
    def use_application_warehouse(self):
        if self.warehouse:
            with get_sql_executor().use_warehouse(self.warehouse):
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

    def drop_application_before_upgrade(
        self,
        policy: PolicyBase,
        interactive: bool,
        cascade: bool = False,
    ):
        sql_executor = get_sql_executor()
        with sql_executor.use_role(self.role):
            if cascade:
                try:
                    if application_objects := self.get_objects_owned_by_application():
                        application_objects_str = _application_objects_to_str(
                            application_objects
                        )
                        self.console.message(
                            f"The following objects are owned by application {self.name} and need to be dropped:\n{application_objects_str}"
                        )
                except ProgrammingError as err:
                    if err.errno != APPLICATION_NO_LONGER_AVAILABLE:
                        generic_sql_error_handler(err)
                    self.console.warning(
                        "The application owns other objects but they could not be determined."
                    )
                user_prompt = "Do you want the Snowflake CLI to drop these objects, then drop the existing application object and recreate it?"
            else:
                user_prompt = "Do you want the Snowflake CLI to drop the existing application object and recreate it?"

            if not policy.should_proceed(user_prompt):
                if interactive:
                    self.console.message("Not upgrading the application object.")
                    raise typer.Exit(0)
                else:
                    self.console.message(
                        "Cannot upgrade the application object non-interactively without --force."
                    )
                    raise typer.Exit(1)
            try:
                cascade_msg = " (cascade)" if cascade else ""
                self.console.step(
                    f"Dropping application object {self.name}{cascade_msg}."
                )
                cascade_sql = " cascade" if cascade else ""
                sql_executor.execute_query(f"drop application {self.name}{cascade_sql}")
            except ProgrammingError as err:
                if err.errno == APPLICATION_OWNS_EXTERNAL_OBJECTS and not cascade:
                    # We need to cascade the deletion, let's try again (only if we didn't try with cascade already)
                    return self.drop_application_before_upgrade(
                        policy=policy,
                        interactive=interactive,
                        cascade=True,
                    )
                else:
                    generic_sql_error_handler(err)

    def get_events(
        self,
        package_name: str,
        since: str | datetime | None = None,
        until: str | datetime | None = None,
        record_types: list[str] | None = None,
        scopes: list[str] | None = None,
        consumer_org: str = "",
        consumer_account: str = "",
        consumer_app_hash: str = "",
        first: int = -1,
        last: int = -1,
    ):
        record_types = record_types or []
        scopes = scopes or []

        if first >= 0 and last >= 0:
            raise ValueError("first and last cannot be used together")

        account_event_table = get_snowflake_facade().get_account_event_table()
        if account_event_table is None:
            raise NoEventTableForAccount()

        # resource_attributes uses the unquoted/uppercase app and package name
        app_name = unquote_identifier(self.name)
        package_name = unquote_identifier(package_name)
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
                from {account_event_table}
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
        sql_executor = get_sql_executor()
        try:
            return sql_executor.execute_query(query, cursor_class=DictCursor).fetchall()
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                raise ClickException(
                    dedent(
                        f"""\
                    Event table '{account_event_table}' does not exist or you are not authorized to perform this operation.
                    Please check your EVENT_TABLE parameter to ensure that it is set to a valid event table."""
                    )
                ) from err
            else:
                generic_sql_error_handler(err)

    def stream_events(
        self,
        package_name: str,
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
                package_name=package_name,
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
                    package_name=package_name,
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

    @span("get_snowsight_url_for_app")
    def get_snowsight_url(self) -> str:
        """Returns the URL that can be used to visit this app via Snowsight."""
        name = identifier_for_url(self.name)
        with self.use_application_warehouse():
            sql_executor = get_sql_executor()
            return make_snowsight_url(
                sql_executor._conn, f"/#/apps/application/{name}"  # noqa: SLF001
            )


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


def _application_objects_to_str(
    application_objects: list[ApplicationOwnedObject],
) -> str:
    """
    Returns a list in an "(Object Type) Object Name" format. Database-level and schema-level object names are fully qualified:
    (COMPUTE_POOL) POOL_NAME
    (DATABASE) DB_NAME
    (SCHEMA) DB_NAME.PUBLIC
    ...
    """
    return "\n".join([_application_object_to_str(obj) for obj in application_objects])


def _application_object_to_str(obj: ApplicationOwnedObject) -> str:
    return f"({obj['type']}) {obj['name']}"
