from __future__ import annotations

import json
import os
import re
from datetime import datetime
from functools import cached_property
from pathlib import Path
from textwrap import dedent
from typing import Any, List, Literal, Optional, Set, Union

import typer
from click import BadOptionUsage, ClickException, UsageError
from pydantic import Field, field_validator
from snowflake.cli._plugins.connection.util import UIParameter
from snowflake.cli._plugins.nativeapp.artifacts import (
    VersionInfo,
    build_bundle,
    find_setup_script_file,
    find_version_info_in_manifest_file,
)
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    DEFAULT_CHANNEL,
    DEFAULT_DIRECTIVE,
    EXTERNAL_DISTRIBUTION,
    INTERNAL_DISTRIBUTION,
    MAX_VERSIONS_IN_RELEASE_CHANNEL,
    NAME_COL,
    OWNER_COL,
    PATCH_COL,
    VERSION_COL,
)
from snowflake.cli._plugins.nativeapp.entities.application_package_child_interface import (
    ApplicationPackageChildInterface,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
    CouldNotDropApplicationPackageWithVersions,
    ObjectPropertyNotFoundError,
    SetupScriptFailedValidation,
)
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
    PolicyBase,
)
from snowflake.cli._plugins.nativeapp.sf_facade import get_snowflake_facade
from snowflake.cli._plugins.nativeapp.sf_facade_exceptions import (
    InsufficientPrivilegesError,
)
from snowflake.cli._plugins.nativeapp.sf_sql_facade import ReleaseChannel, Version
from snowflake.cli._plugins.nativeapp.utils import needs_confirmation, sanitize_dir_name
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli._plugins.stage.diff import DiffResult, compute_stage_diff
from snowflake.cli._plugins.stage.manager import (
    DefaultStagePathParts,
    StageManager,
    StagePathParts,
)
from snowflake.cli._plugins.stage.utils import print_diff_to_console
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.cli_global_context import span
from snowflake.cli.api.entities.common import (
    EntityBase,
    attach_spans_to_entity_actions,
)
from snowflake.cli.api.entities.utils import (
    drop_generic_object,
    execute_post_deploy_hooks,
    generic_sql_error_handler,
    get_sql_executor,
    sync_deploy_root_with_stage,
    validation_item_to_str,
)
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBaseWithArtifacts,
    Identifier,
    PostDeployHook,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
    IdentifierField,
    UpdatableModel,
)
from snowflake.cli.api.project.schemas.v1.native_app.package import DistributionOptions
from snowflake.cli.api.project.util import (
    SCHEMA_AND_NAME,
    VALID_IDENTIFIER_REGEX,
    append_test_resource_suffix,
    identifier_to_show_like_pattern,
    same_identifiers,
    sql_match,
    to_identifier,
    unquote_identifier,
)
from snowflake.cli.api.utils.cursor import find_all_rows
from snowflake.connector import DictCursor, ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

ApplicationPackageChildrenTypes = (
    StreamlitEntityModel | FunctionEntityModel | ProcedureEntityModel
)


class ApplicationPackageChildIdentifier(UpdatableModel):
    schema_: Optional[str] = Field(
        title="Child entity schema", alias="schema", default=None
    )


class EnsureUsableByField(UpdatableModel):
    application_roles: Optional[Union[str, Set[str]]] = Field(
        title="One or more application roles to be granted with the required privileges",
        default=None,
    )

    @field_validator("application_roles")
    @classmethod
    def ensure_app_roles_is_a_set(
        cls, application_roles: Optional[Union[str, Set[str]]]
    ) -> Optional[Union[Set[str]]]:
        if isinstance(application_roles, str):
            return set([application_roles])
        return application_roles


class ApplicationPackageChildField(UpdatableModel):
    target: str = Field(title="The key of the entity to include in this package")
    ensure_usable_by: Optional[EnsureUsableByField] = Field(
        title="Automatically grant the required privileges on the child object and its schema",
        default=None,
    )
    identifier: ApplicationPackageChildIdentifier = Field(
        title="Entity identifier", default=None
    )


class ApplicationPackageEntityModel(EntityModelBaseWithArtifacts):
    type: Literal["application package"] = DiscriminatorField()  # noqa: A003
    bundle_root: Optional[str] = Field(
        title="Folder at the root of your project where artifacts necessary to perform the bundle step are stored",
        default="output/bundle/",
    )
    children_artifacts_dir: Optional[str] = Field(
        title="Folder under deploy_root where the child artifacts will be stored",
        default="_children/",
    )
    generated_root: Optional[str] = Field(
        title="Subdirectory of the deploy root where files generated by the Snowflake CLI will be written",
        default="__generated/",
    )
    stage: Optional[str] = IdentifierField(
        title="Identifier of the stage that stores the application artifacts",
        default="app_src.stage",
    )
    scratch_stage: Optional[str] = IdentifierField(
        title="Identifier of the stage that stores temporary scratch data used by the Snowflake CLI",
        default="app_src.stage_snowflake_cli_scratch",
    )
    distribution: Optional[DistributionOptions] = Field(
        title="Distribution of the application package created by the Snowflake CLI",
        default="internal",
    )
    manifest: Optional[str] = Field(
        title="Path to manifest.yml. Unused and deprecated starting with Snowflake CLI 3.2",
        default="",
    )

    stage_subdirectory: Optional[str] = Field(
        title="Subfolder in stage to upload the artifacts to, instead of the root of the application package's stage",
        default="",
    )
    children: Optional[List[ApplicationPackageChildField]] = Field(
        title="Entities that will be bundled and deployed as part of this application package",
        default=[],
    )
    enable_release_channels: Optional[bool] = Field(
        title="Enable release channels for this application package",
        default=None,
    )

    @field_validator("children")
    @classmethod
    def verify_children_behind_flag(
        cls, input_value: Optional[List[ApplicationPackageChildField]]
    ) -> Optional[List[ApplicationPackageChildField]]:
        if input_value and not FeatureFlag.ENABLE_NATIVE_APP_CHILDREN.is_enabled():
            raise AttributeError("Application package children are not supported yet")
        return input_value

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

    @field_validator("stage")
    @classmethod
    def validate_source_stage(cls, input_value: str):
        if not re.match(SCHEMA_AND_NAME, input_value):
            raise ValueError(
                "Incorrect value for stage of native_app. Expected format for this field is {schema_name}.{stage_name} "
            )
        return input_value


@attach_spans_to_entity_actions(entity_name="app_pkg")
class ApplicationPackageEntity(EntityBase[ApplicationPackageEntityModel]):
    """
    A Native App application package.
    """

    @property
    def project_root(self) -> Path:
        return self._workspace_ctx.project_root

    @property
    def deploy_root(self) -> Path:
        return (
            self.project_root
            / self._entity_model.deploy_root
            / self._entity_model.stage_subdirectory
        )

    @property
    def children_artifacts_deploy_root(self) -> Path:
        return self.deploy_root / self._entity_model.children_artifacts_dir

    @property
    def bundle_root(self) -> Path:
        return self.project_root / self._entity_model.bundle_root

    @property
    def generated_root(self) -> Path:
        return self.deploy_root / self._entity_model.generated_root

    @property
    def name(self) -> str:
        return self._entity_model.fqn.name

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
    def scratch_stage_path(self) -> DefaultStagePathParts:
        return DefaultStagePathParts.from_fqn(
            f"{self.name}.{self._entity_model.scratch_stage}"
        )

    @cached_property
    def stage_path(self) -> DefaultStagePathParts:
        stage_fqn = f"{self.name}.{self._entity_model.stage}"
        subdir = self._entity_model.stage_subdirectory
        return DefaultStagePathParts.from_fqn(stage_fqn, subdir)

    @property
    def post_deploy_hooks(self) -> list[PostDeployHook] | None:
        model = self._entity_model
        return model.meta and model.meta.post_deploy

    def action_bundle(self, action_ctx: ActionContext, *args, **kwargs):
        return self._bundle(action_ctx)

    def action_diff(
        self, action_ctx: ActionContext, print_to_console: bool, *args, **kwargs
    ):
        """
        Compute the diff between the local artifacts and the remote ones on the stage.
        """
        bundle_map = self._bundle()
        diff = compute_stage_diff(
            local_root=self.deploy_root,
            stage_path=self.stage_path,
        )

        if print_to_console:
            print_diff_to_console(diff, bundle_map)

        return diff

    def action_deploy(
        self,
        action_ctx: ActionContext,
        prune: bool,
        recursive: bool,
        paths: List[Path],
        validate: bool,
        interactive: bool,
        force: bool,
        *args,
        **kwargs,
    ):
        return self._deploy(
            action_ctx=action_ctx,
            bundle_map=None,
            prune=prune,
            recursive=recursive,
            paths=paths,
            print_diff=True,
            validate=validate,
            stage_path=self.stage_path,
            interactive=interactive,
            force=force,
        )

    def action_drop(self, action_ctx: ActionContext, force_drop: bool, *args, **kwargs):
        console = self._workspace_ctx.console
        sql_executor = get_sql_executor()
        needs_confirm = True

        # 1. If existing application package is not found, exit gracefully
        show_obj_row = self.get_existing_app_pkg_info()
        if show_obj_row is None:
            console.warning(
                f"Role {self.role} does not own any application package with the name {self.name}, or the application package does not exist."
            )
            return

        # 2. Check for versions in the application package
        versions_in_pkg = get_snowflake_facade().show_versions(self.name, self.role)
        if len(versions_in_pkg) > 0:
            # allow dropping a package with versions when --force is set
            if not force_drop:
                raise CouldNotDropApplicationPackageWithVersions(
                    "Drop versions first, or use --force to override."
                )

        # 3. Check distribution of the existing application package
        actual_distribution = self.get_app_pkg_distribution_in_snowflake()
        if not self.verify_project_distribution():
            console.warning(
                f"Dropping application package {self.name} with distribution '{actual_distribution}'."
            )

        # 4. If distribution is internal, check if created by the Snowflake CLI
        row_comment = show_obj_row[COMMENT_COL]
        if actual_distribution == INTERNAL_DISTRIBUTION:
            if row_comment in ALLOWED_SPECIAL_COMMENTS:
                needs_confirm = False
            else:
                if needs_confirmation(needs_confirm, force_drop):
                    console.warning(
                        f"Application package {self.name} was not created by Snowflake CLI."
                    )
        else:
            if needs_confirmation(needs_confirm, force_drop):
                console.warning(
                    f"Application package {self.name} in your Snowflake account has distribution property '{EXTERNAL_DISTRIBUTION}' and could be associated with one or more of your listings on Snowflake Marketplace."
                )

        if needs_confirmation(needs_confirm, force_drop):
            should_drop_object = typer.confirm(
                dedent(
                    f"""\
                        Application package details:
                        Name: {self.name}
                        Created on: {show_obj_row["created_on"]}
                        Distribution: {actual_distribution}
                        Owner: {show_obj_row[OWNER_COL]}
                        Comment: {show_obj_row[COMMENT_COL]}
                        Are you sure you want to drop it?
                    """
                )
            )
            if not should_drop_object:
                console.message(f"Did not drop application package {self.name}.")
                return  # The user desires to keep the application package, therefore exit gracefully

        # All validations have passed, drop object
        drop_generic_object(
            console=console,
            object_type="application package",
            object_name=(self.name),
            role=(self.role),
        )

    def action_validate(
        self,
        action_ctx: ActionContext,
        interactive: bool,
        force: bool,
        use_scratch_stage: bool = True,
        *args,
        **kwargs,
    ):
        self.validate_setup_script(
            action_ctx=action_ctx,
            use_scratch_stage=use_scratch_stage,
            interactive=interactive,
            force=force,
        )
        self._workspace_ctx.console.message("Setup script is valid")

    def action_version_list(
        self, action_ctx: ActionContext, *args, **kwargs
    ) -> SnowflakeCursor:
        """
        Get all existing versions, if defined, for an application package.
        It executes a 'show versions in application package' query and returns all the results.
        """
        return get_snowflake_facade().show_versions(self.name, self.role)

    def action_version_create(
        self,
        action_ctx: ActionContext,
        version: Optional[str],
        patch: Optional[int],
        label: Optional[str],
        skip_git_check: bool,
        interactive: bool,
        force: bool,
        from_stage: Optional[bool],
        *args,
        **kwargs,
    ) -> VersionInfo:
        """
        Create a version and/or patch for a new or existing application package.
        Always performs a deploy action before creating version or patch.
        If version is not provided in CLI, bundle is performed to read version from manifest.yml. Raises a ClickException if version is not found.
        """
        console = self._workspace_ctx.console

        if force:
            policy = AllowAlwaysPolicy()
        elif interactive:
            policy = AskAlwaysPolicy()
        else:
            policy = DenyAlwaysPolicy()

        bundle_map = self._bundle(action_ctx)
        resolved_version, resolved_patch, resolved_label = self.resolve_version_info(
            version=version,
            patch=patch,
            label=label,
            bundle_map=bundle_map,
            policy=policy,
            interactive=interactive,
        )

        if not skip_git_check:
            self.check_index_changes_in_git_repo(policy=policy, interactive=interactive)

        # if user is asking to create the version from the current stage,
        # then do not re-deploy the artifacts or touch the stage
        if from_stage:
            # verify package exists:
            if not self.get_existing_app_pkg_info():
                raise ClickException(
                    "Cannot create version from stage because the application package does not exist yet. "
                    "Try removing --from-stage flag or executing `snow app deploy` to deploy the application package first."
                )
        else:
            self._deploy(
                action_ctx=action_ctx,
                bundle_map=bundle_map,
                prune=True,
                recursive=True,
                paths=[],
                print_diff=True,
                validate=True,
                stage_path=self.stage_path,
                interactive=interactive,
                force=force,
            )

        # Warn if the version exists in a release directive(s)
        try:
            existing_release_directives = (
                self.get_existing_release_directive_info_for_version(resolved_version)
            )
        except InsufficientPrivilegesError:
            warning = (
                "Could not check for existing release directives due to insufficient privileges. "
                "The MANAGE RELEASES privilege is required to check for existing release directives."
            )
        else:
            if existing_release_directives:
                release_directive_names = ", ".join(
                    row["name"] for row in existing_release_directives
                )
                warning = f"Version {resolved_version} already defined in application package {self.name} and in release directive(s): {release_directive_names}."
            else:
                warning = ""

        if warning:
            console.warning(warning)
            user_prompt = (
                f"Are you sure you want to create a new patch for version {resolved_version} in application "
                f"package {self.name}? Once added, this operation cannot be undone."
            )
            if not policy.should_proceed(user_prompt):
                if interactive:
                    console.message("Not creating a new patch.")
                    raise typer.Exit(0)
                else:
                    console.message(
                        "Cannot create a new patch non-interactively without --force."
                    )
                    raise typer.Exit(1)

        # Define a new version in the application package
        if not self.get_existing_version_info(resolved_version):
            self.add_new_version(version=resolved_version, label=resolved_label)
            # A new version created automatically has patch 0, we do not need to further increment the patch.
            return VersionInfo(resolved_version, 0, resolved_label)

        # Add a new patch to an existing (old) version
        patch = self.add_new_patch_to_version(
            version=resolved_version, patch=resolved_patch, label=resolved_label
        )
        return VersionInfo(resolved_version, patch, resolved_label)

    def action_version_drop(
        self,
        action_ctx: ActionContext,
        version: Optional[str],
        interactive: bool,
        force: bool,
        *args,
        **kwargs,
    ):
        """
        Drops a version defined in an application package. If --force is provided, then no user prompts will be executed.
        """
        console = self._workspace_ctx.console

        if force:
            interactive = False
            policy = AllowAlwaysPolicy()
        else:
            policy = AskAlwaysPolicy() if interactive else DenyAlwaysPolicy()

        # 1. Check for existing an existing application package
        show_obj_row = self.get_existing_app_pkg_info()
        if not show_obj_row:
            raise ApplicationPackageDoesNotExistError(self.name)

        # 2. Check distribution of the existing application package
        actual_distribution = self.get_app_pkg_distribution_in_snowflake()
        if not self.verify_project_distribution(
            expected_distribution=actual_distribution
        ):
            console.warning(
                f"Continuing to execute version drop on application package "
                f"{self.name} with distribution '{actual_distribution}'."
            )

        # 3. If the user did not pass in a version string, determine from manifest.yml
        if not version:
            console.message(
                dedent(
                    f"""\
                        Version was not provided through the Snowflake CLI. Checking version in the manifest.yml instead.
                        This step will bundle your app artifacts to determine the location of the manifest.yml file.
                    """
                )
            )
            self._bundle(action_ctx)
            version_info = find_version_info_in_manifest_file(self.deploy_root)
            version = version_info.version_name
            if not version:
                raise ClickException(
                    "Manifest.yml file does not contain a value for the version field."
                )

        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)

        console.step(
            f"About to drop version {version} in application package {self.name}."
        )

        # If user did not provide --force, ask for confirmation
        user_prompt = (
            f"Are you sure you want to drop version {version} "
            f"in application package {self.name}? "
            f"Once dropped, this operation cannot be undone."
        )
        if not policy.should_proceed(user_prompt):
            if interactive:
                console.message("Not dropping version.")
                raise typer.Exit(0)
            else:
                console.message(
                    "Cannot drop version non-interactively without --force."
                )
                raise typer.Exit(1)

        # Drop the version
        get_snowflake_facade().drop_version_from_package(
            package_name=self.name, version=version, role=self.role
        )

        console.message(
            f"Version {version} in application package {self.name} dropped successfully."
        )

    def _validate_target_accounts(self, accounts: list[str]) -> None:
        """
        Validates the target accounts provided by the user.
        """
        for account in accounts:
            if not re.fullmatch(
                f"{VALID_IDENTIFIER_REGEX}\\.{VALID_IDENTIFIER_REGEX}", account
            ):
                raise ClickException(
                    f"Target account {account} is not in a valid format. Make sure you provide the target account in the format 'org.account'."
                )

    def get_sanitized_release_channel(
        self,
        release_channel: Optional[str],
        available_release_channels: Optional[list[ReleaseChannel]] = None,
    ) -> Optional[str]:
        """
        Sanitize the release channel name provided by the user and validate it against the available release channels.

        A return value of None indicates that release channels should not be used. Returns None if:
        - Release channel is not provided
        - Release channels are not enabled in the application package and the user provided the default release channel
        """
        if not release_channel:
            return None

        if available_release_channels is None:
            available_release_channels = get_snowflake_facade().show_release_channels(
                self.name, self.role
            )

        if not available_release_channels and same_identifiers(
            release_channel, DEFAULT_CHANNEL
        ):
            return None

        self.validate_release_channel(release_channel, available_release_channels)
        return release_channel

    def validate_release_channel(
        self,
        release_channel: str,
        available_release_channels: Optional[list[ReleaseChannel]] = None,
    ) -> None:
        """
        Validates the release channel provided by the user and make sure it is a valid release channel for the application package.
        """

        if available_release_channels is None:
            available_release_channels = get_snowflake_facade().show_release_channels(
                self.name, self.role
            )
        if not available_release_channels:
            raise UsageError(
                f"Release channels are not enabled for application package {self.name}."
            )
        for channel in available_release_channels:
            if unquote_identifier(release_channel) == channel["name"]:
                return

        raise UsageError(
            f"Release channel {release_channel} is not available in application package {self.name}. "
            f"Available release channels are: ({', '.join(channel['name'] for channel in available_release_channels)})."
        )

    def action_release_directive_list(
        self,
        action_ctx: ActionContext,
        release_channel: Optional[str],
        like: str,
        *args,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Get all existing release directives for an application package.
        Limit the results to a specific release channel, if provided.

        If `like` is provided, only release directives matching the SQL LIKE pattern are listed.
        """
        release_channel = self.get_sanitized_release_channel(release_channel)

        release_directives = get_snowflake_facade().show_release_directives(
            package_name=self.name,
            role=self.role,
            release_channel=release_channel,
        )

        return [
            directive
            for directive in release_directives
            if sql_match(pattern=like, value=directive.get("name", ""))
        ]

    def action_release_directive_set(
        self,
        action_ctx: ActionContext,
        version: str,
        patch: int,
        release_directive: str,
        release_channel: str,
        target_accounts: Optional[list[str]],
        *args,
        **kwargs,
    ):
        """
        Sets a release directive to the specified version and patch using the specified release channel.
        Target accounts can only be specified for non-default release directives.

        For non-default release directives, update the existing release directive if target accounts are not provided.
        """
        if target_accounts:
            self._validate_target_accounts(target_accounts)

        if target_accounts and same_identifiers(release_directive, DEFAULT_DIRECTIVE):
            raise BadOptionUsage(
                "target_accounts",
                "Target accounts can only be specified for non-default named release directives.",
            )

        sanitized_release_channel = self.get_sanitized_release_channel(release_channel)

        get_snowflake_facade().set_release_directive(
            package_name=self.name,
            release_directive=release_directive,
            release_channel=sanitized_release_channel,
            target_accounts=target_accounts,
            version=version,
            patch=patch,
            role=self.role,
        )

    def action_release_directive_unset(
        self,
        action_ctx: ActionContext,
        release_directive: str,
        release_channel: str,
    ):
        """
        Unsets a release directive from the specified release channel.
        """
        if same_identifiers(release_directive, DEFAULT_DIRECTIVE):
            raise ClickException(
                "Cannot unset default release directive. Please specify a non-default release directive."
            )

        get_snowflake_facade().unset_release_directive(
            package_name=self.name,
            release_directive=release_directive,
            release_channel=self.get_sanitized_release_channel(release_channel),
            role=self.role,
        )

    def action_release_directive_add_accounts(
        self,
        action_ctx: ActionContext,
        release_directive: str,
        release_channel: str,
        target_accounts: list[str],
        *args,
        **kwargs,
    ):
        """
        Adds target accounts to a release directive.
        """

        if not target_accounts:
            raise ClickException("No target accounts provided.")

        self._validate_target_accounts(target_accounts)

        get_snowflake_facade().add_accounts_to_release_directive(
            package_name=self.name,
            release_directive=release_directive,
            release_channel=self.get_sanitized_release_channel(release_channel),
            target_accounts=target_accounts,
            role=self.role,
        )

    def action_release_directive_remove_accounts(
        self,
        action_ctx: ActionContext,
        release_directive: str,
        release_channel: str,
        target_accounts: list[str],
        *args,
        **kwargs,
    ):
        """
        Removes target accounts from a release directive.
        """

        if not target_accounts:
            raise ClickException("No target accounts provided.")

        self._validate_target_accounts(target_accounts)

        get_snowflake_facade().remove_accounts_from_release_directive(
            package_name=self.name,
            release_directive=release_directive,
            release_channel=self.get_sanitized_release_channel(release_channel),
            target_accounts=target_accounts,
            role=self.role,
        )

    def _print_channel_to_console(self, channel: ReleaseChannel) -> None:
        """
        Prints the release channel details to the console.
        """
        console = self._workspace_ctx.console

        console.message(f"""[bold]{channel["name"]}[/bold]""")
        accounts_list: Optional[list[str]] = channel["targets"].get("accounts")
        target_accounts = (
            f"({', '.join(accounts_list)})"
            if accounts_list is not None
            else "ALL ACCOUNTS"
        )

        formatted_created_on = (
            channel["created_on"].astimezone().strftime("%Y-%m-%d %H:%M:%S.%f %Z")
            if channel["created_on"]
            else ""
        )

        formatted_updated_on = (
            channel["updated_on"].astimezone().strftime("%Y-%m-%d %H:%M:%S.%f %Z")
            if channel["updated_on"]
            else ""
        )
        with console.indented():
            console.message(f"Description: {channel['description']}")
            console.message(f"Versions: ({', '.join(channel['versions'])})")
            console.message(f"Created on: {formatted_created_on}")
            console.message(f"Updated on: {formatted_updated_on}")
            console.message(f"Target accounts: {target_accounts}")

    def action_release_channel_list(
        self,
        action_ctx: ActionContext,
        release_channel: Optional[str],
        *args,
        **kwargs,
    ) -> list[ReleaseChannel]:
        """
        Get all existing release channels for an application package.
        If `release_channel` is provided, only the specified release channel is listed.
        """
        console = self._workspace_ctx.console
        available_channels = get_snowflake_facade().show_release_channels(
            self.name, self.role
        )

        filtered_channels = [
            channel
            for channel in available_channels
            if release_channel is None
            or unquote_identifier(release_channel) == channel["name"]
        ]

        if not filtered_channels:
            console.message("No release channels found.")
        else:
            for channel in filtered_channels:
                self._print_channel_to_console(channel)

        return filtered_channels

    def _bundle(self, action_ctx: ActionContext = None):
        model = self._entity_model
        bundle_map = build_bundle(self.project_root, self.deploy_root, model.artifacts)
        bundle_context = BundleContext(
            package_name=self.name,
            artifacts=model.artifacts,
            project_root=self.project_root,
            bundle_root=self.bundle_root,
            deploy_root=self.deploy_root,
            generated_root=self.generated_root,
        )
        compiler = NativeAppCompiler(bundle_context)
        compiler.compile_artifacts()

        if self._entity_model.children:
            # Bundle children and append their SQL to setup script
            # TODO Consider re-writing the logic below as a processor
            children_sql = self._bundle_children(action_ctx=action_ctx)
            setup_file_path = find_setup_script_file(deploy_root=self.deploy_root)
            with open(setup_file_path, "r", encoding="utf-8") as file:
                existing_setup_script = file.read()
            if setup_file_path.is_symlink():
                setup_file_path.unlink()
            with open(setup_file_path, "w", encoding="utf-8") as file:
                file.write(existing_setup_script)
                file.write("\n-- AUTO GENERATED CHILDREN SECTION\n")
                file.write("\n".join(children_sql))
                file.write("\n")

        return bundle_map

    def action_release_channel_add_accounts(
        self,
        action_ctx: ActionContext,
        release_channel: str,
        target_accounts: list[str],
        *args,
        **kwargs,
    ):
        """
        Adds target accounts to a release channel.
        """

        if not target_accounts:
            raise ClickException("No target accounts provided.")

        self.validate_release_channel(release_channel)
        self._validate_target_accounts(target_accounts)

        get_snowflake_facade().add_accounts_to_release_channel(
            package_name=self.name,
            release_channel=release_channel,
            target_accounts=target_accounts,
            role=self.role,
        )

    def action_release_channel_remove_accounts(
        self,
        action_ctx: ActionContext,
        release_channel: str,
        target_accounts: list[str],
        *args,
        **kwargs,
    ):
        """
        Removes target accounts from a release channel.
        """

        if not target_accounts:
            raise ClickException("No target accounts provided.")

        self.validate_release_channel(release_channel)
        self._validate_target_accounts(target_accounts)

        get_snowflake_facade().remove_accounts_from_release_channel(
            package_name=self.name,
            release_channel=release_channel,
            target_accounts=target_accounts,
            role=self.role,
        )

    def action_release_channel_set_accounts(
        self,
        action_ctx: ActionContext,
        release_channel: str,
        target_accounts: list[str],
        *args,
        **kwargs,
    ):
        """
        Sets target accounts for a release channel.
        """

        if not target_accounts:
            raise ClickException("No target accounts provided.")

        self.validate_release_channel(release_channel)
        self._validate_target_accounts(target_accounts)

        get_snowflake_facade().set_accounts_for_release_channel(
            package_name=self.name,
            release_channel=release_channel,
            target_accounts=target_accounts,
            role=self.role,
        )

    def action_release_channel_add_version(
        self,
        action_ctx: ActionContext,
        release_channel: str,
        version: str,
        *args,
        **kwargs,
    ):
        """
        Adds a version to a release channel.
        """

        self.validate_release_channel(release_channel)
        get_snowflake_facade().add_version_to_release_channel(
            package_name=self.name,
            release_channel=release_channel,
            version=version,
            role=self.role,
        )

    def action_release_channel_remove_version(
        self,
        action_ctx: ActionContext,
        release_channel: str,
        version: str,
        *args,
        **kwargs,
    ):
        """
        Removes a version from a release channel.
        """

        self.validate_release_channel(release_channel)
        get_snowflake_facade().remove_version_from_release_channel(
            package_name=self.name,
            release_channel=release_channel,
            version=version,
            role=self.role,
        )

    def _find_version_with_no_recent_update(
        self, versions_info: list[Version], free_versions: set[str]
    ) -> Optional[str]:
        """
        Finds the version with the oldest created_on date from the free versions.
        """

        if not free_versions:
            return None

        # map of versionId to last Updated Date. Last Updated Date is based on patch creation date.
        last_updated_map: dict[str, datetime] = {}
        for version_info in versions_info:
            last_updated_value = last_updated_map.get(version_info["version"], None)
            if (
                not last_updated_value
                or version_info["created_on"] > last_updated_value
            ):
                last_updated_map[version_info["version"]] = version_info["created_on"]

        oldest_version = None
        oldest_version_last_updated_on = None

        for version in free_versions:
            last_updated = last_updated_map[version]
            if not oldest_version or last_updated < oldest_version_last_updated_on:
                oldest_version = version
                oldest_version_last_updated_on = last_updated

        return oldest_version

    def action_publish(
        self,
        action_ctx: ActionContext,
        version: Optional[str],
        patch: Optional[int],
        release_channel: Optional[str],
        release_directive: str,
        interactive: bool,
        force: bool,
        *args,
        create_version: bool = False,
        from_stage: bool = False,
        label: Optional[str] = None,
        **kwargs,
    ) -> VersionInfo:
        """
        Publishes a version and a patch to a release directive of a release channel.

        The version is first added to the release channel,
        and then the release directive is set to the version and patch provided.

        If the number of versions in a release channel exceeds the maximum allowable versions,
        the user is prompted to remove an existing version to make space for the new version.
        """
        if force:
            policy = AllowAlwaysPolicy()
        elif interactive:
            policy = AskAlwaysPolicy()
        else:
            policy = DenyAlwaysPolicy()

        if from_stage and not create_version:
            raise UsageError(
                "--from-stage flag can only be used with --create-version flag."
            )
        if label is not None and not create_version:
            raise UsageError("--label can only be used with --create-version flag.")

        console = self._workspace_ctx.console
        if create_version:
            result = self.action_version_create(
                action_ctx=action_ctx,
                version=version,
                patch=patch,
                label=label,
                skip_git_check=True,
                interactive=interactive,
                force=force,
                from_stage=from_stage,
            )
            version = result.version_name
            patch = result.patch_number

        if version is None:
            raise UsageError(
                "Please provide a version using --version or use --create-version flag to create a version based on the manifest file."
            )
        if patch is None:
            raise UsageError(
                "Please provide a patch number using --patch or use --create-version flag to auto create a patch."
            )

        versions_info = get_snowflake_facade().show_versions(self.name, self.role)

        available_patches = [
            version_info["patch"]
            for version_info in versions_info
            if version_info["version"] == unquote_identifier(version)
        ]

        if not available_patches:
            raise ClickException(
                f"Version {version} does not exist in application package {self.name}. Use --create-version flag to create a new version."
            )

        if patch not in available_patches:
            raise ClickException(
                f"Patch {patch} does not exist for version {version} in application package {self.name}. Use --create-version flag to add a new patch."
            )

        available_release_channels = get_snowflake_facade().show_release_channels(
            self.name, self.role
        )

        release_channel = self.get_sanitized_release_channel(
            release_channel, available_release_channels
        )

        if release_channel:
            release_channel_info = {}
            for channel_info in available_release_channels:
                if channel_info["name"] == unquote_identifier(release_channel):
                    release_channel_info = channel_info
                    break

            versions_in_channel = release_channel_info["versions"]
            if unquote_identifier(version) not in release_channel_info["versions"]:
                if len(versions_in_channel) >= MAX_VERSIONS_IN_RELEASE_CHANNEL:
                    # If we hit the maximum allowable versions in a release channel, we need to remove one version to make space for the new version
                    all_release_directives = (
                        get_snowflake_facade().show_release_directives(
                            package_name=self.name,
                            role=self.role,
                            release_channel=release_channel,
                        )
                    )

                    # check which versions are attached to any release directive
                    targeted_versions = {d["version"] for d in all_release_directives}

                    free_versions = {
                        v for v in versions_in_channel if v not in targeted_versions
                    }

                    if not free_versions:
                        raise ClickException(
                            f"Maximum number of versions in release channel {release_channel} reached. Cannot add more versions."
                        )

                    version_to_remove = self._find_version_with_no_recent_update(
                        versions_info, free_versions
                    )
                    user_prompt = f"Maximum number of versions in release channel reached. Would you like to remove version {version_to_remove} to make space for version {version}?"
                    if not policy.should_proceed(user_prompt):
                        raise ClickException(
                            "Cannot proceed with publishing the new version. Please remove an existing version from the release channel to make space for the new version, or use --force to automatically clean up unused versions."
                        )

                    console.warning(
                        f"Maximum number of versions in release channel reached. Removing version {version_to_remove} from release_channel {release_channel} to make space for version {version}."
                    )
                    get_snowflake_facade().remove_version_from_release_channel(
                        package_name=self.name,
                        release_channel=release_channel,
                        version=version_to_remove,
                        role=self.role,
                    )

                get_snowflake_facade().add_version_to_release_channel(
                    package_name=self.name,
                    release_channel=release_channel,
                    version=version,
                    role=self.role,
                )

        get_snowflake_facade().set_release_directive(
            package_name=self.name,
            release_directive=release_directive,
            release_channel=release_channel,
            target_accounts=None,
            version=version,
            patch=patch,
            role=self.role,
        )
        return VersionInfo(version, patch, None)

    def _bundle_children(self, action_ctx: ActionContext) -> List[str]:
        # Create _children directory
        children_artifacts_dir = self.children_artifacts_deploy_root
        os.makedirs(children_artifacts_dir)
        children_sql = []
        for child in self._entity_model.children:
            # Create child sub directory
            child_artifacts_dir = children_artifacts_dir / sanitize_dir_name(
                child.target
            )
            try:
                os.makedirs(child_artifacts_dir)
            except FileExistsError:
                raise ClickException(
                    f"Could not create sub-directory at {child_artifacts_dir}. Make sure child entity names do not collide with each other."
                )
            child_entity: ApplicationPackageChildInterface = action_ctx.get_entity(
                child.target
            )
            child_entity.bundle(child_artifacts_dir)
            app_role = (
                to_identifier(
                    child.ensure_usable_by.application_roles.pop()  # TODO Support more than one application role
                )
                if child.ensure_usable_by and child.ensure_usable_by.application_roles
                else None
            )
            child_schema = (
                to_identifier(child.identifier.schema_)
                if child.identifier and child.identifier.schema_
                else None
            )
            children_sql.append(
                child_entity.get_deploy_sql(
                    artifacts_dir=child_artifacts_dir.relative_to(self.deploy_root),
                    schema=child_schema,
                    # TODO Allow users to override the hard-coded value for specific children
                    replace=True,
                )
            )
            if app_role:
                children_sql.append(
                    f"CREATE APPLICATION ROLE IF NOT EXISTS {app_role};"
                )
                if child_schema:
                    children_sql.append(
                        f"GRANT USAGE ON SCHEMA {child_schema} TO APPLICATION ROLE {app_role};"
                    )
                children_sql.append(
                    child_entity.get_usage_grant_sql(
                        app_role=app_role, schema=child_schema
                    )
                )
        return children_sql

    def _deploy(
        self,
        action_ctx: ActionContext,
        bundle_map: BundleMap | None,
        prune: bool,
        recursive: bool,
        paths: list[Path],
        print_diff: bool,
        validate: bool,
        stage_path: StagePathParts,
        interactive: bool,
        force: bool,
        run_post_deploy_hooks: bool = True,
    ) -> DiffResult:
        model = self._entity_model
        workspace_ctx = self._workspace_ctx
        if force:
            policy = AllowAlwaysPolicy()
        elif interactive:
            policy = AskAlwaysPolicy()
        else:
            policy = DenyAlwaysPolicy()

        console = workspace_ctx.console
        stage_path = stage_path or self.stage_path

        # 1. Create a bundle if one wasn't passed in
        bundle_map = bundle_map or self._bundle(action_ctx)

        # 2. Create an empty application package, if none exists
        try:
            self.create_app_package()
        except ApplicationPackageAlreadyExistsError as e:
            console.warning(e.message)
            if not policy.should_proceed("Proceed with using this package?"):
                raise typer.Abort() from e

        with get_sql_executor().use_role(self.role):
            # 3. Upload files from deploy root local folder to the above stage
            diff = sync_deploy_root_with_stage(
                console=console,
                deploy_root=self.deploy_root,
                package_name=self.name,
                bundle_map=bundle_map,
                role=self.role,
                prune=prune,
                recursive=recursive,
                stage_path=stage_path,
                local_paths_to_sync=paths,
                print_diff=print_diff,
            )

        if run_post_deploy_hooks:
            self.execute_post_deploy_hooks()

        if validate:
            self.validate_setup_script(
                action_ctx=action_ctx,
                use_scratch_stage=False,
                interactive=interactive,
                force=force,
            )

        return diff

    def get_existing_version_info(self, version: str) -> Optional[dict]:
        """
        Get the latest patch on an existing version by name in the application package.
        Executes 'show versions like ... in application package' query and returns
        the latest patch in the version as a single row, if one exists. Otherwise,
        returns None.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(self.role):
            try:
                query = f"show versions like {identifier_to_show_like_pattern(version)} in application package {self.name}"
                cursor = sql_executor.execute_query(query, cursor_class=DictCursor)

                if cursor.rowcount is None:
                    raise SnowflakeSQLExecutionError(query)

                matching_rows = find_all_rows(
                    cursor, lambda row: row[VERSION_COL] == unquote_identifier(version)
                )

                if not matching_rows:
                    return None

                return max(matching_rows, key=lambda row: row[PATCH_COL])

            except ProgrammingError as err:
                if err.msg.__contains__("does not exist or not authorized"):
                    raise ApplicationPackageDoesNotExistError(self.name)
                else:
                    generic_sql_error_handler(err=err)
                    return None

    def get_existing_release_directive_info_for_version(
        self, version: str
    ) -> List[dict]:
        """
        Get all existing release directives, if present, set on the version defined in an application package.
        It executes a 'show release directives in application package' query and returns the filtered results, if they exist.
        """
        release_directives = get_snowflake_facade().show_release_directives(
            package_name=self.name, role=self.role
        )
        return [
            directive
            for directive in release_directives
            if directive[VERSION_COL] == unquote_identifier(version)
        ]

    def add_new_version(self, version: str, label: str | None = None) -> None:
        """
        Add a new version with an optional label in application package.
        """
        console = self._workspace_ctx.console
        with_label_prompt = f" labeled {label}" if label else ""

        console.step(
            f"Defining a new version {version}{with_label_prompt} in application package {self.name}"
        )
        get_snowflake_facade().create_version_in_package(
            role=self.role,
            package_name=self.name,
            path_to_version_directory=self.stage_path.full_path,
            version=version,
            label=label,
        )
        console.message(
            f"Version {version}{with_label_prompt} created for application package {self.name}."
        )

    def add_new_patch_to_version(
        self, version: str, patch: int | None = None, label: str | None = None
    ) -> int:
        """
        Add a new patch, optionally a custom one, to an existing version in an application package.
        Returns the patch number of the newly created patch.
        """
        console = self._workspace_ctx.console

        with_label_prompt = f" labeled {label}" if label else ""

        console.step(
            f"Adding new patch to version {version}{with_label_prompt} defined in application package {self.name}"
        )
        new_patch = get_snowflake_facade().add_patch_to_package_version(
            role=self.role,
            package_name=self.name,
            path_to_version_directory=self.stage_path.full_path,
            version=version,
            patch=patch,
            label=label,
        )
        console.message(
            f"Patch {new_patch}{with_label_prompt} created for version {version} defined in application package {self.name}."
        )
        return new_patch

    def check_index_changes_in_git_repo(
        self, policy: PolicyBase, interactive: bool
    ) -> None:
        """
        Checks if the project root, i.e. the native apps project is a git repository. If it is a git repository,
        it also checks if there any local changes to the directory that may not be on the application package stage.
        """

        from git import Repo
        from git.exc import InvalidGitRepositoryError

        console = self._workspace_ctx.console

        try:
            repo = Repo(self.project_root, search_parent_directories=True)
            assert repo.git_dir is not None

            # Check if the repo has any changes, including untracked files
            if repo.is_dirty(untracked_files=True):
                console.warning(
                    "Changes detected in the git repository. "
                    "(Rerun your command with --skip-git-check flag to ignore this check)"
                )
                repo.git.execute(["git", "status"])

                user_prompt = (
                    "You have local changes in this repository that are not part of a previous commit. "
                    "Do you still want to continue?"
                )
                if not policy.should_proceed(user_prompt):
                    if interactive:
                        console.message("Not creating a new version.")
                        raise typer.Exit(0)
                    else:
                        console.message(
                            "Cannot create a new version non-interactively without --force."
                        )
                        raise typer.Exit(1)

        except InvalidGitRepositoryError:
            pass  # not a git repository, which is acceptable

    def get_existing_app_pkg_info(self) -> Optional[dict]:
        """
        Check for an existing application package by the same name as in project definition, in account.
        It executes a 'show application packages like' query and returns the result as single row, if one exists.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(self.role):
            return sql_executor.show_specific_object(
                "application packages", self.name, name_col=NAME_COL
            )

    def get_app_pkg_distribution_in_snowflake(self) -> str:
        """
        Returns the 'distribution' attribute of a 'describe application package' SQL query, in lowercase.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(self.role):
            try:
                desc_cursor = sql_executor.execute_query(
                    f"describe application package {self.name}"
                )
            except ProgrammingError as err:
                generic_sql_error_handler(err)

            if desc_cursor.rowcount is None or desc_cursor.rowcount == 0:
                raise SnowflakeSQLExecutionError()
            else:
                for row in desc_cursor:
                    if row[0].lower() == "distribution":
                        return row[1].lower()
        raise ObjectPropertyNotFoundError(
            property_name="distribution",
            object_type="application package",
            object_name=self.name,
        )

    def verify_project_distribution(
        self,
        expected_distribution: Optional[str] = None,
    ) -> bool:
        """
        Returns true if the 'distribution' attribute of an existing application package in snowflake
        is the same as the attribute specified in project definition file.
        """
        model = self._entity_model
        workspace_ctx = self._workspace_ctx

        actual_distribution = (
            expected_distribution
            if expected_distribution
            else self.get_app_pkg_distribution_in_snowflake()
        )
        project_def_distribution = model.distribution.lower()
        if actual_distribution != project_def_distribution:
            workspace_ctx.console.warning(
                dedent(
                    f"""\
                    Application package {self.name} in your Snowflake account has distribution property {actual_distribution},
                    which does not match the value specified in project definition file: {project_def_distribution}.
                    """
                )
            )
            return False
        return True

    def _get_enable_release_channels_flag(self) -> Optional[bool]:
        """
        Returns the requested value of enable_release_channels flag for the application package.
        It retrieves the value from snowflake.yml (and from the configuration file),
        and checks that the feature is enabled in the account.
        If return value is None, it means do not explicitly set the flag.
        """
        value_from_snowflake_yml = self.model.enable_release_channels
        feature_flag_from_config = FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value()
        if value_from_snowflake_yml is not None:
            enable_release_channels = value_from_snowflake_yml
        else:
            enable_release_channels = feature_flag_from_config
            if feature_flag_from_config is not None:
                self._workspace_ctx.console.warning(
                    f"{FeatureFlag.ENABLE_RELEASE_CHANNELS.name} value in config.toml is deprecated."
                    f" Set [enable_release_channels] for the application package in snowflake.yml instead."
                )

        feature_enabled_in_account = (
            get_snowflake_facade().get_ui_parameter(
                UIParameter.NA_FEATURE_RELEASE_CHANNELS, "ENABLED"
            )
            == "ENABLED"
        )
        if enable_release_channels and not feature_enabled_in_account:
            self._workspace_ctx.console.warning(
                f"Ignoring [enable_release_channels] value because "
                "release channels are not enabled in the current account."
            )
            return None

        return enable_release_channels

    def create_app_package(self) -> None:
        """
        Creates the application package with our up-to-date stage if none exists.
        """
        model = self._entity_model
        console = self._workspace_ctx.console

        # 1. Check for existing application package
        show_obj_row = self.get_existing_app_pkg_info()

        if show_obj_row:
            # 2. Check distribution of the existing application package
            actual_distribution = self.get_app_pkg_distribution_in_snowflake()
            if not self.verify_project_distribution(
                expected_distribution=actual_distribution
            ):
                console.warning(
                    f"Continuing to execute `snow app run` on application package {self.name} with distribution '{actual_distribution}'."
                )

            # 3. If actual_distribution is external, skip comment check
            if actual_distribution == INTERNAL_DISTRIBUTION:
                row_comment = show_obj_row[COMMENT_COL]

                if row_comment not in ALLOWED_SPECIAL_COMMENTS:
                    raise ApplicationPackageAlreadyExistsError(self.name)

            # 4. Update the application package with setting enable_release_channels if necessary
            get_snowflake_facade().alter_application_package_properties(
                package_name=self.name,
                enable_release_channels=self._get_enable_release_channels_flag(),
                role=self.role,
            )

            return

        # If no application package pre-exists, create an application package, with the specified distribution in the project definition file.
        console.step(f"Creating new application package {self.name} in account.")
        get_snowflake_facade().create_application_package(
            role=self.role,
            enable_release_channels=self._get_enable_release_channels_flag(),
            distribution=model.distribution,
            package_name=self.name,
        )

    def execute_post_deploy_hooks(self):
        execute_post_deploy_hooks(
            console=self._workspace_ctx.console,
            project_root=self.project_root,
            post_deploy_hooks=self.post_deploy_hooks,
            deployed_object_type="application package",
            role_name=self.role,
            warehouse_name=self.warehouse,
            database_name=self.name,
        )

    def validate_setup_script(
        self,
        action_ctx: ActionContext,
        use_scratch_stage: bool,
        interactive: bool,
        force: bool,
    ):
        workspace_ctx = self._workspace_ctx
        console = workspace_ctx.console

        """Validates Native App setup script SQL."""
        with console.phase(f"Validating Snowflake Native App setup script."):
            validation_result = self.get_validation_result(
                action_ctx=action_ctx,
                use_scratch_stage=use_scratch_stage,
                force=force,
                interactive=interactive,
            )

            # First print warnings, regardless of the outcome of validation
            for warning in validation_result.get("warnings", []):
                console.warning(validation_item_to_str(warning))

            # Then print errors
            for error in validation_result.get("errors", []):
                # Print them as warnings for now since we're going to be
                # revamping CLI output soon
                console.warning(validation_item_to_str(error))

            # Then raise an exception if validation failed
            if validation_result["status"] == "FAIL":
                raise SetupScriptFailedValidation()

    @span("validate_setup_script")
    def get_validation_result(
        self,
        action_ctx: ActionContext,
        use_scratch_stage: bool,
        interactive: bool,
        force: bool,
    ):
        """Call system$validate_native_app_setup() to validate deployed Native App setup script."""
        stage_path = self.stage_path
        if use_scratch_stage:
            stage_path = self.scratch_stage_path
            self._deploy(
                action_ctx=action_ctx,
                bundle_map=None,
                prune=True,
                recursive=True,
                paths=[],
                print_diff=False,
                validate=False,
                stage_path=stage_path,
                interactive=interactive,
                force=force,
                run_post_deploy_hooks=False,
            )
        prefixed_stage_fqn = StageManager.get_standard_stage_prefix(
            stage_path.full_path
        )

        sql_executor = get_sql_executor()
        try:
            cursor = sql_executor.execute_query(
                f"call system$validate_native_app_setup('{prefixed_stage_fqn}')"
            )
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                raise ApplicationPackageDoesNotExistError(self.name)
            generic_sql_error_handler(err)
        else:
            if not cursor.rowcount:
                raise SnowflakeSQLExecutionError()
            return json.loads(cursor.fetchone()[0])
        finally:
            if use_scratch_stage:
                self._workspace_ctx.console.step(
                    f"Dropping stage {self.scratch_stage_path.stage}."
                )
                with sql_executor.use_role(self.role):
                    sql_executor.execute_query(
                        f"drop stage if exists {self.scratch_stage_path.stage}"
                    )

    def resolve_version_info(
        self,
        version: str | None,
        patch: int | None,
        label: str | None,
        bundle_map: BundleMap | None,
        policy: PolicyBase,
        interactive: bool,
    ) -> VersionInfo:
        """Determine version name, patch number, and label from CLI provided values and manifest.yml version entry.
        @param [Optional] version: version name as specified in the command
        @param [Optional] patch: patch number as specified in the command
        @param [Optional] label: version/patch label as specified in the command
        @param [Optional] bundle_map: bundle_map if a deploy_root is prepared. _bundle() is performed otherwise.
        @param policy: CLI policy
        @param interactive: True if command is run in interactive mode, otherwise False

        @return VersionInfo: version_name, patch_number, label resolved from CLI and manifest.yml
        """
        console = self._workspace_ctx.console

        resolved_version = None
        resolved_patch = None
        resolved_label = None

        # If version is specified in CLI, no version information from manifest.yml is used (except for comment, we can't control comment as of now).
        if version is not None:
            console.message(
                "Ignoring version information from the application manifest since a version was explicitly specified with the command."
            )
            resolved_patch = patch
            resolved_label = label
            resolved_version = version

        # When version is not set by CLI, version name is read from manifest.yml. patch and label from CLI will be used, if provided.
        else:
            console.message(
                dedent(
                    f"""\
                        Version was not provided through the Snowflake CLI. Checking version in the manifest.yml instead.
                    """
                )
            )
            if bundle_map is None:
                self._bundle()
            (
                resolved_version,
                patch_manifest,
                label_manifest,
            ) = find_version_info_in_manifest_file(self.deploy_root)
            if resolved_version is None:
                raise ClickException(
                    "Manifest.yml file does not contain a value for the version field."
                )

            # If patch is set in CLI and is also present in manifest.yml with different value, confirmation from
            # user is required to ignore patch from manifest.yml and proceed with CLI value.
            if (
                patch is not None
                and patch_manifest is not None
                and patch_manifest != patch
            ):
                console.warning(
                    f"Cannot resolve version. Found patch: {patch_manifest} in manifest.yml which is different from provided patch {patch}."
                )
                user_prompt = f"Do you want to ignore patch in manifest.yml and proceed with provided --patch {patch}?"
                if not policy.should_proceed(user_prompt):
                    if interactive:
                        console.message("Not creating a new patch.")
                        raise typer.Exit(0)
                    else:
                        console.message(
                            "Could not create a new patch non-interactively without --force."
                        )
                        raise typer.Exit(1)
                resolved_patch = patch
            elif patch is not None:
                resolved_patch = patch
            else:
                resolved_patch = patch_manifest

            # If label is not specified in CLI, label from manifest.yml is used. Even if patch is from CLI.
            resolved_label = label if label is not None else label_manifest

        # Check if patch needs to throw a bad option error, either if application package does not exist or if version does not exist
        # If patch is 0 and version does not exist, it is a valid case, because patch 0 is the first patch in a version.
        if resolved_patch:
            try:
                if not self.get_existing_version_info(to_identifier(resolved_version)):
                    raise BadOptionUsage(
                        option_name="patch",
                        message=f"Version {resolved_version} is not defined in the application package {self.name}. Try again with a patch of 0 or without specifying any patch.",
                    )
            except ApplicationPackageDoesNotExistError as app_err:
                raise BadOptionUsage(
                    option_name="patch",
                    message=f"Application package {self.name} does not exist yet. Try again with a patch of 0 or without specifying any patch.",
                )

        return VersionInfo(
            version_name=resolved_version,
            patch_number=resolved_patch,
            label=resolved_label,
        )
