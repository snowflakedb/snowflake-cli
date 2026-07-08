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
import logging
from dataclasses import dataclass
from typing import List, Optional

import typer
import yaml
from snowflake.cli._plugins.connection.util import get_account_identifier
from snowflake.cli._plugins.dcm.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.dcm.models import (
    DEFAULT_DEFINITION_FILE_NAME,
    DEFAULT_TARGET_NAME,
    DEFAULT_WAREHOUSE_NAME,
    DEFINITIONS_FOLDER,
    MANIFEST_FILE_NAME,
    SOURCES_FOLDER,
    DCMManifest,
    DCMTarget,
    TargetContext,
    render_default_definition,
    render_default_manifest,
    render_target_block,
)
from snowflake.cli._plugins.dcm.progress import DeployProgressTracker
from snowflake.cli._plugins.dcm.reporters import (
    AnalyzeErrorsReporter,
    AnalyzeReporter,
    DependenciesReporter,
    PlanReporter,
    RefreshReporter,
    TestReporter,
)
from snowflake.cli._plugins.dcm.utils import (
    RENDERED_DEFINITIONS_FOLDER,
    announce_rendered_definitions,
    clear_command_artifacts,
    mock_dcm_response,
)
from snowflake.cli._plugins.object.command_aliases import add_object_command_aliases
from snowflake.cli._plugins.object.commands import scope_option
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import (
    ForceOption,
    IdentifierType,
    IfExistsOption,
    IfNotExistsOption,
    InteractiveOption,
    LocalDirectoryType,
    identifier_argument,
    like_option,
    variables_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import (
    DEFAULT_SIZE_LIMIT_MB,
    ObjectType,
)
from snowflake.cli.api.exceptions import CliError, FQNNameError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN, AccountIdentifier
from snowflake.cli.api.output.types import (
    MessageResult,
    QueryResult,
)
from snowflake.cli.api.project.util import same_identifiers, to_quoted_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutor
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)

app = SnowTyperFactory(
    name="dcm",
    help="Manages DCM Projects in Snowflake.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_PROJECTS.is_disabled,
)

dcm_identifier = identifier_argument(sf_object="DCM Project", example="MY_PROJECT")
variables_flag = variables_option(
    'Variables for the execution context; for example: `-D "<key>=<value>"`.'
)


def _from_option_callback(value: Optional[SecurePath]) -> SecurePath:
    """Handles None default by returning cwd."""
    return value if value is not None else SecurePath.cwd()


from_option = typer.Option(
    None,
    "--from",
    help="Local directory path containing DCM project files. Omit to use current directory.",
    show_default=False,
    click_type=LocalDirectoryType(),
    callback=_from_option_callback,
)

alias_option = typer.Option(
    None,
    "--alias",
    help="Alias for the deployment.",
    show_default=False,
)

terse_option = typer.Option(
    False,
    "--terse",
    help="Returns only a subset of output columns.",
    show_default=False,
)

limit_option = typer.Option(
    None,
    "--limit",
    help="Limits the maximum number of rows returned.",
    show_default=False,
)

target_option = typer.Option(
    None,
    "--target",
    help="Target profile from `manifest.yml` to use. Uses `default_target` if not specified.",
    show_default=False,
)

save_output_option = typer.Option(
    False,
    "--save-output",
    help="Save command response and artifacts to local 'out/' directory.",
)


optional_dcm_identifier = typer.Argument(
    None,
    help="""
        Identifier of DCM Project. Example: MY_DB.MY_SCHEMA.MY_PROJECT.
        Supports fully qualified (recommended) or simple names.
        If unqualified, it defaults to the connection's database and schema.
        Optional if `--target` or `default_target` is defined in the manifest.
    """,
    show_default=False,
    click_type=IdentifierType(),
)


init_project_name_option = typer.Option(
    None,
    "--project-name",
    help=(
        "Name of a new DCM Project to create. A subfolder with this name is created "
        "in the current directory to hold the `manifest.yml` and `sources/` files. "
        "Omit it to add a target to an existing `manifest.yml` in the current directory."
    ),
    show_default=False,
)


init_target_option = typer.Option(
    None,
    "--target",
    help=(
        "Name of the target to create in the manifest. Defaults to the current "
        "account alias. Required in non-interactive mode."
    ),
    show_default=False,
)


init_project_identifier_option = typer.Option(
    None,
    "--project-identifier",
    help=(
        "Identifier of the DCM Project object in Snowflake for the new target "
        "(e.g. MY_DB.MY_SCHEMA.MY_PROJECT). Defaults to the target name. If "
        "unqualified, the connection's database and schema are used."
    ),
    show_default=False,
)


_ACCOUNT_GUIDANCE = "The current session account is required to match the manifest target's account_identifier."
_ROLE_GUIDANCE = (
    "The current session role is required to match the manifest target's project_owner."
)


def _warn_cannot_validate(thing_name: str, reason: str, guidance: str) -> None:
    cli_console.warning(
        f"⚠️  Cannot validate target's {thing_name}: {reason}. {guidance}"
    )


def _check_account_identifier(target: DCMTarget) -> None:
    if not target.account_identifier:
        _warn_cannot_validate(
            "account identifier",
            "account_identifier is not specified in the manifest target",
            _ACCOUNT_GUIDANCE,
        )
        return

    try:
        current_account = get_account_identifier(get_cli_context().connection)
    except Exception as e:
        _warn_cannot_validate(
            "account identifier",
            f"current account could not be determined: {e}",
            _ACCOUNT_GUIDANCE,
        )
        return

    if current_account != AccountIdentifier.from_string(target.account_identifier):
        cli_console.warning(
            f"⚠️  Account mismatch: manifest target specifies account_identifier "
            f"'{sanitize_for_terminal(target.account_identifier)}', "
            f"but the current session account is '{sanitize_for_terminal(str(current_account))}'."
        )


def _check_project_owner(target: DCMTarget) -> None:
    if not target.project_owner:
        _warn_cannot_validate(
            "project owner",
            "project_owner is not specified in the manifest target",
            _ROLE_GUIDANCE,
        )
        return

    try:
        current_role = SqlExecutor().current_role()
    except Exception as e:
        _warn_cannot_validate(
            "project owner",
            f"current role could not be determined: {e}",
            _ROLE_GUIDANCE,
        )
        return

    if not current_role:
        _warn_cannot_validate(
            "project owner",
            "current role could not be determined",
            _ROLE_GUIDANCE,
        )
        return

    if not same_identifiers(current_role, target.project_owner):
        cli_console.warning(
            f"⚠️  Role mismatch: manifest target specifies project_owner "
            f"'{sanitize_for_terminal(target.project_owner)}', "
            f"but the current session role is '{sanitize_for_terminal(current_role)}'."
        )


def _resolve_target_context(
    identifier: Optional[FQN],
    target: Optional[str],
    source_path: SecurePath,
    validate_owner: bool = False,
) -> TargetContext:
    """
    Resolve project identifier and configuration from manifest target.

    - If identifier is provided, it takes precedence over target's project_name
    - Configuration is always resolved from target

    Raises:
        CliError: When manifest is invalid or misconfigured
    """
    log.info(
        "Resolving DCM target context (has_identifier=%s, target=%s, source_path=%s).",
        bool(identifier),
        target,
        source_path,
    )
    try:
        manifest = DCMManifest.load(source_path)
        effective_target = manifest.get_effective_target(target)
    except (InvalidManifestError, ManifestConfigurationError) as e:
        log.info("Failed to resolve DCM manifest context: %s.", e)
        raise CliError(str(e))

    project_id = (
        identifier if identifier else FQN.from_string(effective_target.project_name)
    )
    log.info(
        "Resolved DCM target context (project_identifier=%s, has_configuration=%s).",
        project_id,
        bool(effective_target.templating_config),
    )
    _check_account_identifier(effective_target)
    if validate_owner:
        _check_project_owner(effective_target)
    return TargetContext(
        project_identifier=project_id,
        configuration=effective_target.templating_config,
    )


def _resolve_context_with_required_manifest(
    from_location: SecurePath,
    identifier: FQN | None,
    target: str | None,
    validate_owner: bool = False,
) -> TargetContext:
    try:
        context = _resolve_target_context(
            identifier, target, from_location, validate_owner=validate_owner
        )
    except ManifestNotFoundError as e:
        raise CliError(str(e))
    return context


def _resolve_context_with_optional_manifest(
    from_location: SecurePath,
    identifier: FQN | None,
    target: str | None,
    validate_owner: bool = False,
) -> TargetContext:
    try:
        context = _resolve_target_context(
            identifier, target, from_location, validate_owner=validate_owner
        )
    except ManifestNotFoundError:
        if not identifier:
            raise CliError(
                "No manifest.yml found. Please provide a project identifier or create a manifest.yml file."
            )
        if target:
            raise CliError(
                f"Cannot use --target '{target}' without a valid manifest.yml."
            )
        context = TargetContext(project_identifier=identifier)
    return context


add_object_command_aliases(
    app=app,
    object_type=ObjectType.DCM_PROJECT,
    name_argument=dcm_identifier,
    like_option=like_option(
        help_example='`list --like "my%"` lists all DCM Projects that begin with "my"'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["create", "drop", "describe"],
    terse_option=terse_option,
    limit_option=limit_option,
)


@app.command(requires_connection=True)
def deploy(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    alias: Optional[str] = alias_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    skip_plan: bool = typer.Option(
        False,
        "--skip-plan",
        help="Skips planning step.",
        hidden=True,
    ),
    **options,
):
    """
    Deploys local project changes to Snowflake by creating, altering, or dropping objects to match your definition files.
    """
    clear_command_artifacts("deploy")

    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    if skip_plan:
        cli_console.warning("Skipping planning step")

    manager = DCMProjectManager()
    tracker = DeployProgressTracker(conn=manager.connection)
    with tracker.session():
        effective_stage = manager.sync_local_files(
            project_identifier=project_id,
            source_directory=str(from_location.path),
            progress=tracker,
        )
        sfqid = manager.deploy_async(
            project_identifier=project_id,
            configuration=context.configuration,
            from_stage=effective_stage,
            variables=variables,
            alias=alias,
            skip_plan=skip_plan,
        )
        result = tracker.run_deploy_poll(sfqid)

    reporter = PlanReporter(save_output=save_output, command_name="deploy")
    return reporter.process(result)


_PURGE_CONFIRM_COMMAND = "PURGE"
_PURGE_CANCEL_COMMAND = "CANCEL"


def _confirm_purge(project_id: FQN) -> None:
    cli_console.warning(
        f"⚠️  DANGER: This operation will DROP ALL objects managed by DCM Project {project_id}  ⚠️"
    )
    expected_identifier = str(project_id)
    while True:
        user_input = typer.prompt(
            f"Type 'purge {expected_identifier}' to confirm or 'cancel' to abort",
            show_default=False,
        )
        parts = user_input.strip().split(maxsplit=1)
        if not parts:
            continue

        command = parts[0].upper()

        if command == _PURGE_CANCEL_COMMAND:
            raise typer.Abort()

        if command == _PURGE_CONFIRM_COMMAND and len(parts) == 2:
            if parts[1].upper() == expected_identifier.upper():
                return
            cli_console.message(
                f"  Project identifier mismatch. Expected: {expected_identifier}, provided: {parts[1]}"
            )


@app.command(requires_connection=True)
def purge(
    identifier: Optional[FQN] = optional_dcm_identifier,
    alias: Optional[str] = alias_option,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    skip_plan: bool = typer.Option(
        False,
        "--skip-plan",
        help="Skips planning step.",
        hidden=True,
    ),
    **options,
):
    """
    Drops all the objects managed by the DCM Project, but does not drop the project itself.
    """
    clear_command_artifacts("purge")

    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    if not force and not interactive:
        raise CliError(
            "Cannot purge the DCM project non-interactively without --force."
        )
    if not force:
        _confirm_purge(project_id)

    if skip_plan:
        cli_console.warning("Skipping planning step")

    manager = DCMProjectManager()
    tracker = DeployProgressTracker(conn=manager.connection, operation="purge")
    with tracker.session():
        sfqid = manager.purge_async(
            project_identifier=project_id,
            alias=alias,
            skip_plan=skip_plan,
        )
        result = tracker.run_deploy_poll(sfqid)

    reporter = PlanReporter(save_output=save_output, command_name="purge")
    return reporter.process(result)


@app.command(requires_connection=True)
@mock_dcm_response("plan")
def plan(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    delta: bool = typer.Option(
        False,
        "--delta",
        help="Process only statements changed since the last `deploy`, plus statements potentially impacted by those changes.",
    ),
    **options,
):
    """
    Shows what objects would be created, altered, or dropped by the `deploy` command, without applying any changes.
    """
    clear_command_artifacts("plan")

    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    tracker = DeployProgressTracker(conn=manager.connection, operation="plan")
    with tracker.session():
        effective_stage = manager.sync_local_files(
            project_identifier=project_id,
            source_directory=str(from_location.path),
            progress=tracker,
        )
        result = tracker.run_loader_phase(
            lambda: manager.plan(
                project_identifier=project_id,
                configuration=context.configuration,
                from_stage=effective_stage,
                variables=variables,
                save_output=save_output,
                delta=delta,
            ),
            phase_name="PLAN",
            simulated_phases=("RENDER", "COMPILE"),
        )

    reporter = PlanReporter(save_output=save_output, command_name="plan")
    return reporter.process(result)


@app.command(requires_connection=True, hidden=True)
def raw_analyze(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """Analyzes a DCM Project."""
    clear_command_artifacts("raw-analyze")

    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = manager.sync_local_files(
        project_identifier=project_id,
        source_directory=str(from_location.path),
    )

    with cli_console.spinner() as spinner:
        spinner.add_task(description=f"Analyzing dcm project {project_id}", total=None)
        result = manager.raw_analyze(
            project_identifier=project_id,
            configuration=context.configuration,
            from_stage=effective_stage,
            variables=variables,
            save_output=save_output,
        )

    reporter = AnalyzeReporter(save_output=save_output)
    return reporter.process(result)


@app.command(
    name="compile",
    requires_connection=True,
)
def compile_project(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """
    Compiles a DCM Project and prints a formatted list of errors found.
    """
    clear_command_artifacts("compile", folder_name=RENDERED_DEFINITIONS_FOLDER)

    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    tracker = DeployProgressTracker(conn=manager.connection, operation="compile")
    with tracker.session():
        effective_stage = manager.sync_local_files(
            project_identifier=project_id,
            source_directory=str(from_location.path),
            progress=tracker,
        )
        result = tracker.run_loader_phase(
            lambda: manager.raw_analyze(
                project_identifier=project_id,
                configuration=context.configuration,
                from_stage=effective_stage,
                variables=variables,
                save_output=save_output,
                command_name="compile",
                output_folder_name=RENDERED_DEFINITIONS_FOLDER,
            ),
            phase_name="COMPILE",
            simulated_phases=["RENDER"],
        )

    reporter = AnalyzeErrorsReporter(save_output=save_output)
    if save_output:
        announce_rendered_definitions()
    # The reporter prints the trailing "=" divider itself (Reporter.print_separator),
    # on both the success and error paths, so no separate separator is needed here.
    return reporter.process(result)


@app.command(
    requires_connection=True,
)
def dependencies(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """
    Analyzes a DCM Project and generates a dependency diagram.

    The diagram is written as a Mermaid flowchart in a Markdown file that can
    be opened in your IDE's Markdown preview to explore object dependencies.
    """
    clear_command_artifacts("dependencies", folder_name=RENDERED_DEFINITIONS_FOLDER)

    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    tracker = DeployProgressTracker(conn=manager.connection, operation="compile")
    with tracker.session():
        effective_stage = manager.sync_local_files(
            project_identifier=project_id,
            source_directory=str(from_location.path),
            progress=tracker,
        )
        result = tracker.run_loader_phase(
            lambda: manager.raw_analyze(
                project_identifier=project_id,
                configuration=context.configuration,
                from_stage=effective_stage,
                variables=variables,
                save_output=save_output,
                command_name="dependencies",
                output_folder_name=RENDERED_DEFINITIONS_FOLDER,
            ),
            phase_name="COMPILE",
            simulated_phases=["RENDER"],
        )

    reporter = DependenciesReporter(
        project_identifier=project_id, save_output=save_output
    )
    try:
        return reporter.process(result)
    finally:
        if save_output:
            announce_rendered_definitions()


_ACCOUNT_PLACEHOLDER = "MY_ORG-MY_ACCOUNT"


def _resolve_account_context_for_manifest() -> tuple[str, str]:
    """Resolve the account identifier and default target name for a new manifest.

    Returns a tuple ``(account_identifier, target_name)`` where
    ``account_identifier`` is ``ORG-LOCATOR`` and ``target_name`` is the current
    account's alias. Falls back to placeholders (with a warning) if the account
    cannot be determined.
    """
    try:
        *_, cursor = get_cli_context().connection.execute_string(
            "SELECT CURRENT_ORGANIZATION_NAME() AS org, CURRENT_ACCOUNT_NAME() AS alias",
            cursor_class=DictCursor,
        )
        row = cursor.fetchone() or {}
        org = row.get("ORG")
        alias = row.get("ALIAS")
        if not (org and alias):
            raise ValueError(f"org={org!r}, alias={alias!r}")
        return f"{org}-{alias}", alias
    except Exception as e:
        cli_console.warning(
            f"⚠️  Could not determine the current account ({e}). "
            f"Using placeholders in {MANIFEST_FILE_NAME}; update them before deploying."
        )
        return _ACCOUNT_PLACEHOLDER, DEFAULT_TARGET_NAME


def _resolve_project_owner_for_manifest(interactive: bool) -> str:
    """Resolve the current role that will own the DCM Project for a new manifest.

    Uses the current session role when it can be determined. Otherwise the user
    must supply one: prompts for it in interactive mode, or errors so a run never
    silently records a placeholder ``project_owner``.
    """
    role: Optional[str] = None
    try:
        role = SqlExecutor().current_role()
    except Exception as e:
        cli_console.warning(f"⚠️  Could not determine the current role ({e}).")
    if role:
        return role

    if not interactive:
        raise CliError(
            "Could not determine the current role to use as the DCM Project's "
            "project_owner. Run `snow dcm init` interactively to enter a role name."
        )
    entered = typer.prompt(
        "Could not determine the current role. Enter the role that will own the "
        "DCM Project"
    ).strip()
    if not entered:
        raise CliError("A project owner role is required.")
    return entered


def _new_project_dir(cwd: SecurePath, project_name: str) -> SecurePath:
    """Return the subfolder for a new project, erroring if it already exists."""
    name = project_name.strip()
    if not name:
        raise CliError("A project name is required.")
    target_dir = cwd / name
    if target_dir.exists():
        raise CliError(
            f"A '{name}' folder already exists in the current directory. "
            f"Choose a different project name."
        )
    return target_dir


def _prompt_new_project_name(cwd: SecurePath) -> SecurePath:
    """Interactively prompt for a new project name whose folder does not exist."""
    while True:
        name = typer.prompt(
            "Enter a name for the new project (a subfolder with this name is created)"
        ).strip()
        if not name:
            cli_console.warning("A project name is required.")
            continue
        if (cwd / name).exists():
            cli_console.warning(
                f"A '{name}' folder already exists. Choose a different name."
            )
            continue
        return cwd / name


def _resolve_project_location(
    project_name: Optional[str], interactive: bool, force: bool
) -> tuple[SecurePath, bool]:
    """Resolve where ``init`` writes files.

    Returns ``(target_dir, append_to_existing)``. ``append_to_existing`` is True
    when a target is added to an existing ``manifest.yml`` in the current
    directory, and False when a new project is scaffolded in a subfolder.
    """
    cwd = SecurePath.cwd()

    # Explicit new project via --project-name.
    if project_name is not None:
        return _new_project_dir(cwd, project_name), False

    cwd_manifest = cwd / MANIFEST_FILE_NAME
    if cwd_manifest.exists():
        # Add to the existing manifest unless the user opts to start a new project.
        if force or not interactive:
            return cwd, True
        cli_console.message(
            f"A {MANIFEST_FILE_NAME} already exists in the current directory."
        )
        if typer.confirm("Add a new target to it?", default=True):
            return cwd, True
        raise CliError(
            f"A {MANIFEST_FILE_NAME} already exists here. To create a new project, "
            f"change to an empty directory first and run `snow dcm init` from there."
        )

    # No manifest to add to: a new project is required.
    if force or not interactive:
        raise CliError(
            f"No {MANIFEST_FILE_NAME} found in the current directory. Pass "
            f"--project-name to create a new project."
        )
    return _prompt_new_project_name(cwd), False


def _resolve_target_name(
    target: Optional[str],
    default_target: str,
    existing_targets: dict,
    append_to_existing: bool,
    interactive: bool,
    force: bool,
) -> tuple[str, bool]:
    """Resolve the target name and whether it must be appended to the manifest.

    Returns ``(target_name, append_target)``. When adding to an existing manifest,
    a name that already exists is reused (``append_target=False``) in
    non-interactive/``--force`` runs, or re-prompted interactively.
    """

    def _collides(name: str) -> bool:
        return any(str(n).upper() == name.upper() for n in existing_targets)

    def _validate(name: str) -> Optional[str]:
        if not name:
            return "Target name cannot be empty."
        if '"' in name or "\\" in name:
            return 'Target name cannot contain " or \\ characters.'
        return None

    if force or not interactive:
        if not target:
            raise CliError("--target is required in non-interactive mode.")
        name = target.strip()
        error = _validate(name)
        if error:
            raise CliError(error)
        return name, not (append_to_existing and _collides(name))

    if existing_targets:
        cli_console.message("Existing targets:")
        with cli_console.indented():
            for existing in existing_targets:
                cli_console.message(f"- {existing}")
    while True:
        name = typer.prompt(
            "Enter a name for the new target",
            default=target or default_target,
            show_default=True,
        ).strip()
        error = _validate(name)
        if error:
            cli_console.warning(f"{error} Choose another name.")
            continue
        if append_to_existing and _collides(name):
            cli_console.warning(
                f"Target '{name}' already exists in the manifest. "
                f"Choose a different name."
            )
            continue
        return name, True


def _parse_object_name(raw: str) -> FQN:
    """Parse the DCM Project object name, auto-quoting special characters."""
    raw = raw.strip()
    if not raw:
        raise CliError("A DCM Project object name is required.")
    try:
        return FQN.from_string(raw)
    except FQNNameError:
        # Unqualified name with special characters: quote it automatically so it
        # becomes a valid Snowflake identifier instead of failing.
        return FQN.from_string(to_quoted_identifier(raw))


def _resolve_namespace_part(
    kind: str, default: Optional[str], interactive: bool, force: bool
) -> str:
    """Resolve a missing database/schema, confirming the connection default or prompting."""
    if interactive and not force:
        if default and typer.confirm(
            f"Use the connection's default {kind} '{default}'?", default=True
        ):
            return default
        entered = typer.prompt(f"Enter the {kind} for the DCM Project").strip()
        if not entered:
            raise CliError(f"A {kind} is required for the DCM Project.")
        return entered
    if not default:
        raise CliError(
            f"No {kind} could be determined for the DCM Project. Provide a fully "
            f"qualified --project-identifier, or set a {kind} on your connection."
        )
    return default


def _resolve_project_object_identifier(
    project_identifier: Optional[str],
    target_name: str,
    interactive: bool,
    force: bool,
) -> FQN:
    """Resolve the DCM Project object identifier (defaulting to the target name).

    Prompts to confirm/override the name (interactive), auto-quotes special
    characters, and fills in the database/schema when the name is not fully
    qualified.
    """
    if project_identifier is None and interactive and not force:
        project_identifier = typer.prompt(
            "Enter the DCM Project object name",
            default=target_name,
            show_default=True,
        )
    raw = project_identifier if project_identifier is not None else target_name
    fqn = _parse_object_name(raw)

    database = fqn.database or _resolve_namespace_part(
        "database", get_cli_context().connection.database, interactive, force
    )
    schema = fqn.schema or _resolve_namespace_part(
        "schema", get_cli_context().connection.schema, interactive, force
    )
    return fqn.set_database(database).set_schema(schema)


def _resolve_warehouse(interactive: bool) -> Optional[str]:
    """Determine the warehouse to ensure for DCM commands.

    Returns None when the connection already has a warehouse (nothing to do).
    Otherwise returns a warehouse name to ensure exists: either one the user
    names interactively, or the default `DCM_WH`.
    """
    if get_cli_context().connection.warehouse:
        return None

    if interactive:
        warehouse = typer.prompt(
            "No warehouse is configured for this connection. Enter the name of a "
            "warehouse to use, or press Enter to create an X-Small warehouse named "
            f"'{DEFAULT_WAREHOUSE_NAME}'",
            default=DEFAULT_WAREHOUSE_NAME,
            show_default=True,
        ).strip()
        return warehouse or DEFAULT_WAREHOUSE_NAME

    return DEFAULT_WAREHOUSE_NAME


def _warn_configure_warehouse(warehouse: str) -> None:
    """Tell the user how to make the warehouse usable for later DCM commands."""
    cli_console.warning(
        f"⚠️  Your connection has no warehouse. Before running `snow dcm plan` or "
        f"`snow dcm deploy`, use warehouse '{warehouse}' by either:\n"
        f"    - passing --warehouse {warehouse} to those commands, or\n"
        f"    - adding it to your connection in config.toml:\n"
        f"          [connections.<your_connection>]\n"
        f'          warehouse = "{warehouse}"'
    )


def _insert_target_block(manifest_text: str, target_block: str) -> str:
    """Insert ``target_block`` as the last entry of the manifest's ``targets:`` section."""
    lines = manifest_text.splitlines(keepends=True)

    targets_idx = None
    for i, line in enumerate(lines):
        if line[:1].isspace():
            continue  # not a top-level key
        if line.rstrip("\n").rstrip() == "targets:":
            targets_idx = i
            break
    if targets_idx is None:
        raise CliError(
            f"Could not find a top-level 'targets:' section in {MANIFEST_FILE_NAME} "
            f"to append to."
        )

    # The targets block spans indented (or blank) lines after `targets:`.
    last_child = targets_idx
    for j in range(targets_idx + 1, len(lines)):
        line = lines[j]
        if line.strip() == "":
            continue  # blank lines do not terminate the block
        if line[:1].isspace():
            last_child = j
            continue
        break  # a new top-level key ends the targets block

    # Ensure the line we insert after ends with a newline.
    if not lines[last_child].endswith("\n"):
        lines[last_child] = lines[last_child] + "\n"

    insert_at = last_child + 1
    return "".join(lines[:insert_at]) + target_block + "".join(lines[insert_at:])


@dataclass
class _InitPlan:
    """A resolved plan of everything ``snow dcm init`` will create or change."""

    identifier: FQN
    database: str
    schema: str
    warehouse: Optional[str]  # None when the connection already has one
    create_warehouse: bool
    create_database: bool
    create_schema: bool
    manifest_exists: bool
    project_exists: bool
    account_identifier: str
    target_name: str
    project_owner: str
    is_new_project: bool = False
    append_target: bool = False
    existing_manifest_text: Optional[str] = None


def _read_existing_targets(manifest_path: SecurePath) -> tuple[str, dict]:
    """Read an existing manifest, returning its text and its ``targets`` mapping."""
    text = manifest_path.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError as e:
        raise CliError(f"Could not parse {manifest_path.path}: {e}")
    targets = data.get("targets") or {}
    if not isinstance(targets, dict):
        raise CliError(
            f"Cannot append a target: 'targets' in {manifest_path.path} is not a mapping."
        )
    return text, targets


def _build_init_plan(
    dpm: DCMProjectManager,
    identifier: FQN,
    target_name: str,
    append_target: bool,
    account_identifier: str,
    project_owner: str,
    warehouse: Optional[str],
    manifest_exists: bool,
    is_new_project: bool,
    existing_manifest_text: Optional[str],
    project_exists: bool,
) -> _InitPlan:
    """Resolve, without mutating anything, what init needs to create/change."""
    database = identifier.database
    schema = identifier.schema

    if project_exists:
        # The project already exists, so its namespace does too.
        create_database = False
        create_schema = False
    else:
        db_exists = dpm.database_exists(database)
        create_database = not db_exists
        # A missing database implies a missing schema; don't query a schema
        # inside a database that does not exist yet (it would error).
        create_schema = (not dpm.schema_exists(database, schema)) if db_exists else True

    create_warehouse = warehouse is not None and not dpm.warehouse_exists(warehouse)

    return _InitPlan(
        identifier=identifier,
        database=database,
        schema=schema,
        warehouse=warehouse,
        create_warehouse=create_warehouse,
        create_database=create_database,
        create_schema=create_schema,
        manifest_exists=manifest_exists,
        project_exists=project_exists,
        account_identifier=account_identifier,
        target_name=target_name,
        project_owner=project_owner,
        is_new_project=is_new_project,
        append_target=append_target,
        existing_manifest_text=existing_manifest_text,
    )


def _init_actions_requiring_approval(
    plan: _InitPlan, target_dir: SecurePath
) -> List[str]:
    """Human-readable list of changes that need the user's explicit approval."""
    directory = target_dir.path.resolve()
    actions: List[str] = []
    if plan.manifest_exists:
        if plan.append_target:
            actions.append(
                f"Append target '{plan.target_name}' to existing {MANIFEST_FILE_NAME} "
                f"in '{directory}'"
            )
        # Reusing an existing target does not modify the manifest, so it is not a
        # change that needs approval.
    else:
        actions.append(
            f"Create a new project structure with target '{plan.target_name}' "
            f"({MANIFEST_FILE_NAME} and "
            f"{SOURCES_FOLDER}/{DEFINITIONS_FOLDER}/{DEFAULT_DEFINITION_FILE_NAME}) "
            f"in '{directory}'"
        )
    if plan.create_warehouse:
        actions.append(f"Create X-Small warehouse '{plan.warehouse}'")
    if plan.create_database:
        actions.append(f"Create database '{plan.database}'")
    if plan.create_schema:
        actions.append(f"Create schema '{plan.database}.{plan.schema}'")
    if not plan.project_exists:
        actions.append(
            f"Create DCM Project '{plan.identifier}' in Snowflake "
            f"(owned by role '{plan.project_owner}')"
        )
    return actions


def _confirm_init_plan(
    plan: _InitPlan, target_dir: SecurePath, force: bool, interactive: bool
) -> None:
    """Confirm all mutating actions up front, so nothing is created on abort."""
    actions = _init_actions_requiring_approval(plan, target_dir)
    if not actions or force:
        return

    if not interactive:
        listing = "\n".join(f"    - {a}" for a in actions)
        raise CliError(
            "The following changes need approval, but there is no interactive "
            f"terminal:\n{listing}\n"
            "Re-run `snow dcm init` interactively, or pass --force to approve them."
        )

    cli_console.message("`snow dcm init` will make the following changes:")
    with cli_console.indented():
        for action in actions:
            cli_console.message(f"- {action}")
    # For an existing manifest the user has already confirmed adding the target
    # (and named it), so don't ask a second time.
    if plan.manifest_exists:
        return
    if not typer.confirm("Proceed?", default=False):
        raise CliError("Aborted; no changes were made.")


def _scaffold_project_files(
    target_dir: SecurePath, manifest_path: SecurePath, plan: _InitPlan
) -> None:
    """Scaffold a new ``manifest.yml`` and a ``sources/definitions/raw.sql`` placeholder."""
    if not target_dir.exists():
        target_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        render_default_manifest(
            project_name=plan.identifier.identifier,
            account_identifier=plan.account_identifier,
            project_owner=plan.project_owner,
            target_name=plan.target_name,
        )
    )
    cli_console.step(f"Created {manifest_path.path}.")

    definitions_dir = target_dir / SOURCES_FOLDER / DEFINITIONS_FOLDER
    definitions_dir.mkdir(parents=True, exist_ok=True)
    definition_path = definitions_dir / DEFAULT_DEFINITION_FILE_NAME
    if definition_path.exists():
        cli_console.step(f"Using existing {definition_path.path}.")
    else:
        definition_path.write_text(render_default_definition())
        cli_console.step(f"Created {definition_path.path}.")


def _execute_init_plan(
    plan: _InitPlan,
    target_dir: SecurePath,
    manifest_path: SecurePath,
    dpm: DCMProjectManager,
) -> None:
    """Perform the approved plan in a safe order (no side effects before this point)."""
    # 1. Local files.
    if plan.manifest_exists:
        if plan.append_target:
            updated = _insert_target_block(
                plan.existing_manifest_text or "",
                render_target_block(
                    project_name=plan.identifier.identifier,
                    account_identifier=plan.account_identifier,
                    project_owner=plan.project_owner,
                    target_name=plan.target_name,
                ),
            )
            manifest_path.write_text(updated)
            cli_console.step(
                f"Appended target '{plan.target_name}' to {manifest_path.path} "
                f"(default target unchanged)."
            )
        else:
            cli_console.step(
                f"Reusing existing target '{plan.target_name}' in {manifest_path.path}."
            )
    else:
        _scaffold_project_files(target_dir, manifest_path, plan)

    # 2. Warehouse.
    if plan.warehouse is None:
        cli_console.step(f"Using warehouse '{get_cli_context().connection.warehouse}'.")
    elif plan.create_warehouse:
        dpm.create_warehouse(plan.warehouse)
        cli_console.step(f"Created X-Small warehouse '{plan.warehouse}'.")
    else:
        cli_console.step(f"Using existing warehouse '{plan.warehouse}'.")

    # 3. Namespace and project.
    if plan.project_exists:
        cli_console.step(
            f"DCM Project '{plan.identifier}' already exists in Snowflake; skipping creation."
        )
    else:
        if plan.create_database:
            dpm.create_database(plan.database)
            cli_console.step(f"Created database '{plan.database}'.")
        else:
            cli_console.step(f"Using existing database '{plan.database}'.")

        if plan.create_schema:
            dpm.create_schema(plan.database, plan.schema)
            cli_console.step(f"Created schema '{plan.database}.{plan.schema}'.")
        else:
            cli_console.step(f"Using existing schema '{plan.database}.{plan.schema}'.")

        dpm.create(project_identifier=plan.identifier)
        cli_console.step(f"Created DCM Project '{plan.identifier}' in Snowflake.")

    # 4. Guidance for a connection that had no warehouse.
    if plan.warehouse is not None:
        _warn_configure_warehouse(plan.warehouse)


def _print_init_next_steps(plan: _InitPlan, target_dir: SecurePath) -> None:
    """Print short guidance on how to proceed after initialization."""
    cli_console.message("\nNext steps:")
    with cli_console.indented():
        step = 1
        if plan.is_new_project:
            cli_console.message(
                f"{step}. Change into the project folder: `cd {target_dir.path.name}`."
            )
            step += 1
        if plan.manifest_exists:
            cli_console.message(
                f"{step}. Review or add object definitions in the "
                f"'{SOURCES_FOLDER}/{DEFINITIONS_FOLDER}' folder."
            )
        else:
            cli_console.message(
                f"{step}. Add your object definitions to the "
                f"'{SOURCES_FOLDER}/{DEFINITIONS_FOLDER}' folder "
                f"(edit the generated '{DEFAULT_DEFINITION_FILE_NAME}' placeholder)."
            )
        step += 1
        cli_console.message(
            f"{step}. Run `snow dcm plan {plan.identifier} --target {plan.target_name}` "
            f"to preview the changes, then "
            f"`snow dcm deploy {plan.identifier} --target {plan.target_name}` "
            f"to apply them."
        )


@app.command(
    requires_connection=True,
)
def init(
    project_name: Optional[str] = init_project_name_option,
    target: Optional[str] = init_target_option,
    project_identifier: Optional[str] = init_project_identifier_option,
    if_not_exists: bool = IfNotExistsOption(
        help="Do nothing if the project already exists in Snowflake."
    ),
    force: bool = ForceOption,
    interactive: bool = InteractiveOption,
    **options,
):
    """
    Initializes a DCM Project. You either create a new project — pass `--project-name` (or choose one interactively) and a subfolder is created with a `manifest.yml` and a `sources/definitions/raw.sql` placeholder — or add a new target to an existing `manifest.yml` in the current directory. You then name the target (defaults to the account alias) and the DCM Project object (defaults to the target name; special characters are automatically double-quoted). If the object name is not fully qualified, the connection's default database and schema are used (or you are prompted), and they are created if missing. All changes are summarized and confirmed up front; pass `--force` to approve them non-interactively (which then requires `--target`). Nothing is created if you decline. After initialization, prints the next steps.
    """
    # Step 1: choose a new project (subfolder) or an existing manifest in the cwd.
    target_dir, append_to_existing = _resolve_project_location(
        project_name, interactive, force
    )
    manifest_path = target_dir / MANIFEST_FILE_NAME
    is_new_project = not append_to_existing

    existing_manifest_text: Optional[str] = None
    existing_targets: dict = {}
    if append_to_existing:
        existing_manifest_text, existing_targets = _read_existing_targets(manifest_path)

    account_identifier, default_target = _resolve_account_context_for_manifest()

    # Step 2: name the new target.
    target_name, append_target = _resolve_target_name(
        target,
        default_target,
        existing_targets,
        append_to_existing,
        interactive,
        force,
    )

    # Step 3 & 4: name the DCM Project object and qualify its namespace.
    identifier = _resolve_project_object_identifier(
        project_identifier, target_name, interactive, force
    )
    project_owner = _resolve_project_owner_for_manifest(interactive and not force)

    om = ObjectManager()
    project_exists = om.object_exists(object_type="dcm", fqn=identifier)
    if project_exists and not if_not_exists:
        raise CliError(f"DCM Project '{identifier}' already exists.")

    warehouse = _resolve_warehouse(interactive and not force)

    dpm = DCMProjectManager()
    plan = _build_init_plan(
        dpm=dpm,
        identifier=identifier,
        target_name=target_name,
        append_target=append_target,
        account_identifier=account_identifier,
        project_owner=project_owner,
        warehouse=warehouse,
        manifest_exists=append_to_existing,
        is_new_project=is_new_project,
        existing_manifest_text=existing_manifest_text,
        project_exists=project_exists,
    )
    _confirm_init_plan(plan, target_dir, force=force, interactive=interactive)

    with cli_console.phase(f"Initializing DCM Project '{identifier}'"):
        _execute_init_plan(plan, target_dir, manifest_path, dpm)

    _print_init_next_steps(plan, target_dir)

    return MessageResult(f"Initialized DCM Project '{identifier}'.")


@app.command(requires_connection=True)
def create(
    identifier: Optional[FQN] = optional_dcm_identifier,
    if_not_exists: bool = IfNotExistsOption(
        help="Do nothing if the project already exists."
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Creates a DCM Project in Snowflake.
    """
    # `create` is the command where it's crucial to validate the project_owner upfront (validate_owner=True).
    #  It's the command that creates the DCM project object and the role used to execute the command will
    #  have OWNERSHIP privilege on the created DCM project object. If a different role, than the one specified
    #  in the manifest, the target was used, then it's almost certain that it was not intended, and it will
    #  impact who can use the project and how.
    context = _resolve_context_with_optional_manifest(
        from_location, identifier, target, validate_owner=True
    )
    project_id = context.project_identifier

    om = ObjectManager()
    if om.object_exists(object_type="dcm", fqn=project_id):
        message = f"DCM Project '{project_id}' already exists."
        log.info(
            "DCM project already exists during create (project_identifier=%s).",
            project_id,
        )
        if if_not_exists:
            return MessageResult(message)
        raise CliError(message)

    dpm = DCMProjectManager()
    with cli_console.phase(f"Creating DCM Project '{project_id}'"):
        dpm.create(project_identifier=project_id)

    return MessageResult(f"DCM Project '{project_id}' successfully created.")


@app.command(requires_connection=True)
def drop(
    identifier: Optional[FQN] = optional_dcm_identifier,
    if_exists: bool = IfExistsOption(help="Do nothing if the project does not exist."),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Drops a DCM Project. All the objects deployed and managed by this project won't be dropped.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    result = QueryResult(
        ObjectManager().drop(object_type="dcm", fqn=project_id, if_exists=if_exists)
    )
    log.info(
        "DCM project %s is deleted (if_exists).",
        project_id,
    )
    return result


@app.command(requires_connection=True)
def describe(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Provides description of a DCM Project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    return QueryResult(ObjectManager().describe(object_type="dcm", fqn=project_id))


@app.command(requires_connection=True)
def list_deployments(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Lists deployments of given DCM Project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    pm = DCMProjectManager()
    results = pm.list_deployments(project_identifier=project_id)
    return QueryResult(results)


@app.command(requires_connection=True)
def drop_deployment(
    identifier: Optional[FQN] = optional_dcm_identifier,
    deployment: str = typer.Option(
        ...,
        "--deployment",
        help="Name or alias of the deployment to drop. For names containing '$', use single quotes to prevent shell expansion (e.g., 'DEPLOYMENT$1'). If both the deployment name and the alias match two different deployments, the deployment name match has higher precedence.",
        show_default=False,
    ),
    if_exists: bool = IfExistsOption(
        help="Do nothing if the deployment does not exist."
    ),
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    **options,
):
    """
    Drops a deployment from the DCM Project.
    """
    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    # Detect potential shell expansion issues
    if deployment and deployment.upper() == "DEPLOYMENT":
        cli_console.warning(
            f"Deployment name '{deployment}' might be truncated due to shell expansion. "
            f"If you meant to use a deployment like 'DEPLOYMENT$1', try using single quotes: 'DEPLOYMENT$1'."
        )

    dpm = DCMProjectManager()
    dpm.drop_deployment(
        project_identifier=project_id,
        deployment_name=deployment,
        if_exists=if_exists,
    )
    log.info(
        "Dropped %s deployment from project %s.",
        deployment,
        project_id,
    )
    return MessageResult(
        f"Deployment '{deployment}' dropped from DCM Project '{project_id}'."
    )


@app.command(requires_connection=True)
def preview(
    identifier: Optional[FQN] = optional_dcm_identifier,
    object_identifier: FQN = typer.Option(
        ...,
        "--object",
        help="FQN of table/view/dynamic table to be previewed.",
        show_default=False,
        click_type=IdentifierType(),
    ),
    from_location: SecurePath = from_option,
    variables: Optional[List[str]] = variables_flag,
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        help="The maximum number of rows to be returned.",
        show_default=False,
    ),
    target: Optional[str] = target_option,
    **options,
):
    """
    Returns rows from any table, view, dynamic table.
    """
    context = _resolve_context_with_required_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    effective_stage = manager.sync_local_files(
        project_identifier=project_id,
        source_directory=str(from_location.path),
    )

    with cli_console.spinner() as spinner:
        spinner.add_task(
            description=f"Previewing {object_identifier}.",
            total=None,
        )
        result = manager.preview(
            project_identifier=project_id,
            object_identifier=object_identifier,
            configuration=context.configuration,
            from_stage=effective_stage,
            variables=variables,
            limit=limit,
        )

    return QueryResult(result)


@app.command(requires_connection=True)
@mock_dcm_response("refresh")
def refresh(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """
    Refreshes dynamic tables defined in DCM project. It applies only to deployed objects.
    """
    clear_command_artifacts("refresh")

    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    tracker = DeployProgressTracker(conn=manager.connection, operation="refresh")
    with tracker.session():
        result = tracker.run_loader_phase(
            lambda: manager.refresh(project_identifier=project_id),
            phase_name="REFRESH",
        )

    reporter = RefreshReporter(save_output=save_output)
    return reporter.process(result)


@app.command(requires_connection=True)
@mock_dcm_response("test")
def test(
    identifier: Optional[FQN] = optional_dcm_identifier,
    from_location: SecurePath = from_option,
    target: Optional[str] = target_option,
    save_output: bool = save_output_option,
    **options,
):
    """
    Tests all expectations defined in DCM project. It applies only to deployed objects.
    """
    clear_command_artifacts("test")

    context = _resolve_context_with_optional_manifest(from_location, identifier, target)
    project_id = context.project_identifier

    manager = DCMProjectManager()
    tracker = DeployProgressTracker(conn=manager.connection, operation="test")
    with tracker.session():
        result = tracker.run_loader_phase(
            lambda: manager.test(project_identifier=project_id),
            phase_name="TEST",
        )

    reporter = TestReporter(save_output=save_output)
    return reporter.process(result)
