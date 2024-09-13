from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Callable, List, Optional, TypedDict

import typer
from click import ClickException, UsageError
from snowflake.cli._plugins.nativeapp.common_flags import (
    ForceOption,
    InteractiveOption,
    ValidateOption,
)
from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    NAME_COL,
    OWNER_COL,
    PATCH_COL,
    SPECIAL_COMMENT,
    VERSION_COL,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageDoesNotExistError,
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
from snowflake.cli._plugins.nativeapp.utils import (
    needs_confirmation,
)
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.entities.utils import (
    drop_generic_object,
    ensure_correct_owner,
    execute_post_deploy_hooks,
    generic_sql_error_handler,
    print_messages,
)
from snowflake.cli.api.errno import (
    APPLICATION_NO_LONGER_AVAILABLE,
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    CANNOT_UPGRADE_FROM_VERSION_TO_LOOSE_FILES,
    NOT_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    ONLY_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.entities.application_entity_model import (
    ApplicationEntityModel,
)
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.util import (
    extract_schema,
    identifier_to_show_like_pattern,
    unquote_identifier,
)
from snowflake.cli.api.utils.cursor import find_all_rows
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

# Reasons why an `alter application ... upgrade` might fail
UPGRADE_RESTRICTION_CODES = {
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    CANNOT_UPGRADE_FROM_VERSION_TO_LOOSE_FILES,
    ONLY_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    NOT_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    APPLICATION_NO_LONGER_AVAILABLE,
}

ApplicationOwnedObject = TypedDict("ApplicationOwnedObject", {"name": str, "type": str})


class ApplicationEntity(EntityBase[ApplicationEntityModel]):
    """
    A Native App application object, created from an application package.
    """

    def action_deploy(
        self,
        ctx: ActionContext,
        from_release_directive: bool,
        prune: bool,
        recursive: bool,
        paths: List[Path],
        validate: bool = ValidateOption,
        stage_fqn: Optional[str] = None,
        interactive: bool = InteractiveOption,
        version: Optional[str] = None,
        patch: Optional[int] = None,
        force: Optional[bool] = ForceOption,
        *args,
        **kwargs,
    ):
        model = self._entity_model
        app_name = model.fqn.identifier
        debug_mode = model.debug
        if model.meta:
            app_role = getattr(model.meta, "role", ctx.default_role)
            app_warehouse = getattr(model.meta, "warehouse", ctx.default_warehouse)
            post_deploy_hooks = getattr(model.meta, "post_deploy", None)
        else:
            app_role = ctx.default_role
            app_warehouse = ctx.default_warehouse
            post_deploy_hooks = None

        package_entity: ApplicationPackageEntity = ctx.get_entity(model.from_.target)
        package_model: ApplicationPackageEntityModel = (
            package_entity._entity_model  # noqa: SLF001
        )
        package_name = package_model.fqn.identifier
        if package_model.meta and package_model.meta.role:
            package_role = package_model.meta.role
        else:
            package_role = ctx.default_role

        if not stage_fqn:
            stage_fqn = f"{package_name}.{package_model.stage}"
        stage_schema = extract_schema(stage_fqn)

        is_interactive = False
        if force:
            policy = AllowAlwaysPolicy()
        elif interactive:
            is_interactive = True
            policy = AskAlwaysPolicy()
        else:
            policy = DenyAlwaysPolicy()

        def deploy_package():
            package_entity.action_deploy(
                ctx=ctx,
                prune=True,
                recursive=True,
                paths=[],
                validate=validate,
                stage_fqn=stage_fqn,
            )

        self.deploy(
            console=ctx.console,
            project_root=ctx.project_root,
            app_name=app_name,
            app_role=app_role,
            app_warehouse=app_warehouse,
            package_name=package_name,
            package_role=package_role,
            stage_schema=stage_schema,
            stage_fqn=stage_fqn,
            debug_mode=debug_mode,
            validate=validate,
            from_release_directive=from_release_directive,
            is_interactive=is_interactive,
            policy=policy,
            version=version,
            patch=patch,
            post_deploy_hooks=post_deploy_hooks,
            deploy_package=deploy_package,
        )

    def action_drop(
        self,
        ctx: ActionContext,
        interactive: bool,
        force_drop: bool = False,
        cascade: Optional[bool] = None,
        *args,
        **kwargs,
    ):
        model = self._entity_model
        app_name = model.fqn.identifier
        if model.meta and model.meta.role:
            app_role = model.meta.role
        else:
            app_role = ctx.default_role
        self.drop(
            console=ctx.console,
            app_name=app_name,
            app_role=app_role,
            auto_yes=force_drop,
            interactive=interactive,
            cascade=cascade,
        )

    @classmethod
    def drop(
        cls,
        console: AbstractConsole,
        app_name: str,
        app_role: str,
        auto_yes: bool,
        interactive: bool = False,
        cascade: Optional[bool] = None,
    ):
        """
        Attempts to drop the application object if all validations and user prompts allow so.
        """

        needs_confirm = True

        # 1. If existing application is not found, exit gracefully
        show_obj_row = cls.get_existing_app_info(
            app_name=app_name,
            app_role=app_role,
        )
        if show_obj_row is None:
            console.warning(
                f"Role {app_role} does not own any application object with the name {app_name}, or the application object does not exist."
            )
            return

        # 2. Check for the right owner
        ensure_correct_owner(row=show_obj_row, role=app_role, obj_name=app_name)

        # 3. Check if created by the Snowflake CLI
        row_comment = show_obj_row[COMMENT_COL]
        if row_comment not in ALLOWED_SPECIAL_COMMENTS and needs_confirmation(
            needs_confirm, auto_yes
        ):
            should_drop_object = typer.confirm(
                dedent(
                    f"""\
                        Application object {app_name} was not created by Snowflake CLI.
                        Application object details:
                        Name: {app_name}
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
                console.message(f"Did not drop application object {app_name}.")
                # The user desires to keep the app, therefore we can't proceed since it would
                # leave behind an orphan app when we get to dropping the package
                raise typer.Abort()

        # 4. Check for application objects owned by the application
        # This query will fail if the application package has already been dropped, so handle this case gracefully
        has_objects_to_drop = False
        message_prefix = ""
        cascade_true_message = ""
        cascade_false_message = ""
        interactive_prompt = ""
        non_interactive_abort = ""
        try:
            if application_objects := cls.get_objects_owned_by_application(
                app_name=app_name,
                app_role=app_role,
            ):
                has_objects_to_drop = True
                message_prefix = (
                    f"The following objects are owned by application {app_name}"
                )
                cascade_true_message = f"{message_prefix} and will be dropped:"
                cascade_false_message = f"{message_prefix} and will NOT be dropped:"
                interactive_prompt = "Would you like to drop these objects in addition to the application? [y/n/ABORT]"
                non_interactive_abort = "Re-run teardown again with --cascade or --no-cascade to specify whether these objects should be dropped along with the application"
        except ProgrammingError as e:
            if e.errno != APPLICATION_NO_LONGER_AVAILABLE:
                raise
            application_objects = []
            message_prefix = (
                f"Could not determine which objects are owned by application {app_name}"
            )
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
                console.message(cascade_true_message)
                with console.indented():
                    for obj in application_objects:
                        console.message(cls.application_object_to_str(obj))
            elif cascade is False:
                # If the user explicitly passed the --no-cascade flag
                console.message(cascade_false_message)
                with console.indented():
                    for obj in application_objects:
                        console.message(cls.application_object_to_str(obj))
            elif interactive:
                # If the user didn't pass any cascade flag and the session is interactive
                console.message(message_prefix)
                with console.indented():
                    for obj in application_objects:
                        console.message(cls.application_object_to_str(obj))
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
                console.message(message_prefix)
                with console.indented():
                    for obj in application_objects:
                        console.message(cls.application_object_to_str(obj))
                console.message(non_interactive_abort)
                raise typer.Abort()
        elif cascade is None:
            # If there's nothing to drop, set cascade to an explicit False value
            cascade = False

        # 5. All validations have passed, drop object
        drop_generic_object(
            console=console,
            object_type="application",
            object_name=app_name,
            role=app_role,
            cascade=cascade,
        )
        return  # The application object was successfully dropped, therefore exit gracefully

    @staticmethod
    def get_objects_owned_by_application(
        app_name: str,
        app_role: str,
    ) -> List[ApplicationOwnedObject]:
        """
        Returns all application objects owned by this application.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(app_role):
            results = sql_executor.execute_query(
                f"show objects owned by application {app_name}"
            ).fetchall()
            return [{"name": row[1], "type": row[2]} for row in results]

    @classmethod
    def application_objects_to_str(
        cls, application_objects: list[ApplicationOwnedObject]
    ) -> str:
        """
        Returns a list in an "(Object Type) Object Name" format. Database-level and schema-level object names are fully qualified:
        (COMPUTE_POOL) POOL_NAME
        (DATABASE) DB_NAME
        (SCHEMA) DB_NAME.PUBLIC
        ...
        """
        return "\n".join(
            [cls.application_object_to_str(obj) for obj in application_objects]
        )

    @staticmethod
    def application_object_to_str(obj: ApplicationOwnedObject) -> str:
        return f"({obj['type']}) {obj['name']}"

    @classmethod
    def deploy(
        cls,
        console: AbstractConsole,
        project_root: Path,
        app_name: str,
        app_role: str,
        app_warehouse: str,
        package_name: str,
        package_role: str,
        stage_schema: str,
        stage_fqn: str,
        debug_mode: bool,
        validate: bool,
        from_release_directive: bool,
        is_interactive: bool,
        policy: PolicyBase,
        deploy_package: Callable,
        version: Optional[str] = None,
        patch: Optional[int] = None,
        post_deploy_hooks: Optional[List[PostDeployHook]] = None,
        drop_application_before_upgrade: Optional[Callable] = None,
    ):
        """
        Create or upgrade the application object using the given strategy
        (unversioned dev, versioned dev, or same-account release directive).
        """

        # same-account release directive
        if from_release_directive:
            cls.create_or_upgrade_app(
                console=console,
                project_root=project_root,
                package_name=package_name,
                package_role=package_role,
                app_name=app_name,
                app_role=app_role,
                app_warehouse=app_warehouse,
                stage_schema=stage_schema,
                stage_fqn=stage_fqn,
                debug_mode=debug_mode,
                policy=policy,
                install_method=SameAccountInstallMethod.release_directive(),
                is_interactive=is_interactive,
                post_deploy_hooks=post_deploy_hooks,
                drop_application_before_upgrade=drop_application_before_upgrade,
            )
            return

        # versioned dev
        if version:
            try:
                version_exists = cls.get_existing_version_info(
                    version=version,
                    package_name=package_name,
                    package_role=package_role,
                )
                if not version_exists:
                    raise UsageError(
                        f"Application package {package_name} does not have any version {version} defined. Use 'snow app version create' to define a version in the application package first."
                    )
            except ApplicationPackageDoesNotExistError as app_err:
                raise UsageError(
                    f"Application package {package_name} does not exist. Use 'snow app version create' to first create an application package and then define a version in it."
                )

            cls.create_or_upgrade_app(
                console=console,
                project_root=project_root,
                package_name=package_name,
                package_role=package_role,
                app_name=app_name,
                app_role=app_role,
                app_warehouse=app_warehouse,
                stage_schema=stage_schema,
                stage_fqn=stage_fqn,
                debug_mode=debug_mode,
                policy=policy,
                install_method=SameAccountInstallMethod.versioned_dev(version, patch),
                is_interactive=is_interactive,
                post_deploy_hooks=post_deploy_hooks,
                drop_application_before_upgrade=drop_application_before_upgrade,
            )
            return

        # unversioned dev
        deploy_package()
        cls.create_or_upgrade_app(
            console=console,
            project_root=project_root,
            package_name=package_name,
            package_role=package_role,
            app_name=app_name,
            app_role=app_role,
            app_warehouse=app_warehouse,
            stage_schema=stage_schema,
            stage_fqn=stage_fqn,
            debug_mode=debug_mode,
            policy=policy,
            install_method=SameAccountInstallMethod.unversioned_dev(),
            is_interactive=is_interactive,
            post_deploy_hooks=post_deploy_hooks,
            drop_application_before_upgrade=drop_application_before_upgrade,
        )

    @classmethod
    def create_or_upgrade_app(
        cls,
        console: AbstractConsole,
        project_root: Path,
        package_name: str,
        package_role: str,
        app_name: str,
        app_role: str,
        app_warehouse: Optional[str],
        stage_schema: Optional[str],
        stage_fqn: str,
        debug_mode: bool,
        policy: PolicyBase,
        install_method: SameAccountInstallMethod,
        is_interactive: bool = False,
        post_deploy_hooks: Optional[List[PostDeployHook]] = None,
        drop_application_before_upgrade: Optional[Callable] = None,
    ):
        sql_executor = get_sql_executor()
        with sql_executor.use_role(app_role):

            # 1. Need to use a warehouse to create an application object
            with sql_executor.use_warehouse(app_warehouse):

                # 2. Check for an existing application by the same name
                show_app_row = cls.get_existing_app_info(
                    app_name=app_name,
                    app_role=app_role,
                )

                # 3. If existing application is found, perform a few validations and upgrade the application object.
                if show_app_row:

                    install_method.ensure_app_usable(
                        app_name=app_name,
                        app_role=app_role,
                        show_app_row=show_app_row,
                    )

                    # If all the above checks are in order, proceed to upgrade
                    try:
                        console.step(
                            f"Upgrading existing application object {app_name}."
                        )
                        using_clause = install_method.using_clause(stage_fqn)
                        upgrade_cursor = sql_executor.execute_query(
                            f"alter application {app_name} upgrade {using_clause}",
                        )
                        print_messages(console, upgrade_cursor)

                        if install_method.is_dev_mode:
                            # if debug_mode is present (controlled), ensure it is up-to-date
                            if debug_mode is not None:
                                sql_executor.execute_query(
                                    f"alter application {app_name} set debug_mode = {debug_mode}"
                                )

                        # hooks always executed after a create or upgrade
                        if post_deploy_hooks:
                            cls.execute_post_deploy_hooks(
                                console=console,
                                project_root=project_root,
                                post_deploy_hooks=post_deploy_hooks,
                                app_name=app_name,
                                app_warehouse=app_warehouse,
                            )
                        return

                    except ProgrammingError as err:
                        if err.errno not in UPGRADE_RESTRICTION_CODES:
                            generic_sql_error_handler(err=err)
                        else:  # The existing application object was created from a different process.
                            console.warning(err.msg)
                            # TODO Drop the entity here instead of taking a callback once action_drop() is implemented
                            if drop_application_before_upgrade:
                                drop_application_before_upgrade()
                            else:
                                raise NotImplementedError

                # 4. With no (more) existing application objects, create an application object using the release directives
                console.step(f"Creating new application object {app_name} in account.")

                if app_role != package_role:
                    with sql_executor.use_role(package_role):
                        sql_executor.execute_query(
                            f"grant install, develop on application package {package_name} to role {app_role}"
                        )
                        sql_executor.execute_query(
                            f"grant usage on schema {package_name}.{stage_schema} to role {app_role}"
                        )
                        sql_executor.execute_query(
                            f"grant read on stage {stage_fqn} to role {app_role}"
                        )

                try:
                    # by default, applications are created in debug mode when possible;
                    # this can be overridden in the project definition
                    debug_mode_clause = ""
                    if install_method.is_dev_mode:
                        initial_debug_mode = (
                            debug_mode if debug_mode is not None else True
                        )
                        debug_mode_clause = f"debug_mode = {initial_debug_mode}"

                    using_clause = install_method.using_clause(stage_fqn)
                    create_cursor = sql_executor.execute_query(
                        dedent(
                            f"""\
                        create application {app_name}
                            from application package {package_name} {using_clause} {debug_mode_clause}
                            comment = {SPECIAL_COMMENT}
                        """
                        ),
                    )
                    print_messages(console, create_cursor)

                    # hooks always executed after a create or upgrade
                    if post_deploy_hooks:
                        cls.execute_post_deploy_hooks(
                            console=console,
                            project_root=project_root,
                            post_deploy_hooks=post_deploy_hooks,
                            app_name=app_name,
                            app_warehouse=app_warehouse,
                        )

                except ProgrammingError as err:
                    generic_sql_error_handler(err)

    @classmethod
    def execute_post_deploy_hooks(
        cls,
        console: AbstractConsole,
        project_root: Path,
        post_deploy_hooks: Optional[List[PostDeployHook]],
        app_name: str,
        app_warehouse: Optional[str],
    ):
        with cls.use_application_warehouse(app_warehouse):
            execute_post_deploy_hooks(
                console=console,
                project_root=project_root,
                post_deploy_hooks=post_deploy_hooks,
                deployed_object_type="application",
                database_name=app_name,
            )

    @staticmethod
    @contextmanager
    def use_application_warehouse(
        app_warehouse: Optional[str],
    ):
        if app_warehouse:
            with get_sql_executor().use_warehouse(app_warehouse):
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

    @staticmethod
    def get_existing_app_info(
        app_name: str,
        app_role: str,
    ) -> Optional[dict]:
        """
        Check for an existing application object by the same name as in project definition, in account.
        It executes a 'show applications like' query and returns the result as single row, if one exists.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(app_role):
            return sql_executor.show_specific_object(
                "applications", app_name, name_col=NAME_COL
            )

    @staticmethod
    def get_existing_version_info(
        version: str,
        package_name: str,
        package_role: str,
    ) -> Optional[dict]:
        """
        Get the latest patch on an existing version by name in the application package.
        Executes 'show versions like ... in application package' query and returns
        the latest patch in the version as a single row, if one exists. Otherwise,
        returns None.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            try:
                query = f"show versions like {identifier_to_show_like_pattern(version)} in application package {package_name}"
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
                    raise ApplicationPackageDoesNotExistError(package_name)
                else:
                    generic_sql_error_handler(err=err, role=package_role)
                    return None
