from textwrap import dedent
from typing import Callable, Optional

from snowflake.cli._plugins.nativeapp.constants import (
    NAME_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.policy import PolicyBase
from snowflake.cli._plugins.nativeapp.same_account_install_method import (
    SameAccountInstallMethod,
)
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.entities.utils import (
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
from snowflake.cli.api.project.schemas.entities.application_entity_model import (
    ApplicationEntityModel,
)
from snowflake.connector import ProgrammingError

# Reasons why an `alter application ... upgrade` might fail
UPGRADE_RESTRICTION_CODES = {
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    CANNOT_UPGRADE_FROM_VERSION_TO_LOOSE_FILES,
    ONLY_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    NOT_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    APPLICATION_NO_LONGER_AVAILABLE,
}


class ApplicationEntity(EntityBase[ApplicationEntityModel]):
    """
    A Native App application object, created from an application package.
    """

    def action_deploy(
        self,
        ctx: ActionContext,
        *args,
        **kwargs,
    ):
        # TODO
        pass

    @classmethod
    def create_or_upgrade_app(
        cls,
        console: AbstractConsole,
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
                        # TODO Execute app post deploy hooks
                        return

                    except ProgrammingError as err:
                        if err.errno not in UPGRADE_RESTRICTION_CODES:
                            generic_sql_error_handler(err=err)
                        else:  # The existing application object was created from a different process.
                            console.warning(err.msg)
                            # TODO
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
                    # TODO Execute app post deploy hooks

                except ProgrammingError as err:
                    generic_sql_error_handler(err)

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
