from textwrap import dedent
from typing import Optional

import jinja2
from snowcli.cli.nativeapp.constants import (
    COMMENT_COL,
    INTERNAL_DISTRIBUTION,
    LOOSE_FILES_MAGIC_VERSION,
    SPECIAL_COMMENT,
    VERSION_COL,
)
from snowcli.cli.nativeapp.exceptions import (
    ApplicationAlreadyExistsError,
    ApplicationPackageAlreadyExistsError,
    InvalidPackageScriptError,
    MissingPackageScriptError,
)
from snowcli.cli.nativeapp.manager import (
    NativeAppCommandProcessor,
    NativeAppManager,
    _generic_sql_error_handler,
    is_correct_owner,
    log,
)
from snowcli.cli.object.stage.diff import DiffResult
from snowcli.cli.object.stage.manager import StageManager
from snowflake.connector import ProgrammingError


class NativeAppRunProcessor(NativeAppManager, NativeAppCommandProcessor):
    def __init__(self, search_path: Optional[str] = None):
        super().__init__(search_path)

    def create_app_package(self) -> None:
        """
        Creates the application package with our up-to-date stage if none exists.
        """

        # 1. Check for existing existing application package
        show_obj_row = self.get_existing_app_pkg_info()

        if show_obj_row is not None:
            # 1. Check for the right owner role
            is_correct_owner(
                row=show_obj_row, role=self.package_role, obj_name=self.package_name
            )

            # 2. Check distribution of the existing app package
            actual_distribution = self.get_app_pkg_distribution_in_snowflake
            if not self.is_app_pkg_distribution_same_in_sf():
                log.warning(
                    f"Continuing to execute `snow app run` on app pkg {self.package_name} with distribution '{actual_distribution}'."
                )

            # 3. If actual_distribution is external, skip comment check
            if actual_distribution == INTERNAL_DISTRIBUTION:
                row_comment = show_obj_row[COMMENT_COL]

                if row_comment != SPECIAL_COMMENT:
                    raise ApplicationPackageAlreadyExistsError(self.package_name)

            return

        # If no app pkg pre-exists, create an app pkg, with the specified distribution in the project definition file.
        with self.use_role(self.package_role):
            log.info(
                f"Creating new application package {self.package_name} in account."
            )
            self._execute_query(
                dedent(
                    f"""\
                    create application package {self.package_name}
                        comment = {SPECIAL_COMMENT}
                        distribution = {self.package_distribution}
                """
                )
            )

    def _apply_package_scripts(self) -> None:
        """
        Assuming the application package exists and we are using the correct role,
        applies all package scripts in-order to the application package.
        """
        env = jinja2.Environment(
            loader=jinja2.loaders.FileSystemLoader(self.project_root),
            keep_trailing_newline=True,
            undefined=jinja2.StrictUndefined,
        )

        queued_queries = []
        for relpath in self.package_scripts:
            try:
                template = env.get_template(relpath)
                result = template.render(dict(package_name=self.package_name))
                queued_queries.append(result)

            except jinja2.TemplateNotFound as e:
                raise MissingPackageScriptError(e.name)

            except jinja2.TemplateSyntaxError as e:
                raise InvalidPackageScriptError(e.name, e)

            except jinja2.UndefinedError as e:
                raise InvalidPackageScriptError(relpath, e)

        # once we're sure all the templates expanded correctly, execute all of them
        try:
            if self.package_warehouse:
                self._execute_query(f"use warehouse {self.package_warehouse}")

            for i, queries in enumerate(queued_queries):
                log.info(f"Applying package script: {self.package_scripts[i]}")
                self._execute_queries(queries)
        except ProgrammingError as err:
            _generic_sql_error_handler(
                err, role=self.package_role, warehouse=self.package_warehouse
            )

    def _create_dev_app(self, diff: DiffResult) -> None:
        """
        (Re-)creates the application with our up-to-date stage.
        """
        with self.use_role(self.app_role):

            # 1. Need to use a warehouse to create an application instance
            try:
                if self.application_warehouse:
                    self._execute_query(f"use warehouse {self.application_warehouse}")
            except ProgrammingError as err:
                _generic_sql_error_handler(
                    err=err, role=self.app_role, warehouse=self.application_warehouse
                )

            # 2. Check for an existing application by the same name
            show_app_row = self.get_existing_app_info()

            # 3. If existing application is found, perform a few validations and upgrade the instance.
            if show_app_row is not None:

                # Check if not created by snowCLI or not created using "loose files" / stage dev mode.
                if show_app_row[COMMENT_COL] != SPECIAL_COMMENT or (
                    show_app_row[VERSION_COL] != LOOSE_FILES_MAGIC_VERSION
                ):
                    raise ApplicationAlreadyExistsError(self.app_name)

                # Check for the right owner
                is_correct_owner(
                    row=show_app_row, role=self.app_role, obj_name=self.app_name
                )

                # If all the above checks are in order, proceed to upgrade
                try:
                    if diff.has_changes():
                        log.info(f"Upgrading existing application {self.app_name}.")
                        self._execute_query(
                            f"alter application {self.app_name} upgrade using @{self.stage_fqn}"
                        )

                    # ensure debug_mode is up-to-date
                    self._execute_query(
                        f"alter application {self.app_name} set debug_mode = {self.debug_mode}"
                    )

                    return

                except ProgrammingError as err:
                    _generic_sql_error_handler(err)

            # 4. If no existing application is found, create an app using "loose files" / stage dev mode.
            log.info(f"Creating new application {self.app_name} in account.")

            if self.app_role != self.package_role:
                with self.use_role(new_role=self.package_role):
                    self._execute_queries(
                        dedent(
                            f"""\
                        grant install, develop on application package {self.package_name} to role {self.app_role};
                        grant usage on schema {self.package_name}.{self.stage_schema} to role {self.app_role};
                        grant read on stage {self.stage_fqn} to role {self.app_role};
                        """
                        )
                    )

            stage_name = StageManager.quote_stage_name(self.stage_fqn)

            try:
                self._execute_query(
                    dedent(
                        f"""\
                    create application {self.app_name}
                        from application package {self.package_name}
                        using {stage_name}
                        debug_mode = {self.debug_mode}
                        comment = {SPECIAL_COMMENT}
                    """
                    )
                )
            except ProgrammingError as err:
                _generic_sql_error_handler(err)

    def process(self, *args, **kwargs):
        """app run process"""

        # 1. Create an empty application package, if none exists
        self.create_app_package()

        with self.use_role(self.package_role):
            # 2. now that the application package exists, create shared data
            self._apply_package_scripts()

            # 3. Upload files from deploy root local folder to the above stage
            diff = self.sync_deploy_root_with_stage(self.package_role)

        # 4. Create an application if none exists, else upgrade the application
        self._create_dev_app(diff)
