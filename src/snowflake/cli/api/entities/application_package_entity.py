from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    INTERNAL_DISTRIBUTION,
    NAME_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
)
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.entities.utils import (
    ensure_correct_owner,
    generic_sql_error_handler,
    render_script_templates,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.rendering.jinja import (
    jinja_render_from_str,
)
from snowflake.connector import ProgrammingError


class ApplicationPackageEntity(EntityBase[ApplicationPackageEntityModel]):
    """
    A Native App application package.
    """

    def action_bundle(self, ctx: ActionContext):
        model = self._entity_model
        bundle_map = build_bundle(
            ctx.project_root, Path(model.deploy_root), model.artifacts
        )
        bundle_context = BundleContext(
            package_name=model.identifier,
            artifacts=model.artifacts,
            project_root=ctx.project_root,
            bundle_root=Path(model.bundle_root),
            deploy_root=Path(model.deploy_root),
            generated_root=Path(model.generated_root),
        )
        compiler = NativeAppCompiler(bundle_context)
        compiler.compile_artifacts()
        return bundle_map

    @staticmethod
    def get_existing_app_pkg_info(
        package_name: str,
        package_role: str,
    ) -> Optional[dict]:
        """
        Check for an existing application package by the same name as in project definition, in account.
        It executes a 'show application packages like' query and returns the result as single row, if one exists.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            return sql_executor.show_specific_object(
                "application packages", package_name, name_col=NAME_COL
            )

    @staticmethod
    def get_app_pkg_distribution_in_snowflake(
        package_name: str,
        package_role: str,
    ) -> str:
        """
        Returns the 'distribution' attribute of a 'describe application package' SQL query, in lowercase.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            try:
                desc_cursor = sql_executor.execute_query(
                    f"describe application package {package_name}"
                )
            except ProgrammingError as err:
                generic_sql_error_handler(err)

            if desc_cursor.rowcount is None or desc_cursor.rowcount == 0:
                raise SnowflakeSQLExecutionError()
            else:
                for row in desc_cursor:
                    if row[0].lower() == "distribution":
                        return row[1].lower()
        raise ProgrammingError(
            msg=dedent(
                f"""\
                Could not find the 'distribution' attribute for application package {package_name} in the output of SQL query:
                'describe application package {package_name}'
                """
            )
        )

    @classmethod
    def verify_project_distribution(
        cls,
        console: AbstractConsole,
        package_name: str,
        package_role: str,
        package_distribution: str,
        expected_distribution: Optional[str] = None,
    ) -> bool:
        """
        Returns true if the 'distribution' attribute of an existing application package in snowflake
        is the same as the the attribute specified in project definition file.
        """
        actual_distribution = (
            expected_distribution
            if expected_distribution
            else cls.get_app_pkg_distribution_in_snowflake(
                package_name=package_name,
                package_role=package_role,
            )
        )
        project_def_distribution = package_distribution.lower()
        if actual_distribution != project_def_distribution:
            console.warning(
                dedent(
                    f"""\
                    Application package {package_name} in your Snowflake account has distribution property {actual_distribution},
                    which does not match the value specified in project definition file: {project_def_distribution}.
                    """
                )
            )
            return False
        return True

    @staticmethod
    @contextmanager
    def use_package_warehouse(
        package_warehouse: Optional[str],
    ):
        if package_warehouse:
            with get_sql_executor().use_warehouse(package_warehouse):
                yield
        else:
            raise ClickException(
                dedent(
                    f"""\
                Application package warehouse cannot be empty.
                Please provide a value for it in your connection information or your project definition file.
                """
                )
            )

    @classmethod
    def apply_package_scripts(
        cls,
        console: AbstractConsole,
        package_scripts: List[str],
        package_warehouse: Optional[str],
        project_root: Path,
        package_role: str,
        package_name: str,
    ) -> None:
        """
        Assuming the application package exists and we are using the correct role,
        applies all package scripts in-order to the application package.
        """

        if package_scripts:
            console.warning(
                "WARNING: native_app.package.scripts is deprecated. Please migrate to using native_app.package.post_deploy."
            )

        queued_queries = render_script_templates(
            project_root,
            jinja_render_from_str,
            dict(package_name=package_name),
            package_scripts,
        )

        # once we're sure all the templates expanded correctly, execute all of them
        with cls.use_package_warehouse(
            package_warehouse=package_warehouse,
        ):
            try:
                for i, queries in enumerate(queued_queries):
                    console.step(f"Applying package script: {package_scripts[i]}")
                    get_sql_executor().execute_queries(queries)
            except ProgrammingError as err:
                generic_sql_error_handler(
                    err, role=package_role, warehouse=package_warehouse
                )

    @classmethod
    def create_app_package(
        cls,
        console: AbstractConsole,
        package_name: str,
        package_role: str,
        package_distribution: str,
    ) -> None:
        """
        Creates the application package with our up-to-date stage if none exists.
        """

        # 1. Check for existing existing application package
        show_obj_row = cls.get_existing_app_pkg_info(
            package_name=package_name,
            package_role=package_role,
        )

        if show_obj_row:
            # 1. Check for the right owner role
            ensure_correct_owner(
                row=show_obj_row, role=package_role, obj_name=package_name
            )

            # 2. Check distribution of the existing application package
            actual_distribution = cls.get_app_pkg_distribution_in_snowflake(
                package_name=package_name,
                package_role=package_role,
            )
            if not cls.verify_project_distribution(
                console=console,
                package_name=package_name,
                package_role=package_role,
                package_distribution=package_distribution,
                expected_distribution=actual_distribution,
            ):
                console.warning(
                    f"Continuing to execute `snow app run` on application package {package_name} with distribution '{actual_distribution}'."
                )

            # 3. If actual_distribution is external, skip comment check
            if actual_distribution == INTERNAL_DISTRIBUTION:
                row_comment = show_obj_row[COMMENT_COL]

                if row_comment not in ALLOWED_SPECIAL_COMMENTS:
                    raise ApplicationPackageAlreadyExistsError(package_name)

            return

        # If no application package pre-exists, create an application package, with the specified distribution in the project definition file.
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            console.step(f"Creating new application package {package_name} in account.")
            sql_executor.execute_query(
                dedent(
                    f"""\
                    create application package {package_name}
                        comment = {SPECIAL_COMMENT}
                        distribution = {package_distribution}
                """
                )
            )
