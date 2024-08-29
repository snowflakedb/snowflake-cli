import json
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Callable, List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    EXTERNAL_DISTRIBUTION,
    INTERNAL_DISTRIBUTION,
    NAME_COL,
    OWNER_COL,
    SPECIAL_COMMENT,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
    CouldNotDropApplicationPackageWithVersions,
    SetupScriptFailedValidation,
)
from snowflake.cli._plugins.nativeapp.utils import (
    needs_confirmation,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.entities.utils import (
    drop_generic_object,
    ensure_correct_owner,
    execute_post_deploy_hooks,
    generic_sql_error_handler,
    render_script_templates,
    sync_deploy_root_with_stage,
    validation_item_to_str,
)
from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
)
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.util import extract_schema
from snowflake.cli.api.rendering.jinja import (
    get_basic_jinja_env,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor


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

    def action_deploy(
        self,
        ctx: ActionContext,
        prune: bool,
        recursive: bool,
        paths: List[Path],
        validate: bool,
    ):
        model = self._entity_model
        package_name = model.fqn.identifier
        if model.meta and model.meta.role:
            package_role = model.meta.role
        else:
            package_role = ctx.default_role

        # 1. Create a bundle
        bundle_map = self.action_bundle(ctx)

        # 2. Create an empty application package, if none exists
        self.create_app_package(
            console=ctx.console,
            package_name=package_name,
            package_role=package_role,
            package_distribution=model.distribution,
        )

        with get_sql_executor().use_role(package_role):
            # 3. Upload files from deploy root local folder to the above stage
            stage_fqn = f"{package_name}.{model.stage}"
            stage_schema = extract_schema(stage_fqn)
            sync_deploy_root_with_stage(
                console=ctx.console,
                deploy_root=Path(model.deploy_root),
                package_name=package_name,
                stage_schema=stage_schema,
                bundle_map=bundle_map,
                role=package_role,
                prune=prune,
                recursive=recursive,
                stage_fqn=stage_fqn,
                local_paths_to_sync=paths,
                print_diff=True,
            )

        if model.meta and model.meta.post_deploy:
            self.execute_post_deploy_hooks(
                console=ctx.console,
                project_root=ctx.project_root,
                post_deploy_hooks=model.meta.post_deploy,
                package_name=package_name,
                package_warehouse=model.meta.warehouse or ctx.default_warehouse,
            )

        if validate:
            self.validate_setup_script(
                console=ctx.console,
                package_name=package_name,
                package_role=package_role,
                stage_fqn=stage_fqn,
                use_scratch_stage=False,
                scratch_stage_fqn="",
                deploy_to_scratch_stage_fn=lambda *args: None,
            )

    def action_drop(
        self,
        ctx: ActionContext,
        force_drop: bool,
    ):
        model = self._entity_model
        package_name = model.fqn.identifier
        if model.meta and model.meta.role:
            package_role = model.meta.role
        else:
            package_role = ctx.default_role

        self.drop(
            console=ctx.console,
            package_name=package_name,
            package_role=package_role,
            force_drop=force_drop,
        )

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
            dict(package_name=package_name),
            package_scripts,
            get_basic_jinja_env(),
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

    @classmethod
    def execute_post_deploy_hooks(
        cls,
        console: AbstractConsole,
        project_root: Path,
        post_deploy_hooks: Optional[List[PostDeployHook]],
        package_name: str,
        package_warehouse: Optional[str],
    ):
        with cls.use_package_warehouse(package_warehouse):
            execute_post_deploy_hooks(
                console=console,
                project_root=project_root,
                post_deploy_hooks=post_deploy_hooks,
                deployed_object_type="application package",
                database_name=package_name,
            )

    @classmethod
    def validate_setup_script(
        cls,
        console: AbstractConsole,
        package_name: str,
        package_role: str,
        stage_fqn: str,
        use_scratch_stage: bool,
        scratch_stage_fqn: str,
        deploy_to_scratch_stage_fn: Callable,
    ):
        """Validates Native App setup script SQL."""
        with console.phase(f"Validating Snowflake Native App setup script."):
            validation_result = cls.get_validation_result(
                console=console,
                package_name=package_name,
                package_role=package_role,
                stage_fqn=stage_fqn,
                use_scratch_stage=use_scratch_stage,
                scratch_stage_fqn=scratch_stage_fqn,
                deploy_to_scratch_stage_fn=deploy_to_scratch_stage_fn,
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

    @staticmethod
    def get_validation_result(
        console: AbstractConsole,
        package_name: str,
        package_role: str,
        stage_fqn: str,
        use_scratch_stage: bool,
        scratch_stage_fqn: str,
        deploy_to_scratch_stage_fn: Callable,
    ):
        """Call system$validate_native_app_setup() to validate deployed Native App setup script."""
        if use_scratch_stage:
            stage_fqn = scratch_stage_fqn
            deploy_to_scratch_stage_fn()
        prefixed_stage_fqn = StageManager.get_standard_stage_prefix(stage_fqn)
        sql_executor = get_sql_executor()
        try:
            cursor = sql_executor.execute_query(
                f"call system$validate_native_app_setup('{prefixed_stage_fqn}')"
            )
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                raise ApplicationPackageDoesNotExistError(package_name)
            generic_sql_error_handler(err)
        else:
            if not cursor.rowcount:
                raise SnowflakeSQLExecutionError()
            return json.loads(cursor.fetchone()[0])
        finally:
            if use_scratch_stage:
                console.step(f"Dropping stage {scratch_stage_fqn}.")
                with sql_executor.use_role(package_role):
                    sql_executor.execute_query(
                        f"drop stage if exists {scratch_stage_fqn}"
                    )

    @classmethod
    def drop(
        cls,
        console: AbstractConsole,
        package_name: str,
        package_role: str,
        force_drop: bool,
    ):
        sql_executor = get_sql_executor()
        needs_confirm = True

        # 1. If existing application package is not found, exit gracefully
        show_obj_row = cls.get_existing_app_pkg_info(
            package_name=package_name,
            package_role=package_role,
        )
        if show_obj_row is None:
            console.warning(
                f"Role {package_role} does not own any application package with the name {package_name}, or the application package does not exist."
            )
            return

        # 2. Check for the right owner
        ensure_correct_owner(row=show_obj_row, role=package_role, obj_name=package_name)

        with sql_executor.use_role(package_role):
            # 3. Check for versions in the application package
            show_versions_query = f"show versions in application package {package_name}"
            show_versions_cursor = sql_executor.execute_query(
                show_versions_query, cursor_class=DictCursor
            )
            if show_versions_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_versions_query)

            if show_versions_cursor.rowcount > 0:
                # allow dropping a package with versions when --force is set
                if not force_drop:
                    raise CouldNotDropApplicationPackageWithVersions(
                        "Drop versions first, or use --force to override."
                    )

        # 4. Check distribution of the existing application package
        actual_distribution = cls.get_app_pkg_distribution_in_snowflake(
            package_name=package_name,
            package_role=package_role,
        )
        if not cls.verify_project_distribution(
            console=console,
            package_name=package_name,
            package_role=package_role,
            package_distribution=actual_distribution,
        ):
            console.warning(
                f"Dropping application package {package_name} with distribution '{actual_distribution}'."
            )

        # 5. If distribution is internal, check if created by the Snowflake CLI
        row_comment = show_obj_row[COMMENT_COL]
        if actual_distribution == INTERNAL_DISTRIBUTION:
            if row_comment in ALLOWED_SPECIAL_COMMENTS:
                needs_confirm = False
            else:
                if needs_confirmation(needs_confirm, force_drop):
                    console.warning(
                        f"Application package {package_name} was not created by Snowflake CLI."
                    )
        else:
            if needs_confirmation(needs_confirm, force_drop):
                console.warning(
                    f"Application package {package_name} in your Snowflake account has distribution property '{EXTERNAL_DISTRIBUTION}' and could be associated with one or more of your listings on Snowflake Marketplace."
                )

        if needs_confirmation(needs_confirm, force_drop):
            should_drop_object = typer.confirm(
                dedent(
                    f"""\
                        Application package details:
                        Name: {package_name}
                        Created on: {show_obj_row["created_on"]}
                        Distribution: {actual_distribution}
                        Owner: {show_obj_row[OWNER_COL]}
                        Comment: {show_obj_row[COMMENT_COL]}
                        Are you sure you want to drop it?
                    """
                )
            )
            if not should_drop_object:
                console.message(f"Did not drop application package {package_name}.")
                return  # The user desires to keep the application package, therefore exit gracefully

        # All validations have passed, drop object
        drop_generic_object(
            console=console,
            object_type="application package",
            object_name=package_name,
            role=package_role,
        )
