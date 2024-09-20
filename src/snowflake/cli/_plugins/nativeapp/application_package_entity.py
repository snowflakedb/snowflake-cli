import json
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Callable, List, Optional

import typer
from click import BadOptionUsage, ClickException
from snowflake.cli._plugins.nativeapp.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.nativeapp.artifacts import (
    BundleMap,
    build_bundle,
    find_version_info_in_manifest_file,
)
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.compiler import NativeAppCompiler
from snowflake.cli._plugins.nativeapp.constants import (
    ALLOWED_SPECIAL_COMMENTS,
    COMMENT_COL,
    EXTERNAL_DISTRIBUTION,
    INTERNAL_DISTRIBUTION,
    NAME_COL,
    OWNER_COL,
    PATCH_COL,
    SPECIAL_COMMENT,
    VERSION_COL,
)
from snowflake.cli._plugins.nativeapp.exceptions import (
    ApplicationPackageAlreadyExistsError,
    ApplicationPackageDoesNotExistError,
    CouldNotDropApplicationPackageWithVersions,
    SetupScriptFailedValidation,
)
from snowflake.cli._plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
    PolicyBase,
)
from snowflake.cli._plugins.nativeapp.utils import (
    needs_confirmation,
)
from snowflake.cli._plugins.stage.diff import DiffResult
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor
from snowflake.cli.api.entities.utils import (
    drop_generic_object,
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
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.schemas.v1.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.util import (
    extract_schema,
    identifier_to_show_like_pattern,
    to_identifier,
    unquote_identifier,
)
from snowflake.cli.api.rendering.jinja import (
    get_basic_jinja_env,
)
from snowflake.cli.api.utils.cursor import find_all_rows
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor


class ApplicationPackageEntity(EntityBase[ApplicationPackageEntityModel]):
    """
    A Native App application package.
    """

    def action_bundle(self, ctx: ActionContext, *args, **kwargs):
        model = self._entity_model
        return self.bundle(
            project_root=ctx.project_root,
            deploy_root=Path(model.deploy_root),
            bundle_root=Path(model.bundle_root),
            generated_root=Path(model.generated_root),
            package_name=model.identifier,
            artifacts=model.artifacts,
        )

    def action_deploy(
        self,
        ctx: ActionContext,
        prune: bool,
        recursive: bool,
        paths: List[Path],
        validate: bool,
        interactive: bool,
        force: bool,
        stage_fqn: Optional[str] = None,
        *args,
        **kwargs,
    ):
        model = self._entity_model
        package_name = model.fqn.identifier

        if force:
            policy = AllowAlwaysPolicy()
        elif interactive:
            policy = AskAlwaysPolicy()
        else:
            policy = DenyAlwaysPolicy()

        return self.deploy(
            console=ctx.console,
            project_root=ctx.project_root,
            deploy_root=Path(model.deploy_root),
            bundle_root=Path(model.bundle_root),
            generated_root=Path(model.generated_root),
            artifacts=model.artifacts,
            bundle_map=None,
            package_name=package_name,
            package_role=(model.meta and model.meta.role) or ctx.default_role,
            package_distribution=model.distribution,
            prune=prune,
            recursive=recursive,
            paths=paths,
            print_diff=True,
            validate=validate,
            stage_fqn=stage_fqn or f"{package_name}.{model.stage}",
            package_warehouse=(
                (model.meta and model.meta.warehouse) or ctx.default_warehouse
            ),
            post_deploy_hooks=model.meta and model.meta.post_deploy,
            package_scripts=[],  # Package scripts are not supported in PDFv2
            policy=policy,
        )

    def action_drop(self, ctx: ActionContext, force_drop: bool, *args, **kwargs):
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

    def action_validate(
        self, ctx: ActionContext, interactive: bool, force: bool, *args, **kwargs
    ):
        model = self._entity_model
        package_name = model.fqn.identifier
        stage_fqn = f"{package_name}.{model.stage}"
        if model.meta and model.meta.role:
            package_role = model.meta.role
        else:
            package_role = ctx.default_role

        def deploy_to_scratch_stage_fn():
            self.action_deploy(
                ctx=ctx,
                prune=True,
                recursive=True,
                paths=[],
                validate=False,
                stage_fqn=f"{package_name}.{model.scratch_stage}",
                interactive=interactive,
                force=force,
            )

        self.validate_setup_script(
            console=ctx.console,
            package_name=package_name,
            package_role=package_role,
            stage_fqn=stage_fqn,
            use_scratch_stage=True,
            scratch_stage_fqn=f"{package_name}.{model.scratch_stage}",
            deploy_to_scratch_stage_fn=deploy_to_scratch_stage_fn,
        )
        ctx.console.message("Setup script is valid")

    def action_version_list(
        self, ctx: ActionContext, *args, **kwargs
    ) -> SnowflakeCursor:
        model = self._entity_model
        return self.version_list(
            package_name=model.fqn.identifier,
            package_role=(model.meta and model.meta.role) or ctx.default_role,
        )

    def action_version_create(
        self,
        ctx: ActionContext,
        version: Optional[str],
        patch: Optional[int],
        skip_git_check: bool,
        interactive: bool,
        force: bool,
        *args,
        **kwargs,
    ):
        model = self._entity_model
        package_name = model.fqn.identifier
        return self.version_create(
            console=ctx.console,
            project_root=ctx.project_root,
            deploy_root=Path(model.deploy_root),
            bundle_root=Path(model.bundle_root),
            generated_root=Path(model.generated_root),
            artifacts=model.artifacts,
            package_name=package_name,
            package_role=(model.meta and model.meta.role) or ctx.default_role,
            package_distribution=model.distribution,
            prune=True,
            recursive=True,
            paths=None,
            print_diff=True,
            validate=True,
            stage_fqn=f"{package_name}.{model.stage}",
            package_warehouse=(
                (model.meta and model.meta.warehouse) or ctx.default_warehouse
            ),
            post_deploy_hooks=model.meta and model.meta.post_deploy,
            package_scripts=[],  # Package scripts are not supported in PDFv2
            version=version,
            patch=patch,
            skip_git_check=skip_git_check,
            force=force,
            interactive=interactive,
        )

    @staticmethod
    def bundle(
        project_root: Path,
        deploy_root: Path,
        bundle_root: Path,
        generated_root: Path,
        artifacts: list[PathMapping],
        package_name: str,
    ):
        bundle_map = build_bundle(project_root, deploy_root, artifacts)
        bundle_context = BundleContext(
            package_name=package_name,
            artifacts=artifacts,
            project_root=project_root,
            bundle_root=bundle_root,
            deploy_root=deploy_root,
            generated_root=generated_root,
        )
        compiler = NativeAppCompiler(bundle_context)
        compiler.compile_artifacts()
        return bundle_map

    @classmethod
    def deploy(
        cls,
        console: AbstractConsole,
        project_root: Path,
        deploy_root: Path,
        bundle_root: Path,
        generated_root: Path,
        artifacts: list[PathMapping],
        bundle_map: BundleMap | None,
        package_name: str,
        package_role: str,
        package_distribution: str,
        package_warehouse: str | None,
        prune: bool,
        recursive: bool,
        paths: List[Path] | None,
        print_diff: bool,
        validate: bool,
        stage_fqn: str,
        post_deploy_hooks: list[PostDeployHook] | None,
        package_scripts: List[str],
        policy: PolicyBase,
    ) -> DiffResult:
        # 1. Create a bundle if one wasn't passed in
        bundle_map = bundle_map or cls.bundle(
            project_root=project_root,
            deploy_root=deploy_root,
            bundle_root=bundle_root,
            generated_root=generated_root,
            artifacts=artifacts,
            package_name=package_name,
        )

        # 2. Create an empty application package, if none exists
        try:
            cls.create_app_package(
                console=console,
                package_name=package_name,
                package_role=package_role,
                package_distribution=package_distribution,
            )
        except ApplicationPackageAlreadyExistsError as e:
            cc.warning(e.message)
            if not policy.should_proceed("Proceed with using this package?"):
                raise typer.Abort() from e
        with get_sql_executor().use_role(package_role):
            if package_scripts:
                cls.apply_package_scripts(
                    console=console,
                    package_scripts=package_scripts,
                    package_warehouse=package_warehouse,
                    project_root=project_root,
                    package_role=package_role,
                    package_name=package_name,
                )

            # 3. Upload files from deploy root local folder to the above stage
            stage_schema = extract_schema(stage_fqn)
            diff = sync_deploy_root_with_stage(
                console=console,
                deploy_root=deploy_root,
                package_name=package_name,
                stage_schema=stage_schema,
                bundle_map=bundle_map,
                role=package_role,
                prune=prune,
                recursive=recursive,
                stage_fqn=stage_fqn,
                local_paths_to_sync=paths,
                print_diff=print_diff,
            )

        if post_deploy_hooks:
            cls.execute_post_deploy_hooks(
                console=console,
                project_root=project_root,
                post_deploy_hooks=post_deploy_hooks,
                package_name=package_name,
                package_warehouse=package_warehouse,
            )

        if validate:
            cls.validate_setup_script(
                console=console,
                package_name=package_name,
                package_role=package_role,
                stage_fqn=stage_fqn,
                use_scratch_stage=False,
                scratch_stage_fqn="",
                deploy_to_scratch_stage_fn=lambda *args: None,
            )

        return diff

    @staticmethod
    def version_list(package_name: str, package_role: str) -> SnowflakeCursor:
        """
        Get all existing versions, if defined, for an application package.
        It executes a 'show versions in application package' query and returns all the results.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            show_obj_query = f"show versions in application package {package_name}"
            show_obj_cursor = sql_executor.execute_query(show_obj_query)

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            return show_obj_cursor

    @classmethod
    def version_create(
        cls,
        console: AbstractConsole,
        project_root: Path,
        deploy_root: Path,
        bundle_root: Path,
        generated_root: Path,
        artifacts: list[PathMapping],
        package_name: str,
        package_role: str,
        package_distribution: str,
        package_warehouse: str | None,
        prune: bool,
        recursive: bool,
        paths: List[Path] | None,
        print_diff: bool,
        validate: bool,
        stage_fqn: str,
        post_deploy_hooks: list[PostDeployHook] | None,
        package_scripts: List[str],
        version: Optional[str],
        patch: Optional[int],
        force: bool,
        interactive: bool,
        skip_git_check: bool,
    ):
        """
        Perform bundle, application package creation, stage upload, version and/or patch to an application package.
        """
        is_interactive = False
        if force:
            policy = AllowAlwaysPolicy()
        elif interactive:
            is_interactive = True
            policy = AskAlwaysPolicy()
        else:
            policy = DenyAlwaysPolicy()

        if skip_git_check:
            git_policy = DenyAlwaysPolicy()
        else:
            git_policy = AllowAlwaysPolicy()

        # Make sure version is not None before proceeding any further.
        # This will raise an exception if version information is not found. Patch can be None.
        bundle_map = None
        if not version:
            console.message(
                dedent(
                    f"""\
                        Version was not provided through the Snowflake CLI. Checking version in the manifest.yml instead.
                        This step will bundle your app artifacts to determine the location of the manifest.yml file.
                    """
                )
            )
            bundle_map = cls.bundle(
                project_root=project_root,
                deploy_root=deploy_root,
                bundle_root=bundle_root,
                generated_root=generated_root,
                artifacts=artifacts,
                package_name=package_name,
            )
            version, patch = find_version_info_in_manifest_file(deploy_root)
            if not version:
                raise ClickException(
                    "Manifest.yml file does not contain a value for the version field."
                )

        # Check if --patch needs to throw a bad option error, either if application package does not exist or if version does not exist
        if patch is not None:
            try:
                if not cls.get_existing_version_info(
                    version, package_name, package_role
                ):
                    raise BadOptionUsage(
                        option_name="patch",
                        message=f"Cannot create a custom patch when version {version} is not defined in the application package {package_name}. Try again without using --patch.",
                    )
            except ApplicationPackageDoesNotExistError as app_err:
                raise BadOptionUsage(
                    option_name="patch",
                    message=f"Cannot create a custom patch when application package {package_name} does not exist. Try again without using --patch.",
                )

        if git_policy.should_proceed():
            cls.check_index_changes_in_git_repo(
                console=console,
                project_root=project_root,
                policy=policy,
                is_interactive=is_interactive,
            )

        cls.deploy(
            console=console,
            project_root=project_root,
            deploy_root=deploy_root,
            bundle_root=bundle_root,
            generated_root=generated_root,
            artifacts=artifacts,
            bundle_map=bundle_map,
            package_name=package_name,
            package_role=package_role,
            package_distribution=package_distribution,
            prune=prune,
            recursive=recursive,
            paths=paths,
            print_diff=print_diff,
            validate=validate,
            stage_fqn=stage_fqn,
            package_warehouse=package_warehouse,
            post_deploy_hooks=post_deploy_hooks,
            package_scripts=package_scripts,
            policy=policy,
        )

        # Warn if the version exists in a release directive(s)
        existing_release_directives = (
            cls.get_existing_release_directive_info_for_version(
                package_name, package_role, version
            )
        )

        if existing_release_directives:
            release_directive_names = ", ".join(
                row["name"] for row in existing_release_directives
            )
            console.warning(
                dedent(
                    f"""\
                    Version {version} already defined in application package {package_name} and in release directive(s): {release_directive_names}.
                    """
                )
            )

            user_prompt = (
                f"Are you sure you want to create a new patch for version {version} in application "
                f"package {package_name}? Once added, this operation cannot be undone."
            )
            if not policy.should_proceed(user_prompt):
                if is_interactive:
                    console.message("Not creating a new patch.")
                    raise typer.Exit(0)
                else:
                    console.message(
                        "Cannot create a new patch non-interactively without --force."
                    )
                    raise typer.Exit(1)

        # Define a new version in the application package
        if not cls.get_existing_version_info(version, package_name, package_role):
            cls.add_new_version(
                console=console,
                package_name=package_name,
                package_role=package_role,
                stage_fqn=stage_fqn,
                version=version,
            )
            return  # A new version created automatically has patch 0, we do not need to further increment the patch.

        # Add a new patch to an existing (old) version
        cls.add_new_patch_to_version(
            console=console,
            package_name=package_name,
            package_role=package_role,
            stage_fqn=stage_fqn,
            version=version,
            patch=patch,
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

    @classmethod
    def get_existing_release_directive_info_for_version(
        cls,
        package_name: str,
        package_role: str,
        version: str,
    ) -> List[dict]:
        """
        Get all existing release directives, if present, set on the version defined in an application package.
        It executes a 'show release directives in application package' query and returns the filtered results, if they exist.
        """
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            show_obj_query = (
                f"show release directives in application package {package_name}"
            )
            show_obj_cursor = sql_executor.execute_query(
                show_obj_query, cursor_class=DictCursor
            )

            if show_obj_cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(show_obj_query)

            show_obj_rows = find_all_rows(
                show_obj_cursor,
                lambda row: row[VERSION_COL] == unquote_identifier(version),
            )

            return show_obj_rows

    @classmethod
    def add_new_version(
        cls,
        console: AbstractConsole,
        package_name: str,
        package_role: str,
        stage_fqn: str,
        version: str,
    ) -> None:
        """
        Defines a new version in an existing application package.
        """
        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            console.step(
                f"Defining a new version {version} in application package {package_name}"
            )
            add_version_query = dedent(
                f"""\
                    alter application package {package_name}
                        add version {version}
                        using @{stage_fqn}
                """
            )
            sql_executor.execute_query(add_version_query, cursor_class=DictCursor)
            console.message(
                f"Version {version} created for application package {package_name}."
            )

    @classmethod
    def add_new_patch_to_version(
        cls,
        console: AbstractConsole,
        package_name: str,
        package_role: str,
        stage_fqn: str,
        version: str,
        patch: Optional[int] = None,
    ):
        """
        Add a new patch, optionally a custom one, to an existing version in an application package.
        """
        # Make the version a valid identifier, adding quotes if necessary
        version = to_identifier(version)
        sql_executor = get_sql_executor()
        with sql_executor.use_role(package_role):
            console.step(
                f"Adding new patch to version {version} defined in application package {package_name}"
            )
            add_version_query = dedent(
                f"""\
                    alter application package {package_name}
                        add patch {patch if patch else ""} for version {version}
                        using @{stage_fqn}
                """
            )
            result_cursor = sql_executor.execute_query(
                add_version_query, cursor_class=DictCursor
            )

            show_row = result_cursor.fetchall()[0]
            new_patch = show_row["patch"]
            console.message(
                f"Patch {new_patch} created for version {version} defined in application package {package_name}."
            )

    @classmethod
    def check_index_changes_in_git_repo(
        cls,
        console: AbstractConsole,
        project_root: Path,
        policy: PolicyBase,
        is_interactive: bool,
    ) -> None:
        """
        Checks if the project root, i.e. the native apps project is a git repository. If it is a git repository,
        it also checks if there any local changes to the directory that may not be on the application package stage.
        """

        from git import Repo
        from git.exc import InvalidGitRepositoryError

        try:
            repo = Repo(project_root, search_parent_directories=True)
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
                    if is_interactive:
                        console.message("Not creating a new version.")
                        raise typer.Exit(0)
                    else:
                        console.message(
                            "Cannot create a new version non-interactively without --force."
                        )
                        raise typer.Exit(1)

        except InvalidGitRepositoryError:
            pass  # not a git repository, which is acceptable

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

        with sql_executor.use_role(package_role):
            # 2. Check for versions in the application package
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

        # 3. Check distribution of the existing application package
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

        # 4. If distribution is internal, check if created by the Snowflake CLI
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
