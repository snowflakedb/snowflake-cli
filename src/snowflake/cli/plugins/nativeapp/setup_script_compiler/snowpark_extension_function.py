import os
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.plugins.nativeapp.utils import is_parent_directory, is_single_quoted
from snowflake.snowpark._internal.udf_utils import UDFColumn
from snowflake.snowpark._internal.utils import TempObjectType

STAGE_PREFIX = "@"
TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"


def get_handler_path_without_suffix(file_path: Path) -> str:
    parent_dir = str(file_path.parent).strip(os.path.sep)
    file_name = file_path.name.split(".")[0]
    return f"{parent_dir}.{file_name}"


class ExtensionFunctionProperties:
    """
    This is the mutable version of snowflake.snowpark._internal.udf_utils.ExtensionFunctionProperties,
    so that we may perform validation, reassignment and new assignments for generating Snowpark SQL DDL.
    """

    def __init__(
        self,
        func: Union[Callable, Tuple[str, str]],
        object_type: TempObjectType,
        object_name: str,
        input_args: List[UDFColumn],
        input_sql_types: List[str],
        return_sql: str,
        all_imports: Optional[str],
        all_packages: str,
        handler: Optional[str],
        external_access_integrations: Optional[List[str]],
        secrets: Optional[Dict[str, str]],
        inline_python_code: Optional[str],
        native_app_params: Optional[Dict[str, Any]],
        raw_imports: Optional[List[Union[str, Tuple[str, str]]]],
        runtime_version: str = "3.8",
        replace: bool = False,
        if_not_exists: bool = False,
        execute_as: Optional[Literal["caller", "owner"]] = None,
        anonymous: bool = False,
    ) -> None:
        # Set initial properties
        self.func = func
        self.object_type = object_type
        self.object_name = object_name
        self.input_args = input_args
        self.input_sql_types = input_sql_types
        self.return_sql = return_sql
        self.all_imports = all_imports
        self.all_packages = all_packages
        self.handler = handler
        self.external_access_integrations = external_access_integrations
        self.secrets = secrets
        self.inline_python_code = inline_python_code
        self.native_app_params = native_app_params
        self.raw_imports = raw_imports
        self.runtime_version = runtime_version
        self.replace = replace
        self.if_not_exists = if_not_exists
        self.execute_as = execute_as
        self.anonymous = anonymous

        # Set additional properties
        self.set_schema()
        self.set_object_name_for_udf_sp()

        self.validate_raw_imports()
        self.set_all_imports()

    def set_deploy_root(self, deploy_root: Path):
        self.deploy_root = deploy_root

    def set_schema(self):
        if self.native_app_params is not None and "schema" in self.native_app_params:
            self.schema = self.native_app_params["schema"]
        else:
            cc.warning(
                f"Could not find a schema for {self.handler}, proceeding without the schema."
            )

    def set_object_name_for_udf_sp(self):
        if self.object_name.startswith(TEMP_OBJECT_NAME_PREFIX):
            self.object_name = f"{self.schema}.{self.handler}"
        # Else, use the name provided by the user

    def set_source_file(self, source_file: Path):
        self.source_file = source_file

    def set_destination_file(self, dest_file: Path):
        self.dest_file = dest_file

    def set_handler(self):
        if isinstance(self.func, Callable):
            self.handler = f"{get_handler_path_without_suffix(self.dest_file)}.{self.func.__name__}"
        else:
            # FYI: This is how Snowpark creates handler string when func is of type Tuple[str, str]
            udf_file_name = os.path.basename(self.func[0])
            # for a compressed file, it might have multiple extensions
            # and we should remove all extensions
            udf_file_name_base = udf_file_name.split(".")[0]
            self.handler = f"{udf_file_name_base}.{self.func[1]}"

    def validate_raw_imports(self):
        assert self.all_imports is ""

        if self.raw_imports is None or len(self.raw_imports) == 0:
            cc.warning(
                f"Could not find any imports for {self.object_name}, proceeding without use of imports."
            )

        valid_imports: List[Union[str, Tuple[str, str]]] = []
        for raw_import in self.raw_imports:
            if isinstance(raw_import, str):
                self.get_valid_imports(valid_imports, raw_import)
            elif isinstance(raw_import, tuple) and len(raw_import) == 2:
                self.get_valid_imports(valid_imports, raw_import[0], raw_import[1])
            else:
                raise TypeError(
                    f"{(self.object_type).replace(' ', '-')}-level import can only be a file path (str) "
                    "or a tuple of the file path (str) and the import path (str)."
                )
        self.valid_imports = valid_imports

    def get_valid_imports(
        self,
        valid_imports: List[Union[str, Tuple[str, str]]],
        path: str,
        import_path: Optional[str] = None,
    ):
        trimmed_path = path.strip()
        trimmed_import_path = import_path.strip() if import_path else None

        # For trimmed_path, follow Snowpark checks
        if trimmed_path.startswith(STAGE_PREFIX):
            cc.warning(
                f"Cannot specify stage name in import path in case of Snowflake Native Apps. Proceeding without use of this import for {self.object_name}."
            )
        else:
            if not os.path.exists(trimmed_path):
                cc.warning(
                    f"Could not find {trimmed_path} in your filesystem. Proceeding without use of this import for {self.object_name}."
                )
            if not os.path.isfile(trimmed_path) and not os.path.isdir(trimmed_path):
                # os.path.isfile() returns True when the passed in file is a symlink.
                # So this code might not be reachable. To avoid mistakes, keep it here for now.
                raise ValueError(
                    f"You must only specify a local file or directory in the 'imports' property for {self.object_name}."
                )

            abs_path = os.path.abspath(trimmed_path)

            if trimmed_import_path is not None:
                # the import path only works for the directory and the Python file
                if os.path.isdir(abs_path):
                    import_file_path = trimmed_import_path.replace(".", os.path.sep)
                elif os.path.isfile(abs_path) and abs_path.endswith(".py"):
                    import_file_path = (
                        f"{trimmed_import_path.replace('.', os.path.sep)}.py"
                    )
                else:
                    import_file_path = None

                if import_file_path is not None:
                    if not is_parent_directory(
                        parent_dir=self.deploy_root, file_path=import_file_path
                    ):
                        raise ValueError(
                            f"import_path {trimmed_import_path} is invalid "
                        )
                valid_imports.append((trimmed_path, trimmed_import_path))
            else:
                valid_imports.append(trimmed_path)

    def set_all_imports(self):
        # Ensure that the raw_imports have been validated
        all_urls: List[str] = []
        for valid_import in self.valid_imports:
            if isinstance(valid_import, str):
                stage_import = valid_import.replace(os.path.sep, ".")
                all_urls.append(stage_import)
            else:
                all_urls.append(valid_import[1])
        self.all_imports = ",".join(
            [url if is_single_quoted(url) else f"'{url}'" for url in all_urls]
        )

    def get_object_type_as_str(self) -> str:
        return self.object_type.value.replace("_", " ")

    def generate_create_sql_statement(self) -> str:
        if self.object_type == TempObjectType.PROCEDURE and self.anonymous:
            cc.warning(
                f"""{(self.object_type).replace(' ', '-')} {self.object_name} cannot be an anonymous procedure in a Snowflake Native App.
                       Skipping generation of SQL for this object."""
            )
            return ""

        add_replace = f" OR REPLACE " if self.replace else ""
        sql_func_args = ",".join(
            [f"{a.name} {t}" for a, t in zip(self.input_args, self.input_sql_types)]
        )
        imports_in_sql = f"IMPORTS=({self.all_imports})" if self.all_imports else ""
        packages_in_sql = f"PACKAGES=({self.all_packages})" if self.all_packages else ""
        external_access_integrations_in_sql = (
            f"\nEXTERNAL_ACCESS_INTEGRATIONS=({','.join(self.external_access_integrations)})"
            if self.external_access_integrations
            else ""
        )
        secrets_in_sql = (
            f"""\nSECRETS=({",".join([f"'{k}'={v}" for k, v in self.secrets.items()])})"""
            if self.secrets
            else ""
        )
        if self.execute_as is None:
            execute_as_sql = ""
        else:
            execute_as_sql = f"""\nEXECUTE AS {self.execute_as.upper()}"""

        if (self.inline_python_code is None) or (len(self.inline_python_code) == 0):
            inline_python_code_in_sql = ""
        else:
            inline_python_code_in_sql = f"""
            AS $$
            {self.inline_python_code}
            $$
            """

        create_query = dedent(
            f"""
            CREATE{add_replace}
            {self.object_type.value.replace("_", " ")} {"IF NOT EXISTS" if self.if_not_exists else ""} {self.object_name}({sql_func_args})
            {self.return_sql}
            LANGUAGE PYTHON
            RUNTIME_VERSION={self.runtime_version}
            {imports_in_sql}
            {packages_in_sql}
            {external_access_integrations_in_sql}
            {secrets_in_sql}
            HANDLER='{self.handler}'{execute_as_sql}
            {inline_python_code_in_sql}
            """
        )
        return create_query

    def generate_grant_sql_statements(self) -> str:
        if (
            self.native_app_params is not None
            and "application_roles" in self.native_app_params
        ):
            grant_sql_statements = []
            for app_role in self.native_app_params["application_roles"]:
                grant_sql_statement = dedent(
                    f"""\
                    GRANT USAGE ON {self.get_object_type_as_str()} {self.object_name}
                    TO APPLICATION ROLE {app_role.upper()};
                    """
                )
                grant_sql_statements.append(grant_sql_statement)
            return "\n".join(grant_sql_statements)
        else:
            cc.warning(
                dedent(
                    f"""
                {self.get_object_type_as_str()} {self.object_name} in {self.source_file} does not have application roles specified in its definition.
                Skipping generation of 'GRANT USAGE ON ...' SQL statement for this object.
            """
                )
            )

            return ""


def convert_snowpark_object_to_internal_rep(
    extension_function_properties: Any,
) -> ExtensionFunctionProperties:
    return ExtensionFunctionProperties(
        replace=extension_function_properties.replace,
        func=extension_function_properties.func,
        object_type=extension_function_properties.object_type,
        if_not_exists=extension_function_properties.if_not_exists,
        object_name=extension_function_properties.object_name,
        input_args=extension_function_properties.input_args,
        input_sql_types=extension_function_properties.input_sql_types,
        return_sql=extension_function_properties.return_sql,
        runtime_version=extension_function_properties.runtime_version,
        all_imports=extension_function_properties.all_imports,
        all_packages=extension_function_properties.all_packages,
        external_access_integrations=extension_function_properties.external_access_integrations,
        secrets=extension_function_properties.secrets,
        handler=extension_function_properties.handler,
        execute_as=extension_function_properties.execute_as,
        inline_python_code=extension_function_properties.inline_python_code,
        native_app_params=extension_function_properties.native_app_params,
        raw_imports=extension_function_properties.raw_imports,
        anonymous=extension_function_properties.anonymous,
    )
