import os
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.plugins.nativeapp.artifacts import NotInDeployRootError
from snowflake.cli.plugins.nativeapp.codegen.constants import (
    STAGE_PREFIX,
    TEMP_OBJECT_NAME_PREFIX,
)
from snowflake.cli.plugins.nativeapp.utils import is_single_quoted


class TempObjectType(Enum):
    """Replicates snowflake.snowpark._internal.utils.TempObjectType"""

    TABLE = "TABLE"
    VIEW = "VIEW"
    STAGE = "STAGE"
    FUNCTION = "FUNCTION"
    FILE_FORMAT = "FILE_FORMAT"
    QUERY_TAG = "QUERY_TAG"
    COLUMN = "COLUMN"
    PROCEDURE = "PROCEDURE"
    TABLE_FUNCTION = "TABLE_FUNCTION"
    DYNAMIC_TABLE = "DYNAMIC_TABLE"
    AGGREGATE_FUNCTION = "AGGREGATE_FUNCTION"
    CTE = "CTE"


def get_handler_path_without_suffix(file_path: Path, deploy_root: Path) -> str:
    if file_path.is_relative_to(deploy_root):
        norm_parent_path = os.path.normpath(file_path.parent)
        parent_paths = norm_parent_path.split(os.path.sep)
        file_name = file_path.name.split(".")[0]
        return f"{'.'.join(parent_paths)}.{file_name}"
    else:
        raise NotInDeployRootError(
            artifact_src="_",
            dest_path=file_path,
            deploy_root=deploy_root,
        )


def get_object_type_as_text(object_type: TempObjectType) -> str:
    return object_type.value.replace("_", " ")


class ExtensionFunctionProperties:
    """
    This is the mutable version of snowflake.snowpark._internal.udf_utils.ExtensionFunctionProperties,
    so that we may perform validation, reassignment and new assignments for generating Snowpark SQL DDL.
    """

    def __init__(
        self,
        func: Union[str, Tuple[str, str]],
        object_type: TempObjectType,
        object_name: str,
        input_args: List[Dict[str, Any]],
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
        # We allow native_app_params to be None as this obj could be used in other contexts as well
        self.native_app_params = native_app_params
        self.raw_imports = raw_imports
        self.runtime_version = runtime_version
        self.replace = replace
        self.if_not_exists = if_not_exists
        self.execute_as = execute_as
        self.anonymous = anonymous

    def set_deploy_root(self, deploy_root: Path) -> None:
        """
        Sets the deploy_root.
        """
        self.deploy_root = deploy_root

    def set_source_file(self, source_file: Path):
        """
        Sets the source py file for this extension function. This source file is guaranteed to exist in the project_root.
        """
        self.source_file = source_file

    def set_destination_file(self, dest_file: Path):
        """
        Sets the dest py file for this extension function. This dest file is guaranteed to exist in the deploy_root.
        """
        self.dest_file = dest_file

    def set_handler(self):
        """
        Sets the handler for the extension function to be used in the creation of the SQL statement.
        """
        # FYI: Needs dest file to be set
        if isinstance(self.func, str):
            self.handler = f"{get_handler_path_without_suffix(self.dest_file, self.deploy_root)}.{self.func}"
        else:
            # isinstance(self.func, Tuple[str, str]) is only possible if using decorator.register_from_file(), which is not allowed in codegen as of now.
            # When allowed, refer to https://github.com/snowflakedb/snowpark-python/blob/v1.15.0/src/snowflake/snowpark/_internal/udf_utils.py#L1092 on resolving handler name
            cc.warning(
                f"Could not determine handler name for {self.func[1]}, proceeding without the handler."
            )
            self.handler = None

    def set_schema(self) -> None:
        """
        Sets the schema (versioned or stateful) for the extension function to be used in the creation of the SQL statement.
        """
        if self.native_app_params is not None and "schema" in self.native_app_params:
            self.schema = self.native_app_params["schema"]
        else:
            cc.warning(
                f"{get_object_type_as_text(self.object_type)} {self.object_name} in {self.source_file} does not have a schema specified in its definition."
            )
            self.schema = None

    def set_application_roles(self) -> None:
        """
        Sets the application roles for the extension function to be used in the grant privileges SQL statement(s).
        """
        if (
            self.native_app_params is not None
            and "application_roles" in self.native_app_params
        ):
            self.application_roles = self.native_app_params["application_roles"]
        else:
            cc.warning(
                dedent(
                    f"""
                {get_object_type_as_text(self.object_type)} {self.object_name} in {self.source_file} does not have application roles specified in its definition.
                """
                )
            )
            self.application_roles = []

    def set_object_name_for_udf_sp(self) -> None:
        """
        Sets the name of the extension function to be used in the creation of the SQL statement.
        """
        # FYI: Needs schema and handler to be set
        if self.object_name.startswith(TEMP_OBJECT_NAME_PREFIX):
            assert self.handler is not None
            assert self.schema is not None
            self.object_name = f"{self.schema}.{self.handler}"
        # Else, use the name provided by the user

    def validate_raw_imports(self):
        """
        Validates the set of raw imports received from Snowpark's ExtensionFunctionProperties for this object.
        """
        assert (
            self.all_imports is ""
        )  # all_imports from sandbox should be empty, i.e. they have not been resolved due to lack of session.

        valid_imports: List[
            Union[str, Tuple[str, str]]
        ] = []  # All paths will be relative to the deploy_root

        if self.raw_imports is None or len(self.raw_imports) == 0:
            cc.warning(
                f"Could not find any imports for {self.object_name}, proceeding without use of imports."
            )

        for raw_import in self.raw_imports:
            if isinstance(raw_import, str):
                self.get_valid_imports(valid_imports, raw_import)
            elif isinstance(raw_import, tuple) and len(raw_import) == 2:
                self.get_valid_imports(valid_imports, raw_import[0], raw_import[1])
            else:
                # From snowflake-snowpark-python
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
        """
        Validates a specific import for this object.
        """
        trimmed_path = path.strip()
        trimmed_import_path = import_path.strip() if import_path else None

        # For trimmed_path, follow Snowpark checks
        if trimmed_path.startswith(STAGE_PREFIX):
            cc.warning(
                f"Cannot specify stage name in import path in case of Snowflake Native Apps. Proceeding without use of this import for {self.object_name}."
            )
        else:
            if (
                trimmed_import_path is None
            ):  # trimmed_path is both src and dest import path
                trimmed_path_obj = Path(self.deploy_root / trimmed_path)
                if trimmed_path_obj.exists():
                    # If only one value is specified, i.e. only trimmed_path, then it must also be
                    # a valid path on the stage, i.e. deploy_root
                    trimmed_path = (
                        f"/{trimmed_path}"
                        if not trimmed_path.startswith("/")
                        else trimmed_path
                    )
                    valid_imports.append(trimmed_path)
                else:
                    cc.warning(
                        f"Could not find {trimmed_path} in your deploy_root. Proceeding without use of this import for {self.object_name}."
                    )

                if not (trimmed_path_obj.is_file() or trimmed_path_obj.is_dir()):
                    # os.path.isfile() returns True when the passed in file is a symlink.
                    # So this code might not be reachable. To avoid mistakes, keep it here for now.
                    cc.warning(
                        f"You must only specify a local file or directory in the 'imports' property for {self.object_name}."
                    )
            else:
                abs_path = Path(
                    trimmed_path
                ).absolute()  # trimmed_path is src (therefore use without deploy_root) and trimmed_import_path is dest import path
                # the import path only works for the directory and the Python file
                if abs_path.is_dir():
                    import_file_path = trimmed_import_path.replace(".", os.path.sep)
                elif abs_path.is_file():
                    import_file_path = f"{trimmed_import_path.replace('.', os.path.sep)}{abs_path.suffix}"
                else:
                    cc.warning(
                        f"You must only specify a local file or directory in the 'imports' property for {self.object_name}."
                    )

                if Path(self.deploy_root / import_file_path).exists():
                    # If both values are specified, i.e. both trimmed_path and trimmed_import_path, then
                    # only trimmed_import_path should be a valid path on the stage
                    import_file_path = (
                        f"/{import_file_path}"
                        if not import_file_path.startswith("/")
                        else import_file_path
                    )
                    valid_imports.append((trimmed_path, import_file_path))
                else:
                    cc.warning(
                        f"Could not find {import_file_path} in your deploy_root. Proceeding without use of this import for {self.object_name}."
                    )

    def set_all_imports(self):
        """
        Sets validated imports in string form to be used in the creation of the SQL statement.
        """
        all_urls: List[str] = []
        for valid_import in self.valid_imports:
            if isinstance(valid_import, str):
                all_urls.append(valid_import)
            else:
                all_urls.append(valid_import[1])
        self.all_imports = ",".join(
            [url if is_single_quoted(url) else f"'{url}'" for url in all_urls]
        )

    def set_additional_properties(self, py_file: Path, dest_file: Path):
        self.set_source_file(py_file)
        self.set_destination_file(dest_file)
        self.set_handler()
        self.set_schema()
        self.set_application_roles()
        self.set_object_name_for_udf_sp()
        self.validate_raw_imports()
        self.set_all_imports()

    def generate_create_sql_statement(self):
        pass

    def generate_grant_sql_statements(self):
        pass


def convert_json_data_to_internal_rep(json_data) -> List[ExtensionFunctionProperties]:
    extension_function_lst: List[ExtensionFunctionProperties] = []
    for item in json_data:
        extension_function_lst.append(
            ExtensionFunctionProperties(
                object_type=TempObjectType(item["object_type"]),
                object_name=item["object_name"],
                input_args=item["input_args"],
                input_sql_types=item["input_sql_types"],
                return_sql=item["return_sql"],
                runtime_version=item["runtime_version"],
                all_imports=item["all_imports"],
                all_packages=item["all_packages"],
                handler=item["handler"],
                external_access_integrations=item["external_access_integrations"],
                secrets=item["secrets"],
                inline_python_code=item["inline_python_code"],
                native_app_params=item["native_app_params"],
                raw_imports=item["raw_imports"],
                replace=item["replace"],
                if_not_exists=item["if_not_exists"],
                execute_as=item["execute_as"],
                anonymous=item["anonymous"],
                func=item["func"],
            )
        )
    return extension_function_lst
