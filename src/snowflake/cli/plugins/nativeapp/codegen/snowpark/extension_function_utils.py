from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from snowflake.cli.api.console import cli_console as cc

TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"


def add_defaults_to_extension_function(ex_fn: Dict[str, Any]) -> bool:
    """
    Helper function to add in defaults for keys that do not exist in the dictionary or contain empty strings when they should not.
    This helper function is needed because different callback sources can create dictionaries with different/missing keys, and since
    Snowflake CLI may not own all callback implementations, the dictionaries need to have the minimum set of keys and their default
    values to be used in creation of the SQL DDL statements.

    Returns:
    A boolean value, True if everything has been successfully validated and assigned, False if an error was encountered.
    """

    # Must have keys
    try:
        ex_fn["object_type"] = ex_fn["object_type"].upper()
        assert ex_fn["object_type"] is not ""
        ex_fn["object_name"] = ex_fn["object_name"].upper()
        assert ex_fn["object_name"] is not ""
        ex_fn["return_sql"] = ex_fn["return_sql"].upper()
        assert ex_fn["return_sql"] is not ""
    except KeyError as err:
        cc.warning(f"{err}")
        return False
    except AssertionError as err:
        cc.warning(f"{err}")
        return False

    # Other optional keys
    ex_fn["anonymous"] = ex_fn.get("anonymous", False)
    ex_fn["replace"] = ex_fn.get("replace", False)
    ex_fn["if_not_exists"] = ex_fn.get("if_not_exists", False)

    if ex_fn["replace"] and ex_fn["if_not_exists"]:
        cc.warning("Options 'replace' and 'if_not_exists' are incompatible.")
        return False

    try:
        assert len(ex_fn["input_args"]) == len(ex_fn["input_sql_types"])
    except AssertionError as err:
        cc.warning(
            "The number of function parameters does not match the number of parameter types."
        )
        return False

    has_imports = ex_fn.get("all_imports", None) and len(ex_fn["all_imports"]) > 0
    ex_fn["all_imports"] = ex_fn["all_imports"] if has_imports else None

    has_packages = ex_fn.get("all_packages", None) and len(ex_fn["all_packages"]) > 0
    ex_fn["all_packages"] = ex_fn["all_packages"] if has_packages else None

    has_eai = (
        ex_fn.get("external_access_integrations", None)
        and len(ex_fn["external_access_integrations"]) > 0
    )
    ex_fn["external_access_integrations"] = (
        ex_fn["external_access_integrations"] if has_eai else None
    )

    has_secrets = ex_fn.get("secrets", None) and len(ex_fn["secrets"]) > 0
    ex_fn["secrets"] = ex_fn["secrets"] if has_secrets else None

    has_execute_as = ex_fn.get("execute_as", None) and len(ex_fn["execute_as"]) > 0
    ex_fn["execute_as"] = ex_fn["execute_as"].upper() if has_execute_as else None

    has_inline_code = (
        ex_fn.get("inline_python_code", None) and len(ex_fn["inline_python_code"]) > 0
    )
    ex_fn["inline_python_code"] = (
        ex_fn["inline_python_code"] if has_inline_code else None
    )

    # Cannot use KeyError check as only Java, Python and Scala need this value
    has_runtime_version = (
        ex_fn.get("runtime_version", None) and len(ex_fn["runtime_version"]) > 0
    )
    ex_fn["runtime_version"] = ex_fn["runtime_version"] if has_runtime_version else None

    # Cannot use KeyError check as only Java, Python and Scala need this value
    has_handler = ex_fn.get("handler", None) and len(ex_fn["handler"]) > 0
    ex_fn["handler"] = ex_fn["handler"] if has_handler else None

    # Cannot use KeyError check as only Native App Extension Functions need this value
    has_schema = ex_fn.get("schema", None) and len(ex_fn["schema"]) > 0
    ex_fn["schema"] = ex_fn["schema"] if has_schema else None

    # Cannot use KeyError check as only Native App Extension Functions need this value
    has_app_roles = (
        ex_fn.get("application_roles", None) and len(ex_fn["application_roles"]) > 0
    )
    ex_fn["application_roles"] = (
        [app_role.upper() for app_role in ex_fn["application_roles"]]
        if has_app_roles
        else None
    )

    # Cannot use KeyError check as only Native App Extension Functions need this value
    has_raw_imports = ex_fn.get("raw_imports", None) and len(ex_fn["raw_imports"]) > 0
    ex_fn["raw_imports"] = ex_fn["raw_imports"] if has_raw_imports else None

    return True


def _get_handler_path_without_suffix(
    file_path: Path, deploy_root: Path, suffix_str_to_rm: Optional[str] = None
) -> str:
    """
    Get a handler for an extension function based on the file path on the stage. If a specific suffix needs to be removed from the path,
    then that is also taken into account.
    """
    # rel_file_path = file_path.absolute().relative_to(deploy_root.resolve())  # No leading slash
    # stem, suffix = os.path.splitext(rel_file_path)
    # if (suffix_str_to_rm is not None) and (suffix == suffix_str_to_rm):
    #     file_parts = Path(stem).parts
    # else:
    #     file_parts = rel_file_path.parts
    # return ".".join(file_parts)
    return "NotImplementedHandler"


def _get_object_type_as_text(object_type: str) -> str:
    return object_type.replace("_", " ")


def _get_handler(
    dest_file: Path, func: Union[str, Tuple[str, str]], deploy_root: Path
) -> Optional[str]:
    """
    Gets the handler for the extension function to be used in the creation of the SQL statement.
    """
    if isinstance(func, str):
        return f"{_get_handler_path_without_suffix(file_path=dest_file, suffix_str_to_rm='.py', deploy_root=deploy_root)}.{func}"
    else:
        # isinstance(self.func, Tuple[str, str]) is only possible if using decorator.register_from_file(), which is not allowed in codegen as of now.
        # When allowed, refer to https://github.com/snowflakedb/snowpark-python/blob/v1.15.0/src/snowflake/snowpark/_internal/udf_utils.py#L1092 on resolving handler name
        cc.warning(
            f"Could not determine handler name for {func[1]}, proceeding without the handler."
        )
        return None


def _get_schema_and_name_for_extension_function(
    object_name: str, schema: str, handler: str
) -> Optional[str]:
    """
    Gets the name of the extension function to be used in the creation of the SQL statement.
    It will use the schema and the handler as the object name if the function name is determined to be a Snowpark-generated placeholder.
    Otherwise, it will honor the user's input for object name.

    """
    if object_name.startswith(TEMP_OBJECT_NAME_PREFIX):
        return f"{schema}.{handler}"
    else:
        return f"{schema}.{object_name}"


def is_single_quoted(name: str) -> bool:
    """
    Helper function to do a generic check on whether the provided string is surrounded by single quotes.
    """
    return name.startswith("'") and name.endswith("'")


def ensure_single_quoted(obj_lst: List[str]) -> List[str]:
    """
    Helper function to ensure that a list of object strings is transformed to a list of object strings surrounded by single quotes.
    """
    return [obj if is_single_quoted(obj) else f"'{obj}'" for obj in obj_lst]


def _get_all_imports(
    raw_imports: List[Union[str, Tuple[str, str]]], suffix_str: str
) -> str:
    """
    Creates a string containing all the relevant imports for an extension function. This string is used in the creation of the SQL statement.

    Parameters:
        raw_imports (List[Union[str, Tuple[str, str]]]): The raw imports that will be used to create the final string.
                    The function needes to handle different input types, similar to snowpark.
                    Example 1: [("tests/resources/test_udf_dir/test_udf_file.py", "resources.test_udf_dir.test_udf_file")]
                    Example 2: session.add_import("tests/resources/test_udf_dir/test_udf_file.py")
                    Example 3: session.add_import("tests/resources/test_udf_dir/test_udf_file.py", import_path="resources.test_udf_dir.test_udf_file")
        suffix_str (str): The suffix to add to the import path, if determined by the function. Must contain the "." part of the suffix as well.

    Returns:
        A string containing all the imports.
    """
    all_urls: List[str] = []
    for raw_import in raw_imports:  # Example 1
        if isinstance(raw_import, str):  # Example 2
            all_urls.append(raw_import)
        else:  # Example 3
            local_path = Path(raw_import[0])
            stage_import = raw_import[1]
            suffix_str = local_path.suffix
            if suffix_str != "":
                # We use suffix check here instead of local_path.is_file() as local_path may not exist, making is_file() False.
                # We do not provide validation on local_path existing, and hence should not fail or treat it differently than any other file.
                without_suffix = "/".join(stage_import.split("."))
                all_urls.append(f"{without_suffix}{suffix_str}")
            else:
                file_path = "/".join(stage_import.split("."))
                all_urls.append(file_path)
    return ",".join(ensure_single_quoted(all_urls))


def _enrich_entity(
    entity: Dict[str, Any], py_file: Path, deploy_root: Path, suffix_str: str
):
    """
    Sets additional properties for a given entity object, that could not be set earlier due to missing information or limited access to the execution context.
    """
    entity["handler"] = _get_handler(
        dest_file=py_file, func=entity["func"], deploy_root=deploy_root
    )
    entity["object_name"] = _get_schema_and_name_for_extension_function(
        object_name=entity["object_name"],
        schema=entity["schema"],
        handler=entity["handler"],
    )
    entity["all_imports"] = _get_all_imports(
        raw_imports=entity["raw_imports"] or [], suffix_str=suffix_str
    )
