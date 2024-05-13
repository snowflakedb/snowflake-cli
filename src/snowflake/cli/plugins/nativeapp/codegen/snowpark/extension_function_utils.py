from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from click.exceptions import ClickException


class MalformedExtensionFunctionError(ClickException):
    """Required extension function attribute is missing."""

    def __init__(self, message: str):
        super().__init__(message=message)


TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"


def _sanitize_ex_fn_attribute(
    attr: str,
    ex_fn: Dict[str, Any],
    make_uppercase: bool = False,
    expected_type: Type = str,
    default_value: Any = None,
):
    has_attr = ex_fn.get(attr, None) and len(ex_fn[attr]) > 0
    if has_attr:
        if not isinstance(ex_fn[attr], expected_type):
            raise MalformedExtensionFunctionError(
                f"Attribute '{attr}' of extension function must be of type '{expected_type}'."
            )
        if expected_type == str and make_uppercase:
            ex_fn[attr] = ex_fn[attr].upper()
        else:
            ex_fn[attr] = ex_fn[attr]
    else:
        ex_fn[attr] = default_value


def _create_missing_attr_str(attribute: str, py_file: Path):
    return f"Required attribute '{attribute}' of extension function is missing for an extension function defined in python file {py_file.absolute()}."


def _is_function_wellformed(ex_fn: Dict[str, Any]) -> bool:
    tuple_type: Tuple = ()
    if ex_fn.get("func", None):
        if isinstance(ex_fn["func"], str):
            return ex_fn["func"].strip() != ""
        elif isinstance(ex_fn["func"], type(tuple_type)):
            return isinstance(ex_fn["func"][1], str) and ex_fn["func"][1].strip() != ""
    return False


def sanitize_extension_function_data(ex_fn: Dict[str, Any], py_file: Path) -> bool:
    """
    Helper function to add in defaults for keys that do not exist in the dictionary or contain empty strings when they should not.
    This helper function is needed because different callback sources can create dictionaries with different/missing keys, and since
    Snowflake CLI may not own all callback implementations, the dictionaries need to have the minimum set of keys and their default
    values to be used in creation of the SQL DDL statements.

    Returns:
    A boolean value, True if everything has been successfully validated and assigned, False if an error was encountered.
    """

    # Must have keys to create an extension function in SQL for Native Apps
    _sanitize_ex_fn_attribute(
        attr="object_type", ex_fn=ex_fn, make_uppercase=True, expected_type=str
    )
    if ex_fn["object_type"] is None:
        raise MalformedExtensionFunctionError(
            _create_missing_attr_str(attribute="object_type", py_file=py_file)
        )

    _sanitize_ex_fn_attribute(
        attr="object_name", ex_fn=ex_fn, make_uppercase=True, expected_type=str
    )
    if ex_fn["object_name"] is None:
        raise MalformedExtensionFunctionError(
            _create_missing_attr_str(attribute="object_name", py_file=py_file)
        )

    _sanitize_ex_fn_attribute(
        attr="return_sql", ex_fn=ex_fn, make_uppercase=True, expected_type=str
    )
    if ex_fn["return_sql"] is None:
        raise MalformedExtensionFunctionError(
            _create_missing_attr_str(attribute="return_sql", py_file=py_file)
        )

    if not _is_function_wellformed(ex_fn=ex_fn):
        raise MalformedExtensionFunctionError(
            _create_missing_attr_str(attribute="func", py_file=py_file)
        )

    optional_expected_type: Optional[List] = []
    default_raw_imports: List[Union[str, Tuple[str, str]]] = []
    _sanitize_ex_fn_attribute(
        attr="raw_imports",
        ex_fn=ex_fn,
        expected_type=type(optional_expected_type),
        default_value=default_raw_imports,
    )
    if len(ex_fn["raw_imports"]) == 0:
        raise MalformedExtensionFunctionError(
            _create_missing_attr_str(attribute="raw_imports", py_file=py_file)
        )

    _sanitize_ex_fn_attribute(
        attr="schema", ex_fn=ex_fn, make_uppercase=True, expected_type=str
    )
    if ex_fn["schema"] is None:
        raise MalformedExtensionFunctionError(
            f"Required attribute 'schema' in 'native_app_params' of extension function is missing for an extension function defined in python file {py_file.absolute()}."
        )

    # Other optional keys
    ex_fn["anonymous"] = ex_fn.get("anonymous", False)
    ex_fn["replace"] = ex_fn.get("replace", False)
    ex_fn["if_not_exists"] = ex_fn.get("if_not_exists", False)

    if ex_fn["replace"] and ex_fn["if_not_exists"]:
        raise MalformedExtensionFunctionError(
            "Options 'replace' and 'if_not_exists' are incompatible."
        )

    default_input_args: List[Dict[str, Any]] = []
    _sanitize_ex_fn_attribute(
        attr="input_args",
        ex_fn=ex_fn,
        expected_type=type(optional_expected_type),
        default_value=default_input_args,
    )

    default_input_types: List[str] = []
    _sanitize_ex_fn_attribute(
        attr="input_sql_types",
        ex_fn=ex_fn,
        expected_type=type(optional_expected_type),
        default_value=default_input_types,
    )
    # input_args and input_sql_types can be None as a function may not accept any arguments
    if (
        isinstance(ex_fn["input_args"], List)
        and isinstance(ex_fn["input_sql_types"], List)
    ) and len(ex_fn["input_args"]) != len(ex_fn["input_sql_types"]):
        raise MalformedExtensionFunctionError(
            "The number of extension function parameters does not match the number of parameter types."
        )

    _sanitize_ex_fn_attribute(attr="all_imports", ex_fn=ex_fn, expected_type=str)
    _sanitize_ex_fn_attribute(attr="all_packages", ex_fn=ex_fn, expected_type=str)
    _sanitize_ex_fn_attribute(
        attr="external_access_integrations", ex_fn=ex_fn, expected_type=List
    )
    _sanitize_ex_fn_attribute(attr="secrets", ex_fn=ex_fn, expected_type=Dict)
    _sanitize_ex_fn_attribute(attr="inline_python_code", ex_fn=ex_fn, expected_type=str)
    _sanitize_ex_fn_attribute(
        attr="execute_as", ex_fn=ex_fn, make_uppercase=True, expected_type=str
    )
    _sanitize_ex_fn_attribute(attr="handler", ex_fn=ex_fn, expected_type=str)

    _sanitize_ex_fn_attribute(attr="runtime_version", ex_fn=ex_fn, expected_type=str)
    if ex_fn["runtime_version"] is None:
        raise MalformedExtensionFunctionError(
            _create_missing_attr_str(attribute="runtime_version", py_file=py_file)
        )

    has_app_roles = (
        ex_fn.get("application_roles", None) and len(ex_fn["application_roles"]) > 0
    )
    if has_app_roles:
        if all(isinstance(app_role, str) for app_role in ex_fn["application_roles"]):
            ex_fn["application_roles"] = [
                app_role.upper() for app_role in ex_fn["application_roles"]
            ]
        else:
            raise MalformedExtensionFunctionError(
                f"Attribute 'application_roles' of extension function must be a list of strings."
            )
    else:
        ex_fn["application_roles"] = []

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


def get_object_type_as_text(object_type: str) -> str:
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
        raise MalformedExtensionFunctionError(
            f"Could not determine handler name for {func[1]}."
        )


def _get_schema_and_name_for_extension_function(
    object_name: str, schema: Optional[str], func: str
) -> Optional[str]:
    """
    Gets the name of the extension function to be used in the creation of the SQL statement.
    It will use the schema and the python function's name as the object name if the function name is determined to be a Snowpark-generated placeholder.
    Otherwise, it will honor the user's input for object name.

    """
    if object_name.startswith(TEMP_OBJECT_NAME_PREFIX):
        return f"{schema}.{func}" if schema else func
    else:
        return f"{schema}.{object_name}" if schema else object_name


def _is_single_quoted(name: str) -> bool:
    """
    Helper function to do a generic check on whether the provided string is surrounded by single quotes.
    """
    return name.startswith("'") and name.endswith("'")


def _ensure_single_quoted(obj_lst: List[str]) -> List[str]:
    """
    Helper function to ensure that a list of object strings is transformed to a list of object strings surrounded by single quotes.
    """
    return [obj if _is_single_quoted(obj) else f"'{obj}'" for obj in obj_lst]


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
    return ",".join(_ensure_single_quoted(all_urls))


def enrich_entity(
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
        func=entity["func"],
    )
    entity["all_imports"] = _get_all_imports(
        raw_imports=entity["raw_imports"] or [], suffix_str=suffix_str
    )
