import functools
import sys
from typing import Any, Callable, List

try:
    import snowflake.snowpark
except ModuleNotFoundError as exc:
    print(
        "An exception occurred while importing snowflake-snowpark-python package: ",
        exc,
        file=sys.stderr,
    )
    sys.exit(1)

found_correct_version = hasattr(
    snowflake.snowpark.context, "_is_execution_environment_sandboxed_for_client"
) and hasattr(snowflake.snowpark.context, "_should_continue_registration")

if not found_correct_version:
    print(
        "Did not find the minimum required version for snowflake-snowpark-python package. Please upgrade to v1.15.0 or higher.",
        file=sys.stderr,
    )
    sys.exit(1)


__snowflake_cli_native_app_internal_callback_return_list: List[Any] = []


def __snowflake_cli_native_app_internal_callback_replacement():
    global __snowflake_cli_native_app_internal_callback_return_list

    def __snowflake_cli_native_app_internal_transform_snowpark_object_to_json(
        extension_function_properties,
    ):

        extension_function_dict = {}
        extension_function_dict[
            "object_type"
        ] = extension_function_properties.object_type.name
        extension_function_dict[
            "object_name"
        ] = extension_function_properties.object_name
        extension_function_dict["input_args"] = [
            {"name": input_arg.name, "datatype": input_arg.datatype.__name__}
            for input_arg in extension_function_properties.input_args
        ]
        extension_function_dict[
            "input_sql_types"
        ] = extension_function_properties.input_sql_types
        extension_function_dict["return_sql"] = extension_function_properties.return_sql
        extension_function_dict[
            "runtime_version"
        ] = extension_function_properties.runtime_version
        extension_function_dict[
            "all_imports"
        ] = extension_function_properties.all_imports
        extension_function_dict[
            "all_packages"
        ] = extension_function_properties.all_packages
        extension_function_dict["handler"] = extension_function_properties.handler
        extension_function_dict[
            "external_access_integrations"
        ] = extension_function_properties.external_access_integrations
        extension_function_dict["secrets"] = extension_function_properties.secrets
        extension_function_dict[
            "inline_python_code"
        ] = extension_function_properties.inline_python_code
        extension_function_dict[
            "raw_imports"
        ] = extension_function_properties.raw_imports
        extension_function_dict["replace"] = extension_function_properties.replace
        extension_function_dict[
            "if_not_exists"
        ] = extension_function_properties.if_not_exists
        extension_function_dict["execute_as"] = extension_function_properties.execute_as
        extension_function_dict["anonymous"] = extension_function_properties.anonymous
        # Set func based on type
        raw_func = extension_function_properties.func
        extension_function_dict["func"] = (
            raw_func.__name__ if isinstance(raw_func, Callable) else raw_func
        )
        # Set native app params based on dictionary
        if extension_function_properties.native_app_params is not None:
            extension_function_dict[
                "schema"
            ] = extension_function_properties.native_app_params["schema"]
            extension_function_dict[
                "application_roles"
            ] = extension_function_properties.native_app_params["application_roles"]
        else:
            extension_function_dict["schema"] = extension_function_dict[
                "application_roles"
            ] = None
        # Imports and handler will be set at a later time.
        return extension_function_dict

    def __snowflake_cli_native_app_internal_callback_append_to_list(
        callback_return_list, extension_function_properties
    ):
        extension_function_dict = (
            __snowflake_cli_native_app_internal_transform_snowpark_object_to_json(
                extension_function_properties
            )
        )
        callback_return_list.append(extension_function_dict)
        return False

    return functools.partial(
        __snowflake_cli_native_app_internal_callback_append_to_list,
        __snowflake_cli_native_app_internal_callback_return_list,
    )


with open("dummy_file.py", mode="r", encoding="utf-8") as udf_code:
    code = udf_code.read()


snowflake.snowpark.context._is_execution_environment_sandboxed_for_client = (  # noqa: SLF001
    True
)
snowflake.snowpark.context._should_continue_registration = (  # noqa: SLF001
    __snowflake_cli_native_app_internal_callback_replacement()
)
snowflake.snowpark.session._is_execution_environment_sandboxed_for_client = (  # noqa: SLF001
    True
)

del globals()["__snowflake_cli_native_app_internal_callback_replacement"]

try:
    exec(code, globals())
except Exception as exc:  # Catch any error
    print("An exception occurred while executing file: ", exc, file=sys.stderr)
    sys.exit(1)

print(__snowflake_cli_native_app_internal_callback_return_list)
