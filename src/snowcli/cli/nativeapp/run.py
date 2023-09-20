from typing import Optional
from click.exceptions import ClickException
from strictyaml import YAML


class ApplicationPackageAlreadyExistsError(ClickException):
    def __init__(self, name: str):
        super().__init__(
            f"An Application Package {name} already exists in account that may have been created without snowCLI. "
        )


class SnowflakeSQLExecutionError(ClickException):
    """
    Could not successfully execute the Snowflake SQL statements.
    """

    def __init__(self, queries: Optional[str] = None):
        super().__init__(
            f"""
                {self.__doc__}
                {queries if queries else ""}
            """
        )


def get_required_field_from_definition(
    index_a: str, index_b: str, existing_definition: dict, generated_yml_overrides: YAML
) -> str:
    """
    Retrieve a twice-nested value from the "native_app" sub-schema in a project definitino yml file.
    If no value is found, such as optionals in case of "package" or "application", then use the default values.

    Args:
        index_a (str): The first key to index into "native_app" sub-schema with.
        index_b (str): The second key to index into "native_app[index_a]" sub-schema with.
        existing_definition (dict): The cached representation of your snowflake.yml, potentially with overrides if snowflake.local.yml is also present.
        generated_yml_overrides (YAML): Provides default values in case of missing values from snowflake.yml

    Returns:
        str

    """
    try:
        return existing_definition[index_a][index_b]
    except KeyError:
        return generated_yml_overrides["native_app"][index_a][index_b]
