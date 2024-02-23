from click import ClickException


def assert_object_definition_does_not_redefine_database_and_schema(
    definition, object_type_description
):
    database = definition.get("database")
    schema = definition.get("schema")
    name = definition["name"]
    number_of_fqn_parts_in_name = len(name.split("."))
    if number_of_fqn_parts_in_name >= 3 and database:
        raise ClickException(
            f"database of {object_type_description} {name} is redefined in its name"
        )
    if number_of_fqn_parts_in_name >= 2 and schema:
        raise ClickException(
            f"schema of {object_type_description} {name} is redefined in its name"
        )
