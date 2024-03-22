from textwrap import dedent

from pydantic import ValidationError


class SchemaValidationError(Exception):
    generic_message = "For field {loc} you provided '{loc}'. This caused: {msg}"
    message_templates = {
        "string_type": "{msg} for field '{loc}', you provided '{input}'",
        "extra_forbidden": "{msg}. You provided field '{loc}' with value '{input}' that is not present in the schema",
        "missing": "Your project definition is missing following fields: {loc}",
    }

    def __init__(self, error: ValidationError):
        errors = error.errors()
        message = f"During evaluation of {error.title} schema following errors were encountered:\n"
        message += "\n".join(
            [
                self.message_templates.get(e["type"], self.generic_message).format(**e)
                for e in errors
            ]
        )

        super().__init__(dedent(message))
