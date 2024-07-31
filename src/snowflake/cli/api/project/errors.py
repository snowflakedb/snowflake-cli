# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from textwrap import dedent

from click import ClickException
from pydantic import ValidationError


class SchemaValidationError(ClickException):
    generic_message = "For field {location} you provided '{input}'. This caused: {msg}"
    message_templates = {
        "string_type": "{msg} for field '{location}', you provided '{input}'.",
        "extra_forbidden": "{msg}. You provided field '{location}' with value '{input}' that is not supported in given version.",
        "missing": "Your project definition is missing the following field: '{location}'",
    }

    def __init__(self, error: ValidationError):
        errors = error.errors()
        message = f"During evaluation of {error.title} in project definition following errors were encountered:\n"
        message += "\n".join(
            [
                self.message_templates.get(e["type"], self.generic_message).format(
                    **e, location=".".join(e["loc"]) if e["loc"] is not None else None
                )
                for e in errors
            ]
        )

        super().__init__(dedent(message))
