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

from __future__ import annotations

import logging

from click import Command
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.app.dev.docs.commands_docs_generator import generate_command_docs
from snowflake.cli.app.dev.docs.project_definition_docs_generator import (
    generate_project_definition_docs,
)

log = logging.getLogger(__name__)


def generate_docs(root: SecurePath, command: Command):
    """
    Generates documentation for each command, its options and for the project definition.
    """
    root.mkdir(exist_ok=True)
    generate_command_docs(root / "commands", command)
    generate_project_definition_docs(root / "project_definition")
