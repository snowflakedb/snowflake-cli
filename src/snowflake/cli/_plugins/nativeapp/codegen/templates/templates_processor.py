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

from pathlib import Path
from typing import Any, Optional

import jinja2
from snowflake.cli._plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
)
from snowflake.cli._plugins.nativeapp.exceptions import InvalidTemplateInFileError
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.cli_global_context import get_cli_context, span
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.metrics import CLICounterField
from snowflake.cli.api.project.schemas.entities.common import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.api.rendering.project_definition_templates import (
    get_client_side_jinja_env,
    has_client_side_templates,
)
from snowflake.cli.api.rendering.sql_templates import (
    choose_sql_jinja_env_based_on_template_syntax,
    has_sql_templates,
)


def _is_sql_file(file: Path) -> bool:
    return file.name.lower().endswith(".sql")


class TemplatesProcessor(ArtifactProcessor):
    """
    Processor class to perform template expansion on all relevant artifacts (specified in the project definition file).
    """

    NAME = "templates"

    def expand_templates_in_file(
        self, src: Path, dest: Path, template_context: dict[str, Any] | None = None
    ) -> None:
        """
        Expand templates in the file.
        """
        if src.is_dir():
            return

        src_file_name = src.relative_to(self._bundle_ctx.project_root)

        try:
            with self.edit_file(dest) as file:
                if not has_client_side_templates(file.contents) and not (
                    _is_sql_file(dest) and has_sql_templates(file.contents)
                ):
                    return
                cc.step(f"Expanding templates in {src_file_name}")
                with cc.indented():
                    try:
                        jinja_env = (
                            choose_sql_jinja_env_based_on_template_syntax(
                                file.contents, reference_name=src_file_name
                            )
                            if _is_sql_file(dest)
                            else get_client_side_jinja_env()
                        )
                        expanded_template = jinja_env.from_string(file.contents).render(
                            template_context or get_cli_context().template_context
                        )

                    # For now, we are printing the source file path in the error message
                    # instead of the destination file path to make it easier for the user
                    # to identify the file that has the error, and edit the correct file.
                    except jinja2.TemplateSyntaxError as e:
                        raise InvalidTemplateInFileError(
                            src_file_name, e, e.lineno
                        ) from e

                    except jinja2.UndefinedError as e:
                        raise InvalidTemplateInFileError(src_file_name, e) from e

                    if expanded_template != file.contents:
                        file.edited_contents = expanded_template
        except UnicodeDecodeError as err:
            cc.warning(
                f"Could not read file {src_file_name}, error: {err.reason}. Skipping this file."
            )

    @span("templates_processor")
    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ) -> None:
        """
        Process the artifact by executing the template expansion logic on it.
        """

        get_cli_context().metrics.set_counter(CLICounterField.TEMPLATES_PROCESSOR, 1)

        bundle_map = BundleMap(
            project_root=self._bundle_ctx.project_root,
            deploy_root=self._bundle_ctx.deploy_root,
        )
        bundle_map.add(artifact_to_process)

        for src, dest in bundle_map.all_mappings(
            absolute=True,
            expand_directories=True,
        ):
            self.expand_templates_in_file(src, dest)
