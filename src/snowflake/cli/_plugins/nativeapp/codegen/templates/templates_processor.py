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

from typing import Optional

import jinja2
from snowflake.cli._plugins.nativeapp.artifacts import BundleMap
from snowflake.cli._plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
)
from snowflake.cli._plugins.nativeapp.exceptions import InvalidTemplateInFileError
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.api.rendering.project_definition_templates import (
    get_client_side_jinja_env,
)
from snowflake.cli.api.rendering.sql_templates import (
    choose_sql_jinja_env_based_on_template_syntax,
)


class TemplatesProcessor(ArtifactProcessor):
    """
    Processor class to perform template expansion on all relevant artifacts (specified in the project definition file).
    """

    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ):
        """
        Process the artifact by executing the template expansion logic on it.
        """
        cc.step(f"Processing artifact {artifact_to_process} with templates processor")

        bundle_map = BundleMap(
            project_root=self._bundle_ctx.project_root,
            deploy_root=self._bundle_ctx.deploy_root,
        )
        bundle_map.add(artifact_to_process)

        for src, dest in bundle_map.all_mappings(
            absolute=True,
            expand_directories=True,
        ):
            if src.is_dir():
                continue
            with self.edit_file(dest) as f:
                file_name = src.relative_to(self._bundle_ctx.project_root)

                jinja_env = (
                    choose_sql_jinja_env_based_on_template_syntax(
                        f.contents, reference_name=file_name
                    )
                    if dest.name.lower().endswith(".sql")
                    else get_client_side_jinja_env()
                )

                try:
                    expanded_template = jinja_env.from_string(f.contents).render(
                        get_cli_context().template_context
                    )

                # For now, we are printing the source file path in the error message
                # instead of the destination file path to make it easier for the user
                # to identify the file that has the error, and edit the correct file.
                except jinja2.TemplateSyntaxError as e:
                    raise InvalidTemplateInFileError(file_name, e, e.lineno) from e

                except jinja2.UndefinedError as e:
                    raise InvalidTemplateInFileError(file_name, e) from e

                if expanded_template != f.contents:
                    f.edited_contents = expanded_template
