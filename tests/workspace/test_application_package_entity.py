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

import yaml
from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)


def _get_app_pkg_entity(project_directory):
    with project_directory("workspaces_simple") as project_root:
        with Path(project_root / "snowflake.yml").open() as definition_file_path:
            project_definition = yaml.safe_load(definition_file_path)
            model = ApplicationPackageEntityModel(
                **project_definition["entities"]["pkg"]
            )
            action_ctx = ActionContext(project_root=project_root)
            return ApplicationPackageEntity(model), action_ctx


def test_bundle(project_directory):
    app_pkg, bundle_ctx = _get_app_pkg_entity(project_directory)

    bundle_result = app_pkg.action_bundle(bundle_ctx)

    deploy_root = bundle_result.deploy_root()
    assert (deploy_root / "README.md").exists()
    assert (deploy_root / "manifest.yml").exists()
    assert (deploy_root / "setup_script.sql").exists()
