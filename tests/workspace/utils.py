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

from textwrap import dedent

APP_PACKAGE_ENTITY = (
    "snowflake.cli.api.entities.application_package_entity.ApplicationPackageEntity"
)

MOCK_SNOWFLAKE_YML_FILE = dedent(
    """\
    definition_version: 2
    entities:
        pkg:
            type: application package
            identifier: myapp_pkg
            artifacts:
                - setup.sql
                - app/README.md
                - src: app/streamlit/*.py
                  dest: ui/
            manifest: app/manifest.yml
        app:
            type: application
            identifier: myapp
            from:
                target: pkg
    """
)

MOCK_SNOWFLAKE_YML_V1_FILE = dedent(
    """\
        definition_version: 1
        native_app:
            name: myapp
            source_stage: app_src.stage
            artifacts:
                - src: app/*
                  dest: ./
    """
)
