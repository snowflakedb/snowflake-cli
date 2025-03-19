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

import pytest
from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import (
    VersionInfo,
    build_bundle,
    find_events_definitions_in_manifest_file,
    find_version_info_in_manifest_file,
)
from snowflake.cli.api.artifacts.common import (
    ArtifactError,
    DeployRootError,
    NotInDeployRootError,
    SourceNotFoundError,
    TooManyFilesError,
)
from snowflake.cli.api.project.definition import load_project
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.project.util import to_identifier
from yaml import safe_dump

from tests.nativeapp.factories import ManifestFactory
from tests.nativeapp.utils import (
    assert_dir_snapshot,
)
from tests.testing_utils.files_and_dirs import temp_local_dir
from tests_common import change_directory


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_napp_project_1_artifacts(project_definition_files, os_agnostic_snapshot):
    project_root = project_definition_files[0].parent
    native_app = load_project(project_definition_files).project_definition.native_app

    with change_directory(project_root) as local_path:
        deploy_root = Path(local_path, native_app.deploy_root)
        build_bundle(local_path, deploy_root, native_app.artifacts)

        assert_dir_snapshot(deploy_root.relative_to(local_path), os_agnostic_snapshot)

        # we should be able to re-bundle without any errors happening
        build_bundle(local_path, deploy_root, native_app.artifacts)

        # any additional files created in the deploy root will be obliterated by re-bundle
        with open(deploy_root / "unknown_file.txt", "w") as handle:
            handle.write("I am an unknown file!")
        assert_dir_snapshot(deploy_root.relative_to(local_path), os_agnostic_snapshot)

        build_bundle(local_path, deploy_root, native_app.artifacts)
        assert_dir_snapshot(deploy_root.relative_to(local_path), os_agnostic_snapshot)


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_source_not_found(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(SourceNotFoundError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="NOTFOUND.md", dest="NOTFOUND.md")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_glob_matched_nothing(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(SourceNotFoundError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="**/*.jar", dest=".")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_outside_deploy_root_three_ways(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="setup.sql", dest="..")],
        )

    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="setup.sql", dest="/")],
        )

    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="app", dest=".")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_bad_deploy_root(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(DeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "..", "deploy"),
            artifacts=[],
        )

    with pytest.raises(DeployRootError):
        with open(project_root / "deploy", "w") as handle:
            handle.write("Deploy root should not be a file...")

        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_no_artifacts(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(ArtifactError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_too_many_files(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(TooManyFilesError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[
                PathMapping(
                    src="app/streamlit/*.py", dest="somehow_combined_streamlits.py"
                )
            ],
        )


@pytest.mark.parametrize(
    "label", [None, "", "label 'nested' quotes", "with$pecial?", "with space"]
)
@pytest.mark.parametrize("patch_name", [None, "1", 42])
@pytest.mark.parametrize(
    "version_name", [None, "v1", "1", 2, "1.2", 1.3, "1.2.3", "0.x", "foo", "abc def"]
)
def test_find_version_info_in_manifest_file(version_name, patch_name, label):
    manifest_contents = {"manifest_version": 1, "version": {}}

    if all(value is None for value in (version_name, patch_name, label)):
        manifest_contents.pop("version")
    if version_name is not None:
        manifest_contents["version"]["name"] = version_name
    if patch_name is not None:
        manifest_contents["version"]["patch"] = patch_name
    if label is not None:
        manifest_contents["version"]["label"] = label

    deploy_root_structure = {"manifest.yml": safe_dump(manifest_contents)}
    with temp_local_dir(deploy_root_structure) as deploy_root:
        v, p, l = find_version_info_in_manifest_file(deploy_root=deploy_root)
        if version_name is None:
            assert v is None
        else:
            assert v == to_identifier(str(version_name))

        if patch_name is None:
            assert p is None
        else:
            assert p == int(patch_name)

        if label is None:
            assert l is None
        else:
            assert l == label


@pytest.mark.parametrize("version_info", ["some_name", ["li1", "li2"], "", 4])
def test_bad_version_info_in_manifest_file_throws_error(version_info):
    manifest_contents = ManifestFactory(version=version_info)

    deploy_root_structure = {"manifest.yml": manifest_contents}
    with temp_local_dir(deploy_root_structure) as deploy_root:
        with pytest.raises(ClickException) as err:
            find_version_info_in_manifest_file(deploy_root=deploy_root)
        assert (
            err.value.message
            == "Error occurred while reading manifest.yml. Received unexpected version format."
        )


def test_read_empty_version_info_in_manifest_file():
    manifest_contents = ManifestFactory(
        version={"name": None, "patch": None, "label": None}
    )

    deploy_root_structure = {"manifest.yml": manifest_contents}
    with temp_local_dir(deploy_root_structure) as deploy_root:

        version_info = find_version_info_in_manifest_file(deploy_root=deploy_root)
        assert version_info == VersionInfo(
            version_name=None, patch_number=None, label=None
        )


def test_read_empty_version_in_manifest_file():
    manifest_contents = ManifestFactory(version=None)

    deploy_root_structure = {"manifest.yml": manifest_contents}
    with temp_local_dir(deploy_root_structure) as deploy_root:

        version_info = find_version_info_in_manifest_file(deploy_root=deploy_root)
        assert version_info == VersionInfo(
            version_name=None, patch_number=None, label=None
        )


@pytest.mark.parametrize(
    "configuration_section, expected_output",
    [
        [
            None,
            [],
        ],
        [
            "test",
            [],
        ],
        [
            ["test"],
            [],
        ],
        [
            {},
            [],
        ],
        [
            {"telemetry_event_definitions": "test"},
            [],
        ],
        [
            {"telemetry_event_definitions": {"key": "value"}},
            [],
        ],
        [
            {"telemetry_event_definitions": []},
            [],
        ],
        [
            {
                "telemetry_event_definitions": [
                    {"type": "ERRORS_AND_WARNINGS", "sharing": "MANDATORY"},
                    {"type": "DEBUG_LOGS", "sharing": "OPTIONAL"},
                ]
            },
            [
                {
                    "name": "SNOWFLAKE$ERRORS_AND_WARNINGS",
                    "type": "ERRORS_AND_WARNINGS",
                    "sharing": "MANDATORY",
                },
                {
                    "name": "SNOWFLAKE$DEBUG_LOGS",
                    "type": "DEBUG_LOGS",
                    "sharing": "OPTIONAL",
                },
            ],
        ],
    ],
)
def test_find_events_in_manifest_file(configuration_section, expected_output):
    manifest_contents = {"manifest_version": 1, "version": {"name": "v1", "patch": 1}}
    manifest_contents["configuration"] = configuration_section

    deploy_root_structure = {"manifest.yml": safe_dump(manifest_contents)}
    with temp_local_dir(deploy_root_structure) as deploy_root:
        assert (
            find_events_definitions_in_manifest_file(deploy_root=deploy_root)
            == expected_output
        )
