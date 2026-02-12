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

import pytest
import yaml
from snowflake.cli._plugins.dcm.manifest import (
    DCM_PROJECT_TYPE,
    MANIFEST_FILE_NAME,
    DCMManifest,
    DCMTarget,
    DCMTemplating,
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli.api.secure_path import SecurePath


class TestDCMManifest:
    def test_manifest_from_dict_minimal(self):
        data = {"manifest_version": "2.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        assert manifest.manifest_version == "2.0"
        assert manifest.project_type == "dcm_project"
        assert manifest.default_target is None
        assert manifest.targets == {}
        assert manifest.templating.defaults == {}
        assert manifest.templating.configurations == {}

    def test_manifest_from_dict_with_targets(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {
                    "project_name": "DB.SCHEMA.PROJECT_DEV",
                    "templating_config": "dev",
                },
                "PROD": {
                    "project_name": "DB.SCHEMA.PROJECT_PROD",
                    "templating_config": "prod",
                },
            },
            "templating": {
                "configurations": {
                    "dev": {"suffix": "_dev"},
                    "prod": {"suffix": ""},
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        assert manifest.default_target == "DEV"
        assert len(manifest.targets) == 2
        assert manifest.targets["DEV"].project_name == "DB.SCHEMA.PROJECT_DEV"
        assert manifest.targets["DEV"].templating_config == "dev"
        assert manifest.targets["PROD"].project_name == "DB.SCHEMA.PROJECT_PROD"
        assert manifest.targets["PROD"].templating_config == "prod"

    def test_manifest_from_dict_with_templating(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "templating": {
                "defaults": {"db_name": "shared_db", "retry_count": 3},
                "configurations": {
                    "dev": {"wh_size": "XSMALL", "suffix": "_dev"},
                    "prod": {"wh_size": "LARGE", "suffix": ""},
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        assert manifest.manifest_version == "2.0"
        assert manifest.project_type == "dcm_project"
        assert manifest.templating.defaults == {
            "db_name": "shared_db",
            "retry_count": 3,
        }
        assert manifest.templating.configurations == {
            "dev": {"wh_size": "XSMALL", "suffix": "_dev"},
            "prod": {"wh_size": "LARGE", "suffix": ""},
        }

    def test_manifest_get_configuration_names(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "templating": {
                "configurations": {
                    "dev": {"suffix": "_dev"},
                    "staging": {"suffix": "_stg"},
                    "prod": {"suffix": ""},
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        config_names = manifest.get_configuration_names()
        assert set(config_names) == {"dev", "staging", "prod"}

    def test_manifest_get_target_names(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target_names = manifest.get_target_names()
        assert set(target_names) == {"DEV", "PROD"}

    def test_manifest_get_target(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "DB.SCHEMA.PROJECT_DEV"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_target("DEV")
        assert target.project_name == "DB.SCHEMA.PROJECT_DEV"

    def test_manifest_get_target_not_found(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {},
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError, match="Target 'UNKNOWN' not found in manifest"
        ):
            manifest.get_target("UNKNOWN")

    def test_manifest_get_effective_target_explicit(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_effective_target("PROD")
        assert target.project_name == "P2"

    def test_manifest_get_effective_target_uses_default(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_effective_target()
        assert target.project_name == "P1"

    def test_manifest_get_effective_target_no_default(self):
        """When multiple targets exist and no default_target is defined, should raise error."""
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError,
            match="No target specified and no default_target defined",
        ):
            manifest.get_effective_target()

    def test_manifest_single_target_auto_default(self):
        """When only one target exists and no default_target is defined, it should be auto-selected."""
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_effective_target()
        assert target.project_name == "P1"

    def test_manifest_validate_success(self):
        data = {"manifest_version": "2.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

    def test_manifest_validate_with_targets_success(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {"project_name": "P1", "templating_config": "dev"},
            },
            "templating": {"configurations": {"dev": {}}},
        }
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

    def test_manifest_validate_missing_type(self):
        data = {"manifest_version": "2.0", "type": ""}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError, match="Manifest file type is undefined"
        ):
            manifest.validate()

    def test_manifest_validate_wrong_type(self):
        data = {"manifest_version": "2.0", "type": "wrong_type"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError, match="Manifest file is defined for type wrong_type"
        ):
            manifest.validate()

    def test_manifest_validate_wrong_version(self):
        data = {"manifest_version": "1.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError,
            match="Manifest version '1.0' is not supported.*>= 2.0 and < 3.0",
        ):
            manifest.validate()

    def test_manifest_validate_version_3_not_supported(self):
        data = {"manifest_version": "3.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError,
            match="Manifest version '3.0' is not supported.*>= 2.0 and < 3.0",
        ):
            manifest.validate()

    @pytest.mark.parametrize("version", ["2", "2.0", "2.1", "2.5", "2.99"])
    def test_manifest_validate_valid_versions(self, version):
        data = {"manifest_version": version, "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

    def test_manifest_get_target_unknown_configuration(self):
        """Configuration validation happens when getting target, not during validate()."""
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {"DEV": {"project_name": "P1", "templating_config": "unknown"}},
            "templating": {"configurations": {"dev": {}}},
        }
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

        with pytest.raises(
            ManifestConfigurationError,
            match="Target 'DEV' references unknown configuration 'unknown'",
        ):
            manifest.get_target("DEV")


class TestDCMTemplating:
    def test_templating_from_dict_none(self):
        templating = DCMTemplating.from_dict(None)

        assert templating.defaults == {}
        assert templating.configurations == {}

    def test_templating_from_dict_empty(self):
        templating = DCMTemplating.from_dict({})

        assert templating.defaults == {}
        assert templating.configurations == {}

    def test_templating_from_dict_with_data(self):
        data = {
            "defaults": {"key": "value"},
            "configurations": {"dev": {"suffix": "_dev"}},
        }
        templating = DCMTemplating.from_dict(data)

        assert templating.defaults == {"key": "value"}
        assert templating.configurations == {"dev": {"suffix": "_dev"}}


class TestDCMTarget:
    def test_target_from_dict_minimal(self):
        data = {"project_name": "DB.SCHEMA.MY_PROJECT"}
        target = DCMTarget.from_dict(data)

        assert target.project_name == "DB.SCHEMA.MY_PROJECT"
        assert target.templating_config is None

    def test_target_from_dict_full(self):
        data = {
            "project_name": "DB.SCHEMA.MY_PROJECT",
            "templating_config": "dev",
        }
        target = DCMTarget.from_dict(data)

        assert target.project_name == "DB.SCHEMA.MY_PROJECT"
        assert target.templating_config == "dev"


class TestLoadManifest:
    def test_raises_when_manifest_file_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            with pytest.raises(
                ManifestNotFoundError,
                match=f"{MANIFEST_FILE_NAME} was not found in directory",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_file_is_empty(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            (project_dir / MANIFEST_FILE_NAME).touch()
            with pytest.raises(
                InvalidManifestError,
                match="Manifest file is empty or invalid",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_file_has_no_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "2.0", "definition": "v1"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_file_has_wrong_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "2.0", "type": "spcs"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=f"Manifest file is defined for type spcs. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_version_is_invalid(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "1", "type": "dcm_project"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=r"Manifest version '1' is not supported.*>= 2.0 and < 3.0",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_version_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"type": "dcm_project"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=r"Manifest version '' is not supported.*>= 2.0 and < 3.0",
            ):
                DCMManifest.load(SecurePath(project_dir))
