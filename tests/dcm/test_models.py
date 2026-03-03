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
from snowflake.cli._plugins.dcm.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli._plugins.dcm.models import (
    DCM_PROJECT_TYPE,
    MANIFEST_FILE_NAME,
    DCMManifest,
)
from snowflake.cli.api.secure_path import SecurePath


class TestDCMManifest:
    def test_manifest_from_dict_minimal(self):
        data = {"manifest_version": 2, "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        assert manifest.manifest_version == 2
        assert manifest.project_type == "dcm_project"
        assert manifest.default_target is None
        assert manifest.targets == {}
        assert manifest.templating.defaults == {}
        assert manifest.templating.configurations == {}

    def test_manifest_from_dict_with_targets(self):
        data = {
            "manifest_version": 2,
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
        assert manifest.targets["DEV"].templating_config == "DEV"
        assert manifest.targets["PROD"].project_name == "DB.SCHEMA.PROJECT_PROD"
        assert manifest.targets["PROD"].templating_config == "PROD"

    def test_manifest_from_dict_with_templating(self):
        data = {
            "manifest_version": 2,
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

        assert manifest.manifest_version == 2
        assert manifest.project_type == "dcm_project"
        assert manifest.templating.defaults == {
            "db_name": "shared_db",
            "retry_count": 3,
        }
        assert manifest.templating.configurations == {
            "DEV": {"wh_size": "XSMALL", "suffix": "_dev"},
            "PROD": {"wh_size": "LARGE", "suffix": ""},
        }

    def test_manifest_get_target_not_found(self):
        data = {
            "manifest_version": 2,
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
            "manifest_version": 2,
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
            "manifest_version": 2,
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
            "manifest_version": 2,
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
            "manifest_version": 2,
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_effective_target()
        assert target.project_name == "P1"

    def test_manifest_validate_success(self):
        data = {"manifest_version": 2, "type": "dcm_project"}
        DCMManifest.from_dict(data)

    def test_manifest_validate_with_targets_success(self):
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {"project_name": "P1", "templating_config": "dev"},
            },
            "templating": {"configurations": {"dev": {}}},
        }
        DCMManifest.from_dict(data)

    def test_manifest_with_case_insensitive_keys(self):
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "default_target": "Dev",
            "targets": {
                "dEv": {"project_name": "P1", "templating_config": "DEV_config"},
            },
            "templating": {"configurations": {"dev_CONFIG": {}}},
        }
        manifest = DCMManifest.from_dict(data)
        assert manifest.get_effective_target("DEV").templating_config == "DEV_CONFIG"

    def test_manifest_validate_missing_type(self):
        data = {"manifest_version": 2, "type": ""}

        with pytest.raises(
            InvalidManifestError, match="Manifest file type is undefined"
        ):
            DCMManifest.from_dict(data)

    def test_manifest_validate_wrong_type(self):
        data = {"manifest_version": 2, "type": "wrong_type"}

        with pytest.raises(
            InvalidManifestError, match="Manifest file is defined for type wrong_type"
        ):
            DCMManifest.from_dict(data)

    @pytest.mark.parametrize("version", [1, 3])
    def test_manifest_validate_version_not_supported(self, version):
        data = {"manifest_version": version, "type": "dcm_project"}

        with pytest.raises(
            InvalidManifestError,
            match=f"Manifest version {version} is not supported. Expected version 2.",
        ):
            DCMManifest.from_dict(data)

    def test_manifest_validate_invalid_version_string(self):
        data = {"manifest_version": "2.0", "type": "dcm_project"}

        with pytest.raises(
            InvalidManifestError,
            match="Manifest version '2.0' is not valid. Expected an integer.",
        ):
            DCMManifest.from_dict(data)

    def test_manifest_get_target_unknown_configuration(self):
        """Configuration validation happens when getting target, not during from_dict()."""
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "targets": {"DEV": {"project_name": "P1", "templating_config": "unknown"}},
            "templating": {"configurations": {"dev": {}}},
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError,
            match="Target 'DEV' references unknown configuration 'UNKNOWN'",
        ):
            manifest.get_target("DEV")


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
                yaml.dump({"manifest_version": 2, "definition": "v1"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_file_has_wrong_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": 2, "type": "spcs"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=f"Manifest file is defined for type spcs. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_version_is_invalid(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": 1, "type": "dcm_project"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=r"Manifest version 1 is not supported. Expected version 2.",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_version_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"type": "dcm_project"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=r"Manifest version is undefined.",
            ):
                DCMManifest.load(SecurePath(project_dir))
