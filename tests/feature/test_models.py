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
"""FSManifest is parsed the same way as DCMManifest (SecurePath + yaml.safe_load)."""

import pytest
import yaml
from snowflake.cli._plugins.feature.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli._plugins.feature.models import (
    FEATURE_STORE_PROJECT_TYPE,
    MANIFEST_FILE_NAME,
    FSManifest,
    FSProjectPaths,
    FSTarget,
    FSTemplating,
    find_project_root,
)
from snowflake.cli.api.secure_path import SecurePath

_DEFAULT_TARGET_FIELDS = {
    "account_identifier": "MY_ORG-MY_ACCOUNT",
    "database": "MY_DB",
    "schema": "MY_SCHEMA",
}


def _write_manifest(project_dir, data: dict) -> None:
    with open(project_dir / MANIFEST_FILE_NAME, "w") as f:
        yaml.dump(data, f, sort_keys=False)


class TestFSManifestFromDict:
    def test_manifest_from_dict_minimal(self):
        data = {"manifest_version": 1, "type": "feature_store"}
        manifest = FSManifest.from_dict(data)

        assert manifest.manifest_version == 1
        assert manifest.project_type == FEATURE_STORE_PROJECT_TYPE
        assert manifest.default_target is None
        assert manifest.targets == {}
        assert manifest.templating.defaults == {}
        assert manifest.templating.configurations == {}

    def test_manifest_from_dict_with_targets(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "default_target": "DEV",
            "targets": {
                "DEV": {
                    "templating_config": "dev",
                    **_DEFAULT_TARGET_FIELDS,
                },
                "PROD": {
                    "templating_config": "prod",
                    **_DEFAULT_TARGET_FIELDS,
                },
            },
            "templating": {
                "configurations": {
                    "dev": {"row_limit": 10},
                    "prod": {"row_limit": 100},
                },
            },
        }
        manifest = FSManifest.from_dict(data)

        assert manifest.default_target == "DEV"
        assert len(manifest.targets) == 2
        assert manifest.targets["DEV"].database == "MY_DB"
        assert manifest.targets["DEV"].templating_config == "DEV"
        assert manifest.targets["PROD"].templating_config == "PROD"

    def test_derives_default_target_from_single_target(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {"sandbox": {**_DEFAULT_TARGET_FIELDS}},
        }
        manifest = FSManifest.from_dict(data)
        assert manifest.default_target == "SANDBOX"

    def test_templating_configuration_names_are_uppercased(self):
        templating = FSTemplating.from_dict(
            {"defaults": {"row_limit": 1}, "configurations": {"dev": {"row_limit": 10}}}
        )
        assert templating.configurations == {"DEV": {"row_limit": 10}}

    def test_get_target_unknown_raises(self):
        manifest = FSManifest.from_dict(
            {
                "manifest_version": 1,
                "type": "feature_store",
                "targets": {"DEV": {**_DEFAULT_TARGET_FIELDS}},
            }
        )
        with pytest.raises(
            ManifestConfigurationError, match="Target 'UNKNOWN' not found in manifest"
        ):
            manifest.get_target("UNKNOWN")

    def test_get_target_unknown_templating_config_raises(self):
        manifest = FSManifest.from_dict(
            {
                "manifest_version": 1,
                "type": "feature_store",
                "targets": {
                    "DEV": {
                        **_DEFAULT_TARGET_FIELDS,
                        "templating_config": "MISSING",
                    }
                },
            }
        )
        with pytest.raises(
            ManifestConfigurationError,
            match="references unknown configuration 'MISSING'",
        ):
            manifest.get_target("DEV")

    def test_get_target_missing_required_fields_raises(self):
        manifest = FSManifest.from_dict(
            {
                "manifest_version": 1,
                "type": "feature_store",
                "targets": {"DEV": {"account_identifier": "ORG-ACCT"}},
            }
        )
        with pytest.raises(
            ManifestConfigurationError,
            match="missing required field\\(s\\): database, schema",
        ):
            manifest.get_target("DEV")

    def test_warehouse_on_target_is_rejected(self):
        with pytest.raises(
            ManifestConfigurationError, match="unsupported field 'warehouse'"
        ):
            FSTarget.from_dict(
                {
                    "name": "DEV",
                    **_DEFAULT_TARGET_FIELDS,
                    "warehouse": "WH",
                }
            )

    def test_get_effective_target_uses_default(self):
        manifest = FSManifest.from_dict(
            {
                "manifest_version": 1,
                "type": "feature_store",
                "default_target": "PROD",
                "targets": {
                    "DEV": {**_DEFAULT_TARGET_FIELDS, "database": "DEV_DB"},
                    "PROD": {**_DEFAULT_TARGET_FIELDS, "database": "PROD_DB"},
                },
            }
        )
        assert manifest.get_effective_target(None).name == "PROD"
        assert manifest.get_effective_target("dev").name == "DEV"


class TestFSManifestFromDictTypeGuards:
    """A malformed ``manifest.yml`` (wrong YAML *types*) must raise
    ``InvalidManifestError`` — mapped to ``CliError`` by
    ``_resolve_project`` — not leak a raw ``AttributeError`` / ``TypeError``
    traceback. These run without the ML wheel (``test_models.py`` is in
    ``conftest._NO_DEP_SAFE``)."""

    def _valid_data(self) -> dict:
        return {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {"DEV": {**_DEFAULT_TARGET_FIELDS}},
        }

    def test_top_level_payload_must_be_mapping(self):
        with pytest.raises(InvalidManifestError):
            FSManifest.from_dict(["manifest_version", 1])  # type: ignore[arg-type]

    def test_targets_as_list_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": ["DEFAULT"],
        }
        with pytest.raises(InvalidManifestError, match="targets"):
            FSManifest.from_dict(data)

    def test_targets_as_string_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": "DEFAULT",
        }
        with pytest.raises(InvalidManifestError, match="targets"):
            FSManifest.from_dict(data)

    def test_target_value_as_list_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {"DEV": ["not", "a", "mapping"]},
        }
        with pytest.raises(InvalidManifestError, match="DEV"):
            FSManifest.from_dict(data)

    def test_target_value_as_string_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {"DEV": "not-a-mapping"},
        }
        with pytest.raises(InvalidManifestError, match="DEV"):
            FSManifest.from_dict(data)

    def test_non_string_target_key_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {1: {**_DEFAULT_TARGET_FIELDS}},
        }
        with pytest.raises(InvalidManifestError):
            FSManifest.from_dict(data)

    def test_templating_config_non_string_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {
                "DEV": {**_DEFAULT_TARGET_FIELDS, "templating_config": 1},
            },
        }
        with pytest.raises(InvalidManifestError, match="templating_config"):
            FSManifest.from_dict(data)

    def test_account_identifier_non_string_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {
                "DEV": {
                    "account_identifier": 123,
                    "database": "MY_DB",
                    "schema": "MY_SCHEMA",
                },
            },
        }
        with pytest.raises(InvalidManifestError, match="account_identifier"):
            FSManifest.from_dict(data)

    def test_database_non_string_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {
                "DEV": {
                    "account_identifier": "MY_ORG-MY_ACCOUNT",
                    "database": ["MY_DB"],
                    "schema": "MY_SCHEMA",
                },
            },
        }
        with pytest.raises(InvalidManifestError, match="database"):
            FSManifest.from_dict(data)

    def test_role_non_string_raises(self):
        data = {
            "manifest_version": 1,
            "type": "feature_store",
            "targets": {
                "DEV": {**_DEFAULT_TARGET_FIELDS, "role": ["R"]},
            },
        }
        with pytest.raises(InvalidManifestError, match="role"):
            FSManifest.from_dict(data)

    def test_fstarget_from_dict_non_mapping_raises(self):
        with pytest.raises(InvalidManifestError):
            FSTarget.from_dict(["not", "a", "mapping"])  # type: ignore[arg-type]

    def test_missing_targets_still_yields_empty(self):
        """Absent ``targets`` is not malformed — it stays an empty map."""
        manifest = FSManifest.from_dict(
            {"manifest_version": 1, "type": "feature_store"}
        )
        assert manifest.targets == {}


class TestLoadManifest:
    def test_raises_when_manifest_file_is_missing(self, tmp_path):
        with pytest.raises(
            ManifestNotFoundError,
            match=f"{MANIFEST_FILE_NAME} was not found in directory",
        ):
            FSManifest.load(SecurePath(tmp_path))

    def test_raises_when_manifest_file_is_empty(self, tmp_path):
        (tmp_path / MANIFEST_FILE_NAME).touch()
        with pytest.raises(
            InvalidManifestError,
            match="Manifest file is empty or invalid",
        ):
            FSManifest.load(SecurePath(tmp_path))

    def test_raises_when_manifest_file_has_no_type(self, tmp_path):
        _write_manifest(tmp_path, {"manifest_version": 1})
        with pytest.raises(
            InvalidManifestError,
            match=f"Manifest file type is undefined. Expected {FEATURE_STORE_PROJECT_TYPE}",
        ):
            FSManifest.load(SecurePath(tmp_path))

    def test_raises_when_manifest_file_has_wrong_type(self, tmp_path):
        _write_manifest(tmp_path, {"manifest_version": 1, "type": "dcm_project"})
        with pytest.raises(
            InvalidManifestError,
            match=f"Manifest file is defined for type dcm_project. Expected {FEATURE_STORE_PROJECT_TYPE}",
        ):
            FSManifest.load(SecurePath(tmp_path))

    def test_raises_when_manifest_version_is_invalid(self, tmp_path):
        _write_manifest(tmp_path, {"manifest_version": 2, "type": "feature_store"})
        with pytest.raises(
            InvalidManifestError,
            match=r"Manifest version 2 is not supported. Expected version 1.",
        ):
            FSManifest.load(SecurePath(tmp_path))

    def test_raises_when_manifest_version_is_missing(self, tmp_path):
        _write_manifest(tmp_path, {"type": "feature_store"})
        with pytest.raises(
            InvalidManifestError,
            match=r"Manifest version is undefined.",
        ):
            FSManifest.load(SecurePath(tmp_path))

    def test_load_round_trip(self, tmp_path):
        _write_manifest(
            tmp_path,
            {
                "manifest_version": 1,
                "type": "feature_store",
                "default_target": "DEFAULT",
                "targets": {"DEFAULT": {**_DEFAULT_TARGET_FIELDS, "role": "MY_ROLE"}},
            },
        )
        manifest = FSManifest.load(SecurePath(tmp_path))
        target = manifest.get_effective_target()
        assert target.database == "MY_DB"
        assert target.role == "MY_ROLE"


class TestFindProjectRoot:
    """``find_project_root`` matches ``DCMManifest.load``: the given
    directory (``--from`` / cwd) must itself contain ``manifest.yml``.
    Ancestors are never searched, so a project can never silently bind to
    a parent ``manifest.yml``."""

    def test_returns_directory_when_manifest_present(self, tmp_path):
        (tmp_path / MANIFEST_FILE_NAME).touch()
        assert find_project_root(tmp_path) == tmp_path.resolve()

    def test_uses_parent_dir_when_start_is_a_file(self, tmp_path):
        (tmp_path / MANIFEST_FILE_NAME).touch()
        some_file = tmp_path / "notes.txt"
        some_file.touch()
        # A file argument resolves against its containing directory.
        assert find_project_root(some_file) == tmp_path.resolve()

    def test_does_not_walk_up_to_parent_manifest(self, tmp_path):
        (tmp_path / MANIFEST_FILE_NAME).touch()
        nested = tmp_path / "sub" / "deep"
        nested.mkdir(parents=True)
        with pytest.raises(ManifestNotFoundError):
            find_project_root(nested)

    def test_missing_manifest_error_names_only_that_directory(self, tmp_path):
        with pytest.raises(ManifestNotFoundError) as excinfo:
            find_project_root(tmp_path)
        msg = str(excinfo.value)
        assert MANIFEST_FILE_NAME in msg
        assert "ancestor" not in msg.lower()

    def test_discover_does_not_walk_up(self, tmp_path):
        (tmp_path / MANIFEST_FILE_NAME).touch()
        nested = tmp_path / "sub"
        nested.mkdir()
        with pytest.raises(ManifestNotFoundError):
            FSProjectPaths.discover(nested)
