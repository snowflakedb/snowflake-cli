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
    DCMTarget,
    DCMTemplating,
)
from snowflake.cli.api.secure_path import SecurePath

_DEFAULT_TARGET_FIELDS = {
    "account_identifier": "MY_ORG-MY_ACCOUNT",
    "project_owner": "MY_ROLE",
}


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
        assert manifest.templating.env_vars == []
        assert manifest.templating.env_secrets == []
        assert manifest.templating.declared_variable_names == set()

    def test_manifest_from_dict_with_targets(self):
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {
                    "project_name": "DB.SCHEMA.PROJECT_DEV",
                    "templating_config": "dev",
                    **_DEFAULT_TARGET_FIELDS,
                },
                "PROD": {
                    "project_name": "DB.SCHEMA.PROJECT_PROD",
                    "templating_config": "prod",
                    **_DEFAULT_TARGET_FIELDS,
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
        # templating is declared here, but without env_vars/env_secrets keys
        # at all (as opposed to declared-empty) -- must still default cleanly.
        assert manifest.templating.env_vars == []
        assert manifest.templating.env_secrets == []
        assert manifest.templating.declared_variable_names == set()

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
                "env_vars": [{"DB_HOST": None}, {"WH_SIZE": None}],
                "env_secrets": [{"AWS_SECRET_KEY": None}],
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
        assert manifest.templating.env_vars == ["DB_HOST", "WH_SIZE"]
        assert manifest.templating.env_secrets == ["AWS_SECRET_KEY"]
        assert manifest.templating.declared_variable_names == {
            "DB_HOST",
            "WH_SIZE",
            "AWS_SECRET_KEY",
        }

    def test_templating_declared_variable_names_env_vars_only(self):
        """env_secrets key is entirely absent here, not just empty."""
        templating = DCMTemplating.from_dict(
            {"env_vars": [{"DB_HOST": None}, {"WH_SIZE": None}]}
        )

        assert templating.env_secrets == []
        assert templating.declared_variable_names == {"DB_HOST", "WH_SIZE"}

    def test_templating_declared_variable_names_env_secrets_only(self):
        """env_vars key is entirely absent here, not just empty."""
        templating = DCMTemplating.from_dict(
            {"env_secrets": [{"AWS_SECRET_KEY": None}]}
        )

        assert templating.env_vars == []

        assert templating.declared_variable_names == {"AWS_SECRET_KEY"}

    def test_templating_rejects_name_in_both_env_vars_and_env_secrets(self):
        """Matches GS's EnvVarsValidator: a name can't be declared in both
        lists at once."""
        with pytest.raises(InvalidManifestError, match="SHARED_NAME"):
            DCMTemplating.from_dict(
                {
                    "env_vars": [{"SHARED_NAME": None}],
                    "env_secrets": [{"SHARED_NAME": None}],
                }
            )

    def test_templating_rejects_duplicate_name_within_env_vars(self):
        with pytest.raises(InvalidManifestError, match="DB_HOST"):
            DCMTemplating.from_dict(
                {"env_vars": [{"DB_HOST": None}, {"DB_HOST": None}]}
            )

    def test_templating_rejects_duplicate_name_within_env_secrets(self):
        with pytest.raises(InvalidManifestError, match="API_KEY"):
            DCMTemplating.from_dict(
                {"env_secrets": [{"API_KEY": None}, {"API_KEY": None}]}
            )

    def test_templating_declared_variable_names_case_preserved(self):
        """Unlike configuration names, env var names are matched by exact
        string equality server-side, so case must be preserved."""
        templating = DCMTemplating.from_dict({"env_vars": [{"db_Host": None}]})

        assert templating.declared_variable_names == {"db_Host"}
        assert "DB_HOST" not in templating.declared_variable_names

    def test_templating_different_case_names_are_not_duplicates(self):
        """DB_HOST and db_host are two distinct declared names (exact
        string equality) -- declaring both is not a duplicate, even though
        a case-insensitive comparison would treat them the same."""
        templating = DCMTemplating.from_dict(
            {"env_vars": [{"DB_HOST": None}, {"db_host": None}]}
        )

        assert templating.env_vars == ["DB_HOST", "db_host"]

    def test_templating_env_vars_each_entry_is_a_single_key_mapping(self):
        """Real manifest shape: each `env_vars`/`env_secrets` entry is a
        single-key mapping (`- BUILD_NUMBER:` in YAML), matching GS's
        EnvVarDefinition/EnvSecretDefinition -- currently-empty placeholders
        reserved for future per-variable properties. The key is the declared
        name; the value (always None today) is ignored."""
        templating = DCMTemplating.from_dict(
            {
                "env_vars": [{"BUILD_NUMBER": None}, {"INCLUDE_REPORTS": None}],
                "env_secrets": [{"API_KEY": None}],
            }
        )

        assert templating.env_vars == ["BUILD_NUMBER", "INCLUDE_REPORTS"]
        assert templating.env_secrets == ["API_KEY"]

    def test_templating_rejects_multi_key_entry(self):
        """Each list entry must declare exactly one name -- matches the
        server's own schema constraint."""
        with pytest.raises(InvalidManifestError, match="exactly one name"):
            DCMTemplating.from_dict({"env_vars": [{"FIRST": None, "SECOND": None}]})

    @pytest.mark.parametrize(
        "bad_name",
        [
            "1STARTS_WITH_DIGIT",
            "has-hyphen",
            "has space",
            "has.dot",
            "",
        ],
    )
    def test_templating_rejects_invalid_env_var_name_format(self, bad_name):
        """Matches GS's EnvVarsValidator name-pattern regex, the POSIX
        portable character set (^[a-zA-Z_][a-zA-Z0-9_]*$)."""
        with pytest.raises(InvalidManifestError, match="POSIX"):
            DCMTemplating.from_dict({"env_vars": [{bad_name: None}]})

    def test_templating_accepts_valid_env_var_name_formats(self):
        """Underscore-prefixed and all-underscore names are valid; only
        digit-first and non-word characters are rejected."""
        templating = DCMTemplating.from_dict(
            {"env_vars": [{"_LEADING_UNDERSCORE": None}, {"trailing_digit1": None}]}
        )

        assert templating.env_vars == ["_LEADING_UNDERSCORE", "trailing_digit1"]

    def test_templating_rejects_reserved_snow_name(self):
        """_snow is reserved for the built-in Jinja context object."""
        with pytest.raises(InvalidManifestError, match="reserved"):
            DCMTemplating.from_dict({"env_secrets": [{"_snow": None}]})

    def test_templating_rejects_non_string_env_var_name(self):
        """A bare (unquoted) numeric-looking YAML key parses as an int,
        not a string -- must fail with a clear manifest error, not a raw
        TypeError from the name-pattern regex match."""
        with pytest.raises(InvalidManifestError, match="must be a string"):
            DCMTemplating.from_dict({"env_vars": [{123: None}]})

    def test_templating_env_vars_rejects_bare_string_entry(self):
        """Real manifest shape requires a single-key mapping (`- NAME:`).
        GS rejects a bare string entry at the schema level, so the CLI
        should fail the same way, fast and client-side."""
        with pytest.raises(InvalidManifestError, match="env_vars"):
            DCMTemplating.from_dict({"env_vars": ["BUILD_NUMBER"]})

    def test_templating_env_secrets_rejects_bare_string_entry(self):
        with pytest.raises(InvalidManifestError, match="env_secrets"):
            DCMTemplating.from_dict({"env_secrets": ["API_KEY"]})

    def test_templating_env_vars_section_present_but_empty(self):
        """`env_vars:`/`env_secrets:` declared with no entries parses as
        None via PyYAML, not []; from_dict must coerce it rather than
        blow up in _declared_names' `for entry in None` loop."""
        templating = DCMTemplating.from_dict({"env_vars": None, "env_secrets": None})

        assert templating.env_vars == []
        assert templating.env_secrets == []
        assert templating.declared_variable_names == set()

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
                "DEV": {"project_name": "P1", **_DEFAULT_TARGET_FIELDS},
                "PROD": {"project_name": "P2", **_DEFAULT_TARGET_FIELDS},
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
                "DEV": {"project_name": "P1", **_DEFAULT_TARGET_FIELDS},
                "PROD": {"project_name": "P2", **_DEFAULT_TARGET_FIELDS},
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
                "DEV": {"project_name": "P1", **_DEFAULT_TARGET_FIELDS},
                "PROD": {"project_name": "P2", **_DEFAULT_TARGET_FIELDS},
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
                "DEV": {"project_name": "P1", **_DEFAULT_TARGET_FIELDS},
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
                "DEV": {
                    "project_name": "P1",
                    "templating_config": "dev",
                    **_DEFAULT_TARGET_FIELDS,
                },
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
                "dEv": {
                    "project_name": "P1",
                    "templating_config": "DEV_config",
                    **_DEFAULT_TARGET_FIELDS,
                },
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
            "targets": {
                "DEV": {
                    "project_name": "P1",
                    "templating_config": "unknown",
                    **_DEFAULT_TARGET_FIELDS,
                }
            },
            "templating": {"configurations": {"dev": {}}},
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError,
            match="Target 'DEV' references unknown configuration 'UNKNOWN'",
        ):
            manifest.get_target("DEV")

    def test_get_target_missing_project_name(self):
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "targets": {
                "DEV": {
                    "account_identifier": "MY_ORG-MY_ACCOUNT",
                    "project_owner": "MY_ROLE",
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError,
            match="Target 'DEV' is missing required field\\(s\\): project_name",
        ):
            manifest.get_target("DEV")

    def test_get_target_missing_account_identifier_does_not_raise(self):
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1", "project_owner": "MY_ROLE"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_target("DEV")
        assert target.account_identifier == ""

    def test_get_target_missing_project_owner_does_not_raise(self):
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "targets": {
                "DEV": {
                    "project_name": "P1",
                    "account_identifier": "MY_ORG-MY_ACCOUNT",
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_target("DEV")
        assert target.project_owner == ""

    def test_get_target_missing_all_required_fields(self):
        data = {
            "manifest_version": 2,
            "type": "dcm_project",
            "targets": {
                "DEV": {},
            },
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError,
            match="Target 'DEV' is missing required field\\(s\\): project_name",
        ):
            manifest.get_target("DEV")

    def test_account_identifier_preserved_as_written(self):
        target = DCMTarget.from_dict(
            {
                "name": "dev",
                "project_name": "P1",
                "account_identifier": "my_org-my_account",
                "project_owner": "my_role",
            }
        )
        assert target.account_identifier == "my_org-my_account"

    def test_project_owner_unquoted_preserved_unchanged(self):
        target = DCMTarget.from_dict(
            {
                "name": "dev",
                "project_name": "P1",
                "account_identifier": "my_org-my_account",
                "project_owner": "my_role",
            }
        )
        assert target.project_owner == "my_role"

    def test_project_owner_with_space_gets_quoted(self):
        target = DCMTarget.from_dict(
            {
                "name": "dev",
                "project_name": "P1",
                "account_identifier": "my_org-my_account",
                "project_owner": "my role",
            }
        )
        assert target.project_owner == '"my role"'

    def test_project_owner_quoted_preserved(self):
        target = DCMTarget.from_dict(
            {
                "name": "dev",
                "project_name": "P1",
                "account_identifier": "my_org-my_account",
                "project_owner": '"my role"',
            }
        )
        assert target.project_owner == '"my role"'

    def test_account_identifier_with_dot_separator_preserved(self):
        target = DCMTarget.from_dict(
            {
                "name": "dev",
                "project_name": "P1",
                "account_identifier": "my_org.my_account",
                "project_owner": "my_role",
            }
        )
        assert target.account_identifier == "my_org.my_account"


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
