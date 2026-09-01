# Copyright (c) 2026 Snowflake Inc.
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

from textwrap import dedent
from unittest.mock import Mock, patch

import pytest
import yaml
from snowflake.cli._plugins.apps.app_yml import (
    APP_YML_FILENAME,
    AppYmlDefinition,
    AppYmlTarget,
    load_app_yml,
    resolve_target,
)
from snowflake.cli._plugins.apps.manager import SnowflakeAppManager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN

_CNG_FLAG = FeatureFlag.ENABLE_APP_SERVICE_COMPUTE_RESOURCE

_APP_YML = dedent(
    """\
    version: 2
    database: USER$
    schema: PUBLIC
    name: MY_APP
    query_warehouse: APP_WH
    package_name: MY_APP
    artifact_repo: SNOWFLAKE_APPS.PUBLIC.MY_APP_REPO
    build_eai: NODEJS_EAI
    code_stage: SNOWFLAKE_APPS.PUBLIC.MY_APP_CODE
    ignore:
      - node_modules
    install:
      commands:
        - ["npm", "ci"]
    build:
      commands:
        - ["npm", "run", "build"]
    run:
      command: ["npm", "start"]
    default_target: dev
    targets:
      dev:
        database: USER$
        schema: PUBLIC
        name: MY_APP_DEV
        account: myorganization-mydevaccount
        query_warehouse: DEV_WH
        label: "My App [dev]"
        description: "Development instance."
        icon: icon.svg
        execute_as_role: MY_APP_DEV_ROLE
        auto_resume: true
        auto_suspend_secs: 300
        min_instances: 1
        max_instances: 1
        external_access_integrations:
          - stripe_sandbox_eai
        secrets:
          - name: STRIPE_API_KEY
            secret: dev_stripe_secret
          - name: DATABASE_URL
            secret: dev_db_secret
        environment_variables:
          - name: LOG_LEVEL
            value: "DEBUG"
      prod:
        database: SNOWFLAKE_APPS
        schema: PUBLIC
        name: MY_APP
        query_warehouse: PROD_WH
    """
)


# A project with no code-storage configured whose default target lands in a
# personal database (USER$). With nothing configured, the CLI must default to a
# workspace there (personal databases do not support stages), matching the
# snowflake.yml flow.
_APP_YML_PERSONAL_DB = dedent(
    """\
    version: 2
    database: USER$
    schema: PUBLIC
    name: WS_APP
    query_warehouse: DEV_WH
    package_name: WS_APP
    artifact_repo: SNOWFLAKE_APPS.PUBLIC.WS_APP_REPO
    build_eai: NODEJS_EAI
    ignore:
      - node_modules
    default_target: dev
    targets:
      dev:
        name: WS_APP_DEV
    """
)


# A regular-database project with no code-storage configured. At deploy time
# the CLI provisions a temporary ``<name>_CODE`` stage it owns end to end,
# building from it and dropping it once the build has consumed it.
_APP_YML_REGULAR_DB_NO_CODE_STORAGE = dedent(
    """\
    version: 2
    name: REG_APP
    database: SNOWFLAKE_APPS
    schema: PUBLIC
    query_warehouse: WH
    package_name: REG_APP
    artifact_repo: SNOWFLAKE_APPS.PUBLIC.REG_REPO
    build_eai: NODEJS_EAI
    ignore:
      - node_modules
    """
)


def _definition(**overrides) -> AppYmlDefinition:
    """Build an :class:`AppYmlDefinition` with the location fields populated.

    ``name`` / ``database`` / ``schema`` / ``query_warehouse`` must be present on
    the resolved target (there is no connection fallback); the helper fills them
    at the top level so tests override only the fields they actually exercise.
    """
    base = dict(
        version=2,
        name="MY_APP",
        database="DB",
        schema="PUBLIC",
        query_warehouse="WH",
    )
    base.update(overrides)
    return AppYmlDefinition(**base)


class TestAppYmlDefinition:
    def test_parses_targets_and_default_target(self):
        model = AppYmlDefinition(**yaml.safe_load(_APP_YML))
        assert model.version == 2
        assert model.package_name == "MY_APP"
        assert model.code_stage == "SNOWFLAKE_APPS.PUBLIC.MY_APP_CODE"
        assert model.artifact_repo == "SNOWFLAKE_APPS.PUBLIC.MY_APP_REPO"
        assert model.build_eai == "NODEJS_EAI"
        # ``default_target`` is a top-level field; ``targets`` holds only real
        # targets.
        assert model.default_target == "dev"
        assert set(model.targets) == {"dev", "prod"}

    def test_legacy_targets_default_key_rejected(self):
        # The pre-release form nested ``default`` under ``targets``; it is now a
        # top-level ``default_target`` and the old form fails with guidance.
        with pytest.raises(ValueError, match="'targets.default' is no longer"):
            _definition(
                targets={"default": "dev", "dev": {"query_warehouse": "WH"}},
            )

    def test_target_fields(self):
        model = AppYmlDefinition(**yaml.safe_load(_APP_YML))
        dev = model.targets["dev"]
        assert dev.database == "USER$"
        assert dev.schema_ == "PUBLIC"
        assert dev.name == "MY_APP_DEV"
        assert dev.account == "myorganization-mydevaccount"
        assert dev.query_warehouse == "DEV_WH"
        assert dev.label == "My App [dev]"
        assert dev.description == "Development instance."
        assert dev.icon == "icon.svg"
        assert dev.execute_as_role == "MY_APP_DEV_ROLE"
        assert dev.auto_resume is True
        assert dev.auto_suspend_secs == 300
        assert dev.min_instances == 1
        assert dev.max_instances == 1
        assert dev.external_access_integrations == ["stripe_sandbox_eai"]
        assert [(s.name, s.secret) for s in dev.secrets] == [
            ("STRIPE_API_KEY", "dev_stripe_secret"),
            ("DATABASE_URL", "dev_db_secret"),
        ]
        assert [(e.name, e.value) for e in dev.environment_variables] == [
            ("LOG_LEVEL", "DEBUG"),
        ]

    def test_environment_variable_scalar_values_coerced_to_strings(self):
        # The service accepts unquoted scalars and stores them as strings;
        # mirror that so ``value: 8080`` / ``value: true`` parse (booleans use
        # their lowercase spelling).
        model = _definition(
            targets={
                "dev": {
                    "environment_variables": [
                        {"name": "PORT", "value": 8080},
                        {"name": "DEBUG", "value": True},
                        {"name": "RATIO", "value": 1.5},
                        {"name": "REGION", "value": "us-west-2"},
                    ]
                }
            },
        )
        assert [
            (e.name, e.value) for e in model.targets["dev"].environment_variables
        ] == [
            ("PORT", "8080"),
            ("DEBUG", "true"),
            ("RATIO", "1.5"),
            ("REGION", "us-west-2"),
        ]

    def test_ignore_parsed(self):
        model = AppYmlDefinition(**yaml.safe_load(_APP_YML))
        assert model.ignore == ["node_modules"]

    def test_bundle_artifacts_is_project_root_minus_ignore(self):
        # ``ignore`` only configures the exclusion list; the effective bundle
        # always uploads the whole project root (./* -> ./) minus those patterns.
        model = AppYmlDefinition(**yaml.safe_load(_APP_YML))
        assert len(model.bundle_artifacts) == 1
        mapping = model.bundle_artifacts[0]
        assert mapping.src == "./*"
        assert mapping.dest == "./"
        assert mapping.ignore == ["node_modules"]

    def test_bundle_artifacts_defaults_to_project_root_without_ignore(self):
        # With no ignore list the whole project root is still uploaded, with an
        # empty ignore list.
        model = _definition()
        assert model.ignore is None
        assert len(model.bundle_artifacts) == 1
        assert model.bundle_artifacts[0].src == "./*"
        assert model.bundle_artifacts[0].dest == "./"
        assert model.bundle_artifacts[0].ignore == []

    def test_build_sections_parsed_when_present(self):
        # install/build/run/dev are owned by the builder service but modeled by
        # the CLI so they can be omitted and defaulted; when present they parse.
        model = AppYmlDefinition(**yaml.safe_load(_APP_YML))
        assert model.install.commands == [["npm", "ci"]]
        assert model.build.commands == [["npm", "run", "build"]]
        assert model.run.command == ["npm", "start"]

    def test_build_sections_default_when_omitted(self):
        # Omitted builder sections fall back to Node conventions so a minimal
        # manifest still describes a buildable app.
        model = _definition()
        assert model.install.commands == [["npm", "ci"]]
        assert model.build.commands == [["npm", "run", "build"]]
        assert model.run.command == ["npm", "start"]
        assert model.dev.command == ["npm", "run", "dev"]

    def test_top_level_artifacts_mapping_ignored(self):
        # The top-level ``artifacts`` block is consumed by the builder service,
        # not the CLI; it must be ignored rather than confused with ``ignore``.
        model = _definition(
            artifacts=[{"src": ".next/standalone", "dest": "./"}],
            ignore=["node_modules"],
        )
        assert not hasattr(model, "artifacts")
        assert model.ignore == ["node_modules"]

    def test_unknown_target_field_ignored(self):
        model = _definition(
            targets={"dev": {"query_warehouse": "WH", "future_field": "x"}},
        )
        assert model.targets["dev"].query_warehouse == "WH"

    def test_minimal_definition(self):
        model = _definition()
        assert model.targets == {}
        assert model.default_target is None

    def test_target_defaults(self):
        target = AppYmlTarget()
        assert target.database is None
        assert target.name is None
        # Defaults are ``None`` (not empty collections) so unset is
        # distinguishable from a deliberately empty value.
        assert target.external_access_integrations is None
        assert target.secrets is None
        assert target.environment_variables is None

    def test_compute_resource_normalized_case_insensitively(self):
        model = _definition(targets={"dev": {"compute_resource": "serverless"}})
        assert model.targets["dev"].compute_resource == "SERVERLESS"

    def test_compute_resource_rejects_unknown_value(self):
        with pytest.raises(ValueError, match="compute_resource must be one of"):
            _definition(targets={"dev": {"compute_resource": "GPU"}})

    def test_compute_resource_defaults_to_none(self):
        assert AppYmlTarget().compute_resource is None

    def test_account_is_parsed_but_hidden_from_schema(self):
        # ``account`` stays functional (parsed, drives the mismatch warning) but
        # is intentionally hidden/undocumented via SkipJsonSchema, so it must not
        # appear in the generated JSON schema.
        assert AppYmlTarget(account="myorg-acct").account == "myorg-acct"
        assert "account" not in AppYmlTarget.model_json_schema()["properties"]
        assert "account" not in AppYmlDefinition.model_json_schema()["properties"]

    def test_code_workspace_parsed(self):
        model = _definition(code_workspace="DB.SCHEMA.MY_WS")
        assert model.code_workspace == "DB.SCHEMA.MY_WS"
        assert model.code_stage is None

    def test_code_stage_and_workspace_are_mutually_exclusive(self):
        # The two name the same thing (where uploaded source lives), so setting
        # both is rejected — matching the snowflake.yml entity.
        with pytest.raises(ValueError, match="mutually exclusive"):
            _definition(
                code_workspace="DB.SCHEMA.MY_WS",
                code_stage="DB.SCHEMA.MY_CODE",
            )

    def test_code_storage_absent_leaves_fields_none(self):
        model = _definition()
        assert model.code_stage is None
        assert model.code_workspace is None
        assert model.ignore is None

    def test_build_job_location_defaults_to_none(self):
        # Unset means "use the builder default" (the caller's personal
        # database); it is distinguishable from a deliberately set value.
        assert _definition().build_job_location is None
        assert AppYmlTarget().build_job_location is None

    def test_build_job_location_parsed_at_baseline_and_target(self):
        model = _definition(
            build_job_location="BASE_DB.BASE_SC",
            targets={"prod": {"build_job_location": "PROD_DB.PROD_SC"}},
        )
        assert model.build_job_location == "BASE_DB.BASE_SC"
        assert model.targets["prod"].build_job_location == "PROD_DB.PROD_SC"

    def test_single_target_requires_explicit_selection(self):
        # Once any target is declared a target must be selected explicitly;
        # even a lone target is not an implicit default.
        model = _definition(targets={"dev": {"query_warehouse": "DEV_WH"}})
        assert model.default_target is None
        with pytest.raises(CliError, match="No target selected"):
            resolve_target(model, None)

    def test_empty_target_deploys_baseline(self):
        # A target declared as an empty mapping ({}) carries no overrides and
        # deploys the baseline unchanged under that target's name.
        model = _definition(
            query_warehouse="BASE_WH",
            default_target="dev",
            targets={"dev": {}},
        )
        name, target = resolve_target(model, None)
        assert name == "dev"
        assert target.query_warehouse == "BASE_WH"

    def test_bodyless_target_treated_as_empty(self):
        # A YAML key with no value (``dev:``) parses to None; it is normalised to
        # an empty target so it deploys the baseline unchanged.
        model = AppYmlDefinition(
            **yaml.safe_load(
                dedent(
                    """\
                    version: 2
                    name: MY_APP
                    database: DB
                    schema: PUBLIC
                    query_warehouse: BASE_WH
                    default_target: dev
                    targets:
                      dev:
                    """
                )
            )
        )
        assert model.targets["dev"] == AppYmlTarget()
        name, target = resolve_target(model, None)
        assert name == "dev"
        assert target.query_warehouse == "BASE_WH"

    def test_top_level_service_fields_are_baseline(self):
        # Service/deployment fields may be declared at the top level as a
        # baseline shared by every target (``name`` is the service name, and
        # ``package_name`` optionally names the artifact-repo package).
        model = AppYmlDefinition(
            version=2,
            package_name="MY_PKG",
            name="MY_SERVICE",
            database="DB",
            schema="PUBLIC",
            query_warehouse="WH",
        )
        assert model.package_name == "MY_PKG"
        assert model.name == "MY_SERVICE"
        assert model.database == "DB"
        assert model.schema_ == "PUBLIC"
        assert model.query_warehouse == "WH"

    def test_package_name_optional(self):
        # ``package_name`` is optional; when unset it defaults to ``name`` at
        # resolve time (see TestDeployFromAppYml).
        model = _definition()
        assert model.package_name is None

    @pytest.mark.parametrize(
        "field",
        ["name", "database", "schema", "query_warehouse"],
    )
    def test_baseline_location_fields_optional_at_parse(self, field):
        # The location fields are no longer required at parse time — they may be
        # supplied per target instead. Requiredness is enforced on the resolved
        # target (see TestResolveRequiredFields), not on the model.
        base = dict(
            version=2,
            name="MY_APP",
            database="DB",
            schema="PUBLIC",
            query_warehouse="WH",
        )
        base.pop(field)
        model = AppYmlDefinition(**base)  # does not raise
        attr = "schema_" if field == "schema" else field
        assert getattr(model, attr) is None


def _resolve_required(model, target=None):
    """Resolve a target through the command path with a stubbed manager.

    ``_resolve_app_yml_target`` is where required-field validation lives; the
    manager is only touched to expand the ``USER$`` shorthand.
    """
    from unittest.mock import MagicMock

    from snowflake.cli._plugins.apps.commands import _resolve_app_yml_target

    manager = MagicMock()
    manager.get_personal_database.return_value = "USER$TESTER"
    return _resolve_app_yml_target(model, target, manager=manager)


class TestResolveRequiredFields:
    """``name`` / ``database`` / ``schema`` / ``query_warehouse`` are required on
    the *resolved* target — set at the top level, per target, or a mix."""

    def test_targets_may_define_required_fields(self):
        # Baseline omits ``name``; each target supplies its own. Resolves fine.
        model = AppYmlDefinition(
            **yaml.safe_load(
                dedent(
                    """\
                    version: 2
                    database: GBLOOM
                    schema: APPS
                    query_warehouse: WH
                    default_target: dev
                    targets:
                      dev:
                        name: MY_APP_DEV
                      prod:
                        name: MY_APP_PROD
                    """
                )
            )
        )
        assert model.name is None  # not required at the top level
        dep = _resolve_required(model, None)
        assert dep.service_name == "MY_APP_DEV"
        assert _resolve_required(model, "prod").service_name == "MY_APP_PROD"

    def test_baseline_may_define_required_fields(self):
        # No targets: the baseline is the resolved target and locates fully.
        dep = _resolve_required(_definition(), None)
        assert dep.service_name == "MY_APP"

    def test_target_supplies_everything_with_empty_baseline(self):
        model = AppYmlDefinition(
            **yaml.safe_load(
                dedent(
                    """\
                    version: 2
                    default_target: dev
                    targets:
                      dev:
                        name: D
                        database: GBLOOM
                        schema: APPS
                        query_warehouse: WH
                    """
                )
            )
        )
        dep = _resolve_required(model, None)
        assert (dep.service_name, dep.database, dep.schema) == ("D", "GBLOOM", "APPS")

    @pytest.mark.parametrize(
        "field",
        ["name", "database", "schema", "query_warehouse"],
    )
    def test_missing_field_on_baseline_without_targets_raises(self, field):
        base = dict(
            version=2,
            name="MY_APP",
            database="DB",
            schema="PUBLIC",
            query_warehouse="WH",
        )
        base.pop(field)
        model = AppYmlDefinition(**base)
        with pytest.raises(CliError) as exc:
            _resolve_required(model, None)
        assert field in str(exc.value)
        assert "the app.yml baseline" in str(exc.value)

    def test_missing_field_on_resolved_target_raises(self):
        # query_warehouse set nowhere -> the resolved target is incomplete.
        model = AppYmlDefinition(
            **yaml.safe_load(
                dedent(
                    """\
                    version: 2
                    database: GBLOOM
                    schema: APPS
                    default_target: dev
                    targets:
                      dev:
                        name: MY_APP_DEV
                    """
                )
            )
        )
        with pytest.raises(CliError) as exc:
            _resolve_required(model, None)
        assert "query_warehouse" in str(exc.value)
        assert "target 'dev'" in str(exc.value)


class TestLoadAppYml:
    def test_missing_file_returns_none(self, tmp_path):
        assert load_app_yml(tmp_path) is None

    def test_version_1_returns_none(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text("version: 1\nname: MY_APP\n")
        assert load_app_yml(tmp_path) is None

    def test_versionless_returns_none(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text("name: MY_APP\n")
        assert load_app_yml(tmp_path) is None

    def test_version_2_is_loaded(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        model = load_app_yml(tmp_path)
        assert model is not None
        assert model.package_name == "MY_APP"
        assert model.default_target == "dev"

    def test_higher_integer_version_raises(self, tmp_path):
        # ``version: 3`` is a newer schema this CLI does not understand; it must
        # fail loudly rather than be parsed against the v2 model.
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace("version: 2", "version: 3")
        )
        with pytest.raises(CliError, match="Unsupported app.yml version"):
            load_app_yml(tmp_path)

    def test_fractional_version_above_2_raises(self, tmp_path):
        # ``version: 2.1`` is above the supported version and must fail rather
        # than silently fall back to snowflake.yml.
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace("version: 2", "version: 2.1")
        )
        with pytest.raises(CliError, match="Unsupported app.yml version"):
            load_app_yml(tmp_path)

    def test_string_higher_version_raises(self, tmp_path):
        # A string form of a higher version is rejected the same way.
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace("version: 2", 'version: "3"')
        )
        with pytest.raises(CliError, match="Unsupported app.yml version"):
            load_app_yml(tmp_path)

    def test_string_fractional_version_above_2_raises(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace("version: 2", 'version: "2.1"')
        )
        with pytest.raises(CliError, match="Unsupported app.yml version"):
            load_app_yml(tmp_path)

    def test_float_version_is_loaded(self, tmp_path):
        # ``version: 2.0`` is a YAML float, not an int, but names version 2.
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace("version: 2", "version: 2.0")
        )
        model = load_app_yml(tmp_path)
        assert model is not None
        assert model.version == 2

    def test_string_version_is_loaded(self, tmp_path):
        # ``version: "2"`` is a YAML string, but names version 2.
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace("version: 2", 'version: "2"')
        )
        model = load_app_yml(tmp_path)
        assert model is not None
        assert model.version == 2

    def test_string_float_version_is_loaded(self, tmp_path):
        # ``version: "2.0"`` is a string naming version 2.
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace("version: 2", 'version: "2.0"')
        )
        model = load_app_yml(tmp_path)
        assert model is not None
        assert model.version == 2

    def test_float_version_1_returns_none(self, tmp_path):
        # A whole-number float below the supported version is still legacy.
        (tmp_path / APP_YML_FILENAME).write_text("version: 1.0\nname: MY_APP\n")
        assert load_app_yml(tmp_path) is None

    def test_fractional_version_below_2_returns_none(self, tmp_path):
        # A fractional version below 2 is a legacy build manifest, not an error:
        # it falls back to snowflake.yml like any other sub-2 version.
        (tmp_path / APP_YML_FILENAME).write_text("version: 1.5\nname: MY_APP\n")
        assert load_app_yml(tmp_path) is None

    def test_non_numeric_version_returns_none(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text("version: latest\nname: MY_APP\n")
        assert load_app_yml(tmp_path) is None

    def test_malformed_yaml_raises(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text("version: 2\n  bad: : :\n")
        with pytest.raises(CliError, match="Could not parse app.yml"):
            load_app_yml(tmp_path)

    def test_non_mapping_raises(self, tmp_path):
        (tmp_path / APP_YML_FILENAME).write_text("- 1\n- 2\n")
        with pytest.raises(CliError, match="must be a mapping"):
            load_app_yml(tmp_path)

    def test_invalid_structure_raises(self, tmp_path):
        # ``targets`` must be a mapping of target configs.
        (tmp_path / APP_YML_FILENAME).write_text(
            "version: 2\npackage_name: MY_APP\ntargets: not-a-mapping\n"
        )
        with pytest.raises(CliError, match="Invalid app.yml"):
            load_app_yml(tmp_path)

    def test_ignore_loaded_as_list(self, tmp_path):
        # ``ignore`` only configures the exclusion list; src/dest are fixed, so
        # the effective bundle always uploads the project root minus these
        # patterns.
        (tmp_path / APP_YML_FILENAME).write_text(
            _APP_YML.replace(
                "ignore:\n  - node_modules\n",
                "ignore:\n  - node_modules\n  - .git\n",
            )
        )
        model = load_app_yml(tmp_path)
        assert model is not None
        assert model.ignore == ["node_modules", ".git"]
        assert model.bundle_artifacts[0].src == "./*"
        assert model.bundle_artifacts[0].ignore == ["node_modules", ".git"]


class TestResolveTarget:
    def _model(self):
        return AppYmlDefinition(**yaml.safe_load(_APP_YML))

    def test_explicit_target(self):
        name, target = resolve_target(self._model(), "prod")
        assert name == "prod"
        assert target.query_warehouse == "PROD_WH"

    def test_defaults_to_targets_default(self):
        name, target = resolve_target(self._model(), None)
        assert name == "dev"
        assert target.query_warehouse == "DEV_WH"

    def test_explicit_target_overrides_default(self):
        name, _ = resolve_target(self._model(), "prod")
        assert name == "prod"

    def test_unknown_target_raises(self):
        with pytest.raises(CliError, match="Target 'staging' is not defined"):
            resolve_target(self._model(), "staging")

    def test_multiple_targets_no_default_raises(self):
        # More than one target and none selected (no --target, no
        # default_target): the single-target shortcut does not apply, so a
        # target must be picked explicitly.
        model = _definition(
            targets={
                "dev": {"query_warehouse": "DEV_WH"},
                "prod": {"query_warehouse": "PROD_WH"},
            },
        )
        with pytest.raises(CliError, match="No target selected"):
            resolve_target(model, None)

    def test_no_targets_uses_baseline(self):
        # Targets are optional: with none declared, the top-level baseline is
        # deployed directly and the resolved name is None.
        model = _definition(name="MY_APP", database="DB", query_warehouse="WH")
        name, target = resolve_target(model, None)
        assert name is None
        assert target.name == "MY_APP"
        assert target.database == "DB"
        assert target.query_warehouse == "WH"

    def test_no_targets_but_target_requested_raises(self):
        model = _definition()
        with pytest.raises(CliError, match="Target 'dev' is not defined"):
            resolve_target(model, "dev")

    def test_target_overrides_baseline(self):
        # A field the target sets overrides the baseline; a field it leaves unset
        # shows the baseline value through.
        model = _definition(
            query_warehouse="BASE_WH",
            label="Base label",
            min_instances=1,
            targets={"prod": {"query_warehouse": "PROD_WH", "min_instances": 3}},
        )
        name, prod = resolve_target(model, "prod")
        assert name == "prod"
        assert prod.query_warehouse == "PROD_WH"  # overridden
        assert prod.min_instances == 3  # overridden
        assert prod.label == "Base label"  # inherited from baseline

    def test_target_list_field_replaces_baseline(self):
        # List fields are replaced wholesale, not concatenated.
        model = _definition(
            external_access_integrations=["base_eai"],
            targets={"prod": {"external_access_integrations": ["prod_eai"]}},
        )
        _, prod = resolve_target(model, "prod")
        assert prod.external_access_integrations == ["prod_eai"]

    def test_target_inherits_baseline_list_when_unset(self):
        model = _definition(
            external_access_integrations=["base_eai"],
            targets={"prod": {"query_warehouse": "WH"}},
        )
        _, prod = resolve_target(model, "prod")
        assert prod.external_access_integrations == ["base_eai"]

    def test_target_overrides_code_stage_inherits_ignore(self):
        # ``ignore`` is an independent field that inherits from the baseline when
        # a target overrides only the code stage.
        model = _definition(
            code_stage="DB.PUBLIC.BASE_CODE",
            ignore=["node_modules"],
            targets={"prod": {"code_stage": "DB.PUBLIC.PROD_CODE"}},
        )
        _, prod = resolve_target(model, "prod")
        assert prod.code_stage == "DB.PUBLIC.PROD_CODE"  # overridden
        assert prod.ignore == ["node_modules"]  # inherited from baseline

    def test_target_switches_stage_to_workspace(self):
        # Naming a workspace on the target replaces the baseline stage backend
        # entirely (they are mutually exclusive) without a validation error.
        model = _definition(
            code_stage="DB.PUBLIC.BASE_CODE",
            targets={"prod": {"code_workspace": "DB.PUBLIC.PROD_WS"}},
        )
        _, prod = resolve_target(model, "prod")
        assert prod.code_workspace == "DB.PUBLIC.PROD_WS"
        assert prod.code_stage is None

    def test_target_inherits_baseline_code_storage_when_unset(self):
        model = _definition(
            code_stage="DB.PUBLIC.BASE_CODE",
            targets={"prod": {"query_warehouse": "WH"}},
        )
        _, prod = resolve_target(model, "prod")
        assert prod.code_stage == "DB.PUBLIC.BASE_CODE"

    def test_target_overrides_build_job_location(self):
        # ``build_job_location`` is a package-build field: a target may override
        # the baseline value, and a target that leaves it unset inherits it.
        model = _definition(
            build_job_location="BASE_DB.BASE_SC",
            targets={
                "prod": {"build_job_location": "PROD_DB.PROD_SC"},
                "dev": {"query_warehouse": "WH"},
            },
        )
        _, prod = resolve_target(model, "prod")
        assert prod.build_job_location == "PROD_DB.PROD_SC"  # overridden
        _, dev = resolve_target(model, "dev")
        assert dev.build_job_location == "BASE_DB.BASE_SC"  # inherited


_COMMANDS = "snowflake.cli._plugins.apps.commands"


class TestTargetAccountWarning:
    """The per-target ``account`` is advisory in Milestone 1: warn (never fail)
    when it differs from the active connection's account."""

    @patch(f"{_COMMANDS}.cli_console")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_mismatch_warns(self, mock_ctx, mock_console):
        from snowflake.cli._plugins.apps.commands import (
            _warn_on_target_account_mismatch,
        )

        mock_ctx.return_value.connection_context = Mock(
            account="myorg-accountA", connection_name="c"
        )
        _warn_on_target_account_mismatch("dev", "myorg-accountB")
        mock_console.warning.assert_called_once()
        assert "per-target account binding is not yet supported" in str(
            mock_console.warning.call_args.args[0]
        )

    @patch(f"{_COMMANDS}.cli_console")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_case_insensitive_match_no_warn(self, mock_ctx, mock_console):
        from snowflake.cli._plugins.apps.commands import (
            _warn_on_target_account_mismatch,
        )

        mock_ctx.return_value.connection_context = Mock(
            account="MYORG-ACCOUNT", connection_name="c"
        )
        _warn_on_target_account_mismatch("dev", "myorg-account")
        mock_console.warning.assert_not_called()

    @patch(f"{_COMMANDS}.cli_console")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_no_target_account_no_warn(self, mock_ctx, mock_console):
        from snowflake.cli._plugins.apps.commands import (
            _warn_on_target_account_mismatch,
        )

        mock_ctx.return_value.connection_context = Mock(
            account="myorg-account", connection_name="c"
        )
        _warn_on_target_account_mismatch("dev", None)
        mock_console.warning.assert_not_called()


class TestBuildServiceSpecification:
    def test_full_target(self):
        target = AppYmlTarget(
            query_warehouse="DEV_WH",
            label="My App [dev]",
            description="Dev",
            icon="icon.svg",
            execute_as_role="MY_APP_DEV_ROLE",
            auto_resume=True,
            auto_suspend_secs=300,
            min_instances=0,
            max_instances=2,
            external_access_integrations=["eai_a", "eai_b"],
            secrets=[
                {"name": "API_KEY", "secret": "api_secret"},
                {"name": "DB_URL", "secret": "db_secret"},
            ],
            environment_variables=[{"name": "LOG_LEVEL", "value": "DEBUG"}],
        )
        spec = yaml.safe_load(SnowflakeAppManager.build_service_specification(target))
        assert spec == {
            "query_warehouse": "DEV_WH",
            "label": "My App [dev]",
            "description": "Dev",
            "icon": "icon.svg",
            "execute_as_role": "MY_APP_DEV_ROLE",
            "auto_resume": True,
            "auto_suspend_secs": 300,
            "min_instances": 0,
            "max_instances": 2,
            "external_access_integrations": ["eai_a", "eai_b"],
            "secrets": [
                {"name": "API_KEY", "secret": "api_secret"},
                {"name": "DB_URL", "secret": "db_secret"},
            ],
            "environment_variables": [{"name": "LOG_LEVEL", "value": "DEBUG"}],
        }

    def test_minimal_target_omits_unset_fields(self):
        target = AppYmlTarget(query_warehouse="WH")
        spec = yaml.safe_load(SnowflakeAppManager.build_service_specification(target))
        assert spec == {"query_warehouse": "WH"}

    def test_url_prefix_emitted_only_when_included(self):
        # ``url_prefix`` is a CNG-only field: emitted only when the caller opts in
        # (the deploy path gates it on the CNG compute resource behind the flag).
        target = AppYmlTarget(query_warehouse="WH", url_prefix="MY_APP")
        included = yaml.safe_load(
            SnowflakeAppManager.build_service_specification(
                target, include_url_prefix=True
            )
        )
        assert included["url_prefix"] == "MY_APP"

        # Dropped by default (non-CNG path), even when set on the target.
        default = yaml.safe_load(
            SnowflakeAppManager.build_service_specification(target)
        )
        assert "url_prefix" not in default

    def test_health_check_emitted_only_when_included(self):
        # ``health_check`` is a CNG-only field: emitted only when the caller opts
        # in (the deploy path gates it on the CNG compute resource behind the
        # flag).
        target = AppYmlTarget(query_warehouse="WH", health_check="/healthz")
        included = yaml.safe_load(
            SnowflakeAppManager.build_service_specification(
                target, include_health_check=True
            )
        )
        assert included["health_check"] == "/healthz"

        # Dropped by default (non-CNG path), even when set on the target.
        default = yaml.safe_load(
            SnowflakeAppManager.build_service_specification(target)
        )
        assert "health_check" not in default

    def test_empty_target_produces_empty_spec(self):
        spec = yaml.safe_load(
            SnowflakeAppManager.build_service_specification(AppYmlTarget())
        )
        assert spec is None or spec == {}

    def test_min_instances_zero_is_emitted(self):
        target = AppYmlTarget(min_instances=0)
        spec = yaml.safe_load(SnowflakeAppManager.build_service_specification(target))
        assert spec == {"min_instances": 0}

    def test_bare_secret_qualified_with_deployment_scope(self):
        # A bare secret name inherits the deployment database/schema so it
        # resolves the same way the CLI's other identifiers do; a fully
        # qualified secret is left untouched.
        target = AppYmlTarget(
            secrets=[
                {"name": "API_KEY", "secret": "api_secret"},
                {"name": "DB_URL", "secret": "OTHER_DB.OTHER_SC.db_secret"},
            ],
        )
        spec = yaml.safe_load(
            SnowflakeAppManager.build_service_specification(
                target, database="APP_DB", schema="APP_SC"
            )
        )
        assert spec["secrets"] == [
            {"name": "API_KEY", "secret": "APP_DB.APP_SC.api_secret"},
            {"name": "DB_URL", "secret": "OTHER_DB.OTHER_SC.db_secret"},
        ]

    def test_bare_secret_left_unqualified_without_scope(self):
        # With no deployment scope supplied the value passes through unchanged.
        target = AppYmlTarget(secrets=[{"name": "API_KEY", "secret": "api_secret"}])
        spec = yaml.safe_load(SnowflakeAppManager.build_service_specification(target))
        assert spec["secrets"] == [{"name": "API_KEY", "secret": "api_secret"}]


class TestCreateOrAlterAppService:
    def test_emits_create_or_alter_ddl(self):
        manager = SnowflakeAppManager()
        service_fqn = FQN(database="DB", schema="SC", name="MY_APP_DEV")
        with patch.object(manager, "execute_query") as mock_exec:
            manager.create_or_alter_app_service(
                service_fqn=service_fqn,
                artifact_repo_fqn="DB.SC.MY_APP_REPO",
                package_name="MY_APP",
                specification="query_warehouse: DEV_WH\n",
                version="LATEST",
            )
        query = mock_exec.call_args[0][0]
        assert "CREATE OR ALTER APPLICATION SERVICE DB.SC.MY_APP_DEV" in query
        assert "FROM ARTIFACT REPOSITORY DB.SC.MY_APP_REPO PACKAGE MY_APP" in query
        assert "VERSION LATEST" in query
        assert "SPECIFICATION = $$\nquery_warehouse: DEV_WH\n$$" in query

    def test_rejects_dollar_dollar_in_specification(self):
        manager = SnowflakeAppManager()
        with patch.object(manager, "execute_query"):
            with pytest.raises(CliError, match="must not contain"):
                manager.create_or_alter_app_service(
                    service_fqn=FQN(database="DB", schema="SC", name="A"),
                    artifact_repo_fqn="DB.SC.REPO",
                    package_name="A",
                    specification="label: $$evil$$\n",
                )

    @patch.object(SnowflakeAppManager, "execute_query")
    def test_defaults_version_to_latest(self, mock_exec):
        manager = SnowflakeAppManager()
        manager.create_or_alter_app_service(
            service_fqn=FQN(database="DB", schema="SC", name="A"),
            artifact_repo_fqn="DB.SC.REPO",
            package_name="A",
            specification="query_warehouse: WH\n",
        )
        assert "VERSION LATEST" in mock_exec.call_args[0][0]

    @pytest.mark.parametrize("compute_resource", ["SERVERLESS", "MANAGED_COMPUTE_POOL"])
    @patch.object(SnowflakeAppManager, "execute_query")
    def test_emits_compute_resource_clause(self, mock_exec, compute_resource):
        # COMPUTE_RESOURCE is a write-once DDL clause (not owned by the
        # SPECIFICATION), so it is emitted alongside it, before SPECIFICATION.
        manager = SnowflakeAppManager()
        manager.create_or_alter_app_service(
            service_fqn=FQN(database="DB", schema="SC", name="A"),
            artifact_repo_fqn="DB.SC.REPO",
            package_name="A",
            specification="query_warehouse: WH\n",
            compute_resource=compute_resource,
        )
        query = mock_exec.call_args[0][0]
        assert f"COMPUTE_RESOURCE = {compute_resource}" in query
        assert query.index("COMPUTE_RESOURCE") < query.index("SPECIFICATION")

    @patch.object(SnowflakeAppManager, "execute_query")
    def test_omits_compute_resource_clause_when_none(self, mock_exec):
        manager = SnowflakeAppManager()
        manager.create_or_alter_app_service(
            service_fqn=FQN(database="DB", schema="SC", name="A"),
            artifact_repo_fqn="DB.SC.REPO",
            package_name="A",
            specification="query_warehouse: WH\n",
            compute_resource=None,
        )
        assert "COMPUTE_RESOURCE" not in mock_exec.call_args[0][0]


def _make_ctx(project_root):
    from snowflake.cli.api.metrics import CLIMetrics

    ctx = Mock()
    ctx.project_root = project_root
    ctx.metrics = CLIMetrics()
    ctx.connection_context = Mock(
        database=None, schema=None, connection_name="default", account=None
    )
    return ctx


def _make_manager_mock(mock_mgr_cls):
    mgr = mock_mgr_cls.return_value
    mgr.get_personal_database.return_value = "USER$TESTUSER"
    mgr.stage_exists.return_value = False
    mgr.artifact_repo_exists.return_value = False
    mgr.upload_to_stage.return_value = []
    mgr.upload_to_workspace.return_value = []
    mgr.workspace_subdirectory_uri.return_value = (
        "snow://workspace/USER$TESTUSER.PUBLIC.SNOWFLAKE_APPS/versions/live/WS_APP"
    )
    mgr.build_app_artifact_repo.return_value = "Build job submitted: DB.SC.BUILD_JOB_1"
    mgr.current_role.return_value = "TEST_ROLE"
    # build_service_specification / resolve_application_service_url are pure —
    # delegate to the real implementations so the deploy path builds a real spec.
    mgr.build_service_specification.side_effect = (
        SnowflakeAppManager.build_service_specification
    )
    mgr.resolve_application_service_url_from_describe.side_effect = (
        SnowflakeAppManager().resolve_application_service_url_from_describe
    )
    return mgr


class TestDeployFromAppYml:
    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_deploy_default_target(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "my-app.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        result = snowflake_app_deploy(
            None, False, False, False, interactive=False, target=None
        )

        assert "my-app.snowflakecomputing.app" in result.message
        # Default target is "dev": personal database (USER$) resolved, dev name.
        mgr.get_personal_database.assert_called_once()
        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["service_fqn"] == FQN(
            database="USER$TESTUSER", schema="PUBLIC", name="MY_APP_DEV"
        )
        assert call["artifact_repo_fqn"] == "SNOWFLAKE_APPS.PUBLIC.MY_APP_REPO"
        assert call["package_name"] == "MY_APP"
        assert call["version"] == "LATEST"
        spec = yaml.safe_load(call["specification"])
        assert spec["query_warehouse"] == "DEV_WH"
        # Bare secret names inherit the deployment scope (USER$TESTUSER.PUBLIC).
        assert spec["secrets"] == [
            {
                "name": "STRIPE_API_KEY",
                "secret": "USER$TESTUSER.PUBLIC.dev_stripe_secret",
            },
            {"name": "DATABASE_URL", "secret": "USER$TESTUSER.PUBLIC.dev_db_secret"},
        ]
        assert spec["external_access_integrations"] == ["stripe_sandbox_eai"]

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_deploy_explicit_target(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        result = snowflake_app_deploy(
            None, False, False, False, interactive=False, target="prod"
        )

        assert "prod.snowflakecomputing.app" in result.message
        # prod has an explicit database, so the personal DB is never resolved.
        mgr.get_personal_database.assert_not_called()
        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["service_fqn"] == FQN(
            database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP"
        )
        spec = yaml.safe_load(call["specification"])
        assert spec["query_warehouse"] == "PROD_WH"

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_deploy_baseline_without_targets(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """With no ``targets`` block, the top-level baseline is deployed
        directly."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(
            dedent(
                """\
                version: 2
                package_name: MY_APP
                name: MY_APP
                database: SNOWFLAKE_APPS
                schema: PUBLIC
                query_warehouse: WH
                code_stage: SNOWFLAKE_APPS.PUBLIC.PKG_CODE
                artifact_repo: SNOWFLAKE_APPS.PUBLIC.PKG_REPO
                """
            )
        )
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "svc.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(None, False, False, False, interactive=False, target=None)

        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["package_name"] == "MY_APP"
        assert call["service_fqn"] == FQN(
            database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP"
        )
        spec = yaml.safe_load(call["specification"])
        assert spec["query_warehouse"] == "WH"

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_deploy_target_overrides_baseline(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """A target's set fields override the top-level baseline; unset fields
        show the baseline through."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(
            dedent(
                """\
                version: 2
                package_name: MY_APP
                name: MY_APP
                database: SNOWFLAKE_APPS
                schema: PUBLIC
                query_warehouse: BASE_WH
                label: "Base label"
                code_stage: SNOWFLAKE_APPS.PUBLIC.PKG_CODE
                artifact_repo: SNOWFLAKE_APPS.PUBLIC.PKG_REPO
                targets:
                  prod:
                    name: MY_APP_PROD
                    query_warehouse: PROD_WH
                """
            )
        )
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(
            None, False, False, False, interactive=False, target="prod"
        )

        call = mgr.create_or_alter_app_service.call_args.kwargs
        # Service name + destination come from baseline (schema/database) and the
        # target's overriding name.
        assert call["service_fqn"] == FQN(
            database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP_PROD"
        )
        spec = yaml.safe_load(call["specification"])
        assert spec["query_warehouse"] == "PROD_WH"  # overridden
        assert spec["label"] == "Base label"  # inherited from baseline

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_target_overrides_package_build_fields(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """A target may override the package-build fields (package_name,
        artifact_repo, build_eai) declared at the top level."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(
            dedent(
                """\
                version: 2
                package_name: BASE_PKG
                name: BASE_SVC
                artifact_repo: SNOWFLAKE_APPS.PUBLIC.BASE_REPO
                build_eai: BASE_EAI
                database: SNOWFLAKE_APPS
                schema: PUBLIC
                query_warehouse: BASE_WH
                code_stage: SNOWFLAKE_APPS.PUBLIC.PKG_CODE
                targets:
                  prod:
                    name: PROD_SVC
                    package_name: PROD_PKG
                    artifact_repo: SNOWFLAKE_APPS.PUBLIC.PROD_REPO
                    build_eai: PROD_EAI
                    query_warehouse: WH
                """
            )
        )
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(
            None, False, False, False, interactive=False, target="prod"
        )

        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["package_name"] == "PROD_PKG"  # overridden
        assert call["artifact_repo_fqn"] == "SNOWFLAKE_APPS.PUBLIC.PROD_REPO"
        # The overriding build_eai is forwarded to the build job.
        assert (
            mgr.build_app_artifact_repo.call_args.kwargs.get("build_eai") == "PROD_EAI"
        )

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_build_job_location_forwarded_to_build(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """``build_job_location`` from the resolved target is forwarded to the
        builder so the build job runs in the requested schema."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(
            dedent(
                """\
                version: 2
                name: MY_APP
                database: SNOWFLAKE_APPS
                schema: PUBLIC
                query_warehouse: WH
                package_name: MY_APP
                build_job_location: BASE_DB.BASE_SC
                code_stage: SNOWFLAKE_APPS.PUBLIC.PKG_CODE
                artifact_repo: SNOWFLAKE_APPS.PUBLIC.PKG_REPO
                default_target: prod
                targets:
                  prod:
                    build_job_location: PROD_DB.PROD_SC
                """
            )
        )
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(
            None, False, False, False, interactive=False, target="prod"
        )

        # The target override wins over the baseline value.
        assert (
            mgr.build_app_artifact_repo.call_args.kwargs.get("build_job_location")
            == "PROD_DB.PROD_SC"
        )

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_build_job_location_defaults_to_none_when_unset(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """With no ``build_job_location`` configured the builder is called with
        ``None`` so it keeps its default (personal database) behaviour."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(
            None, False, False, False, interactive=False, target="prod"
        )

        assert (
            mgr.build_app_artifact_repo.call_args.kwargs.get("build_job_location")
            is None
        )

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_package_name_defaults_to_service_name(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """``package_name`` is optional; when omitted it defaults to the
        (required) service ``name``, and the artifact-repo / code-stage names are
        derived from it too."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(
            dedent(
                """\
                version: 2
                name: MY_APP
                database: SNOWFLAKE_APPS
                schema: PUBLIC
                query_warehouse: WH
                default_target: prod
                code_stage: SNOWFLAKE_APPS.PUBLIC.PKG_CODE
                artifact_repo: SNOWFLAKE_APPS.PUBLIC.PKG_REPO
                targets:
                  prod: {}
                """
            )
        )
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "svc.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        # No package_name anywhere; resolved via default_target.
        snowflake_app_deploy(None, False, False, False, interactive=False, target=None)

        call = mgr.create_or_alter_app_service.call_args.kwargs
        # package_name defaults to the service name.
        assert call["package_name"] == "MY_APP"
        assert call["service_fqn"] == FQN(
            database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP"
        )

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_artifact_repo_and_stage_default_from_name(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """With ``artifact_repo`` / ``code_stage`` omitted, both default off the
        (bare) service ``name`` in the resolved database/schema."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(
            dedent(
                """\
                version: 2
                name: MY_SVC
                database: SNOWFLAKE_APPS
                schema: PUBLIC
                query_warehouse: WH
                default_target: prod
                targets:
                  prod: {}
                """
            )
        )
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        # No code storage configured on a regular database: the temporary
        # <name>_CODE stage default is what gets exercised here.
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "svc.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(None, False, False, False, interactive=False, target=None)

        call = mgr.create_or_alter_app_service.call_args.kwargs
        # artifact_repo defaults to <name>_REPO in the resolved db/schema.
        assert call["artifact_repo_fqn"] == "SNOWFLAKE_APPS.PUBLIC.MY_SVC_REPO"
        # code stage defaults to <name>_CODE in the resolved db/schema.
        stage_fqn = FQN(database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_SVC_CODE")
        assert (
            mgr.build_app_artifact_repo.call_args.kwargs.get("stage_fqn") == stage_fqn
        )

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_fqn_service_name_overrides_database_schema(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """A target ``name`` given as a fully-qualified identifier locates the
        service in its own db/schema, overriding the separate fields."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(
            dedent(
                """\
                version: 2
                name: MY_APP
                database: BASE_DB
                schema: BASE_SC
                query_warehouse: WH
                package_name: MY_APP
                default_target: prod
                code_stage: SNOWFLAKE_APPS.PUBLIC.PKG_CODE
                artifact_repo: SNOWFLAKE_APPS.PUBLIC.PKG_REPO
                targets:
                  prod:
                    database: IGNORED_DB
                    schema: IGNORED_SC
                    name: SVC_DB.SVC_SC.MY_SERVICE
                    query_warehouse: WH
                """
            )
        )
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "svc.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(None, False, False, False, interactive=False, target=None)

        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["service_fqn"] == FQN(
            database="SVC_DB", schema="SVC_SC", name="MY_SERVICE"
        )

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_deploy_personal_db_defaults_to_temporary_workspace(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """A personal-database (USER$) target with no code storage configured
        uploads through a temporary ``<app>_CODE`` workspace (stages are
        unsupported there), builds from the workspace source URI, and drops the
        workspace once the build has consumed it."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML_PERSONAL_DB)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "ws-app.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        result = snowflake_app_deploy(
            None, False, False, False, interactive=False, target=None
        )

        assert "ws-app.snowflakecomputing.app" in result.message
        # Default backend for a personal DB is a temporary ``<app>_CODE``
        # workspace in the resolved USER$ database — never a stage.
        workspace_fqn = FQN(
            database="USER$TESTUSER", schema="PUBLIC", name="WS_APP_DEV_CODE"
        )
        mgr.create_workspace.assert_called_once_with(workspace_fqn)
        mgr.upload_to_workspace.assert_called_once()
        mgr.upload_to_stage.assert_not_called()
        mgr.create_stage.assert_not_called()
        # The role's CREATE WORKSPACE privilege is no longer probed at deploy.
        mgr.role_can_create_workspace.assert_not_called()
        # The build reads from the workspace source URI, not a stage.
        build_kwargs = mgr.build_app_artifact_repo.call_args.kwargs
        assert "source_uri" in build_kwargs
        assert "stage_fqn" not in build_kwargs
        # The temporary workspace is dropped once the build consumes it.
        mgr.drop_workspace_if_exists.assert_called_once_with(workspace_fqn)

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_deploy_regular_db_uses_stage_and_drops_it(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """A regular-database target with an explicit source.stage uploads to
        the stage and drops it once the build has consumed it."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(
            None, False, False, False, interactive=False, target="prod"
        )

        stage_fqn = FQN(database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP_CODE")
        mgr.upload_to_stage.assert_called_once()
        mgr.upload_to_workspace.assert_not_called()
        build_kwargs = mgr.build_app_artifact_repo.call_args.kwargs
        assert build_kwargs.get("stage_fqn") == stage_fqn
        assert "source_uri" not in build_kwargs
        # The stage the upload created is dropped after the build consumes it.
        mgr.drop_stage_if_exists.assert_called_once_with(stage_fqn)

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_deploy_regular_db_no_code_storage_uses_temporary_stage(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """A regular-database target with no code storage configured provisions
        a temporary ``<name>_CODE`` stage, builds from it, and drops it once the
        build has consumed it — without probing CREATE WORKSPACE privileges."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML_REGULAR_DB_NO_CODE_STORAGE)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "reg.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(None, False, False, False, interactive=False, target=None)

        mgr.role_can_create_workspace.assert_not_called()
        stage_fqn = FQN(database="SNOWFLAKE_APPS", schema="PUBLIC", name="REG_APP_CODE")
        mgr.upload_to_stage.assert_called_once()
        mgr.upload_to_workspace.assert_not_called()
        build_kwargs = mgr.build_app_artifact_repo.call_args.kwargs
        assert build_kwargs.get("stage_fqn") == stage_fqn
        assert "source_uri" not in build_kwargs
        # The temporary stage is dropped once the build consumes it.
        mgr.drop_stage_if_exists.assert_called_once_with(stage_fqn)

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_build_only_drops_temporary_stage_from_naming_convention(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        """A ``--build-only`` run for a regular-database target with no code
        storage configured skips the upload but still finds the temporary
        ``<name>_CODE`` stage by its naming convention and drops it once the
        build finishes."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML_REGULAR_DB_NO_CODE_STORAGE)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = ["DONE"]

        result = snowflake_app_deploy(
            None, False, True, False, interactive=False, target=None
        )

        assert "Build completed successfully." in result.message
        # No upload happened, but the deterministic name still lets the build
        # phase drop the temporary stage a prior --upload-only would have made.
        stage_fqn = FQN(database="SNOWFLAKE_APPS", schema="PUBLIC", name="REG_APP_CODE")
        mgr.upload_to_stage.assert_not_called()
        build_kwargs = mgr.build_app_artifact_repo.call_args.kwargs
        assert build_kwargs.get("stage_fqn") == stage_fqn
        mgr.drop_stage_if_exists.assert_called_once_with(stage_fqn)

    _CNG_APP_YML = dedent(
        """\
        version: 2
        name: CNG_APP
        database: SNOWFLAKE_APPS
        schema: PUBLIC
        query_warehouse: WH
        package_name: CNG_APP
        code_stage: SNOWFLAKE_APPS.PUBLIC.CNG_CODE
        artifact_repo: SNOWFLAKE_APPS.PUBLIC.CNG_REPO
        targets:
          prod:
            database: SNOWFLAKE_APPS
            schema: PUBLIC
            name: CNG_APP
            query_warehouse: WH
            compute_resource: SERVERLESS
            url_prefix: CNG_APP
            health_check: /healthz
        """
    )

    @patch(f"{_COMMANDS}._ensure_cng_url_cert_ready")
    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_cng_target_runs_cert_precheck_and_emits_compute_resource(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, mock_cert, tmp_path
    ):
        """A SERVERLESS target (flag on) runs the per-account cert precheck and
        forwards COMPUTE_RESOURCE to CREATE OR ALTER."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(self._CNG_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "cng.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        with patch.object(_CNG_FLAG, "is_enabled", return_value=True):
            snowflake_app_deploy(
                None,
                False,
                False,
                False,
                interactive=False,
                provision_certs=True,
                target="prod",
            )

        # Precheck runs before the app exists, honouring --provision-certs and
        # treating a missing cert as fatal for a full deploy.
        mock_cert.assert_called_once()
        assert mock_cert.call_args.kwargs["provision"] is True
        assert mock_cert.call_args.kwargs["required"] is True
        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["compute_resource"] == "SERVERLESS"
        # url_prefix and health_check are CNG-only fields: emitted on the
        # serverless path.
        spec = yaml.safe_load(call["specification"])
        assert spec["url_prefix"] == "CNG_APP"
        assert spec["health_check"] == "/healthz"

    @patch(f"{_COMMANDS}._ensure_cng_url_cert_ready")
    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_compute_resource_ignored_when_flag_disabled(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, mock_cert, tmp_path
    ):
        """CNG is off by default even though app.yml v2 is on: compute_resource
        is not honoured, so there is no cert precheck and no COMPUTE_RESOURCE."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(self._CNG_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "cng.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        snowflake_app_deploy(
            None, False, False, False, interactive=False, target="prod"
        )

        mock_cert.assert_not_called()
        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["compute_resource"] is None
        # Without the CNG path there is no url_prefix or health_check to emit.
        spec = yaml.safe_load(call["specification"])
        assert "url_prefix" not in spec
        assert "health_check" not in spec

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_upload_only_skips_build_and_deploy(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())

        snowflake_app_deploy(None, True, False, False, interactive=False, target="prod")

        mgr.upload_to_stage.assert_called_once()
        mgr.build_app_artifact_repo.assert_not_called()
        mgr.create_or_alter_app_service.assert_not_called()

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_build_only_skips_deploy(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = ["DONE"]

        snowflake_app_deploy(None, False, True, False, interactive=False, target="prod")

        mgr.build_app_artifact_repo.assert_called_once()
        mgr.create_or_alter_app_service.assert_not_called()

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_promote_only_skips_upload_and_build(
        self, mock_ctx, mock_mgr_cls, mock_bundle, mock_poll, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        # Only the endpoint wait polls; there is no build phase to wait on.
        mock_poll.side_effect = [
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        result = snowflake_app_deploy(
            None, False, False, True, interactive=False, target="prod"
        )

        assert "prod.snowflakecomputing.app" in result.message
        # Upload and build phases are skipped; only the deploy phase runs.
        mock_bundle.assert_not_called()
        mgr.upload_to_stage.assert_not_called()
        mgr.build_app_artifact_repo.assert_not_called()
        call = mgr.create_or_alter_app_service.call_args.kwargs
        assert call["package_name"] == "MY_APP"
        assert call["version"] == "LATEST"

    @patch(f"{_COMMANDS}.get_cli_context")
    def test_target_without_app_yml_raises(self, mock_ctx, tmp_path):
        from snowflake.cli._plugins.apps.commands import snowflake_app_deploy

        # No app.yml present -> --target is not valid for the snowflake.yml flow.
        mock_ctx.return_value = _make_ctx(tmp_path)
        with pytest.raises(CliError, match="--target is only supported"):
            snowflake_app_deploy(
                None, False, False, False, interactive=False, target="dev"
            )


_SNOWFLAKE_YML_OTHER = """definition_version: '2'
entities:
  my_app:
    type: snowflake-app
    identifier: OTHER_APP
    artifacts:
      - src: "*"
        dest: ./
    query_warehouse: SNOWFLAKE_YML_WH
"""


class TestAppYmlRoutingEndToEnd:
    """``snow app deploy`` routing when app.yml drives the SAR flow.

    Exercises the real ``with_app_flow_routing`` decorator so that an
    ``app.yml`` (version 2) project routes to the Snowflake App Runtime flow
    without requiring a ``snowflake.yml``.
    """

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    def test_deploy_with_only_app_yml(
        self, mock_mgr_cls, mock_bundle, mock_poll, runner, tmp_path
    ):
        from tests_common import change_directory

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        with change_directory(tmp_path):
            result = runner.invoke(["app", "deploy", "--target", "prod"])

        assert result.exit_code == 0, result.output
        assert "prod.snowflakecomputing.app" in result.output
        mgr.create_or_alter_app_service.assert_called_once()
        # The snowflake.yml create/upgrade path is never used.
        mgr.create_app_service.assert_not_called()

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    def test_app_yml_wins_when_both_present(
        self, mock_mgr_cls, mock_bundle, mock_poll, runner, tmp_path
    ):
        from tests_common import change_directory

        # Both manifests exist; snowflake.yml must be ignored for the SAR flow.
        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        (tmp_path / "snowflake.yml").write_text(_SNOWFLAKE_YML_OTHER)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "prod.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        with change_directory(tmp_path):
            result = runner.invoke(["app", "deploy", "--target", "prod"])

        assert result.exit_code == 0, result.output
        call = mgr.create_or_alter_app_service.call_args.kwargs
        # Package + warehouse come from app.yml, not snowflake.yml.
        assert call["package_name"] == "MY_APP"
        spec = yaml.safe_load(call["specification"])
        assert spec["query_warehouse"] == "PROD_WH"
        mgr.create_app_service.assert_not_called()

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    def test_deploy_defaults_to_targets_default_without_snowflake_yml(
        self, mock_mgr_cls, mock_bundle, mock_poll, runner, tmp_path
    ):
        from tests_common import change_directory

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mgr = _make_manager_mock(mock_mgr_cls)
        mock_bundle.return_value = Mock(bundle_root=tmp_path, clean_up_output=Mock())
        mock_poll.side_effect = [
            "DONE",
            {"url": "dev.snowflakecomputing.app", "is_upgrading": "false"},
        ]

        with change_directory(tmp_path):
            result = runner.invoke(["app", "deploy"])

        assert result.exit_code == 0, result.output
        call = mgr.create_or_alter_app_service.call_args.kwargs
        # Default target "dev" -> personal database, dev service name.
        assert call["service_fqn"] == FQN(
            database="USER$TESTUSER", schema="PUBLIC", name="MY_APP_DEV"
        )

    def test_native_only_command_unaffected_by_app_yml(self, runner, tmp_path):
        from tests_common import change_directory

        # A native-only command in an app.yml-only project must still require a
        # snowflake.yml (native flow is unchanged), not route to the SAR flow.
        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        with change_directory(tmp_path):
            result = runner.invoke(["app", "run"])
        assert result.exit_code != 0
        assert "snowflake.yml" in result.output


class TestSharedCommandsFromAppYml:
    """``bundle`` / ``validate`` / ``open`` / ``events`` / ``teardown`` resolve
    the target from ``app.yml`` (no ``snowflake.yml`` required)."""

    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_bundle_uses_source_artifacts(self, mock_ctx, mock_bundle, tmp_path):
        from types import SimpleNamespace

        from snowflake.cli._plugins.apps.commands import snowflake_app_bundle

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mock_bundle.return_value = Mock(bundle_root=tmp_path / "b")

        result = snowflake_app_bundle(None)

        # Bundling is target-independent: it uploads the whole project root
        # (./*) minus the global ``ignore`` list.
        bundle_id, bundle_obj = mock_bundle.call_args[0]
        assert bundle_id == "MY_APP"
        assert isinstance(bundle_obj, SimpleNamespace)
        assert bundle_obj.artifacts[0].src == "./*"
        assert bundle_obj.artifacts[0].ignore == ["node_modules"]
        assert "Bundle generated at" in result.message

    @patch(f"{_COMMANDS}.perform_bundle")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_validate_checks_destination_and_bundles(
        self, mock_ctx, mock_mgr_cls, mock_bundle, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_validate

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mgr.database_exists.return_value = True
        mgr.schema_exists.return_value = True
        mock_bundle.return_value = Mock(clean_up_output=Mock())

        result = snowflake_app_validate(None, target="prod")

        mgr.database_exists.assert_called_once_with("SNOWFLAKE_APPS")
        mgr.schema_exists.assert_called_once_with("SNOWFLAKE_APPS", "PUBLIC")
        mock_bundle.return_value.clean_up_output.assert_called_once()
        assert "Valid Snowflake App Runtime project." in result.message

    @patch(f"{_COMMANDS}.typer.launch")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_open_resolves_endpoint_for_target(
        self, mock_ctx, mock_mgr_cls, mock_launch, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_open

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mgr.get_service_endpoint_url.return_value = "https://prod.example.app"

        result = snowflake_app_open(None, False, False, target="prod")

        mgr.get_service_endpoint_url.assert_called_once_with(
            FQN(database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP")
        )
        mock_launch.assert_called_once_with("https://prod.example.app")
        assert result.message == "https://prod.example.app"

    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_events_tails_logs_for_target(self, mock_ctx, mock_mgr_cls, tmp_path):
        from snowflake.cli._plugins.apps.commands import snowflake_app_events

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mgr.get_service_logs.return_value = "line-1\nline-2"

        result = snowflake_app_events(None, None, target="prod")

        service_fqn = mgr.get_service_logs.call_args[0][0]
        assert service_fqn == FQN(
            database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP"
        )
        assert result.message == "line-1\nline-2"

    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_teardown_drops_service_and_code_stage(
        self, mock_ctx, mock_mgr_cls, tmp_path
    ):
        from snowflake.cli._plugins.apps.commands import snowflake_app_teardown
        from snowflake.connector.errors import ProgrammingError

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        # After the drop, describing the service fails -> confirmed gone.
        mgr.describe_app_service.side_effect = ProgrammingError("does not exist")

        result = snowflake_app_teardown(None, True, target="prod")

        mgr.drop_app_service_if_exists.assert_called_once_with(
            FQN(database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP")
        )
        mgr.drop_stage_if_exists.assert_called_once_with(
            FQN(database="SNOWFLAKE_APPS", schema="PUBLIC", name="MY_APP_CODE")
        )
        assert "Successfully dropped application service" in result.message

    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_teardown_personal_db_drops_temporary_workspace(
        self, mock_ctx, mock_mgr_cls, tmp_path
    ):
        """A personal-database target with no code storage is torn down by
        dropping its temporary ``<app>_CODE`` workspace outright (the CLI owns
        it), rather than clearing a subdirectory or dropping a stage."""
        from snowflake.cli._plugins.apps.commands import snowflake_app_teardown
        from snowflake.connector.errors import ProgrammingError

        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML_PERSONAL_DB)
        mock_ctx.return_value = _make_ctx(tmp_path)
        mgr = _make_manager_mock(mock_mgr_cls)
        mgr.describe_app_service.side_effect = ProgrammingError("does not exist")

        result = snowflake_app_teardown(None, True, target="dev")

        mgr.drop_workspace_if_exists.assert_called_once_with(
            FQN(database="USER$TESTUSER", schema="PUBLIC", name="WS_APP_DEV_CODE"),
        )
        mgr.clear_workspace_subdirectory.assert_not_called()
        mgr.drop_stage_if_exists.assert_not_called()
        assert "Successfully dropped application service" in result.message

    @pytest.mark.parametrize(
        "call",
        [
            lambda: __import__(
                "snowflake.cli._plugins.apps.commands", fromlist=["x"]
            ).snowflake_app_validate(None, target="dev"),
            lambda: __import__(
                "snowflake.cli._plugins.apps.commands", fromlist=["x"]
            ).snowflake_app_open(None, False, False, target="dev"),
            lambda: __import__(
                "snowflake.cli._plugins.apps.commands", fromlist=["x"]
            ).snowflake_app_events(None, None, target="dev"),
            lambda: __import__(
                "snowflake.cli._plugins.apps.commands", fromlist=["x"]
            ).snowflake_app_teardown(None, True, target="dev"),
        ],
    )
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    @patch(f"{_COMMANDS}.get_cli_context")
    def test_target_without_app_yml_raises(
        self, mock_ctx, mock_mgr_cls, call, tmp_path
    ):
        # No app.yml -> --target is not valid for the snowflake.yml flow.
        mock_ctx.return_value = _make_ctx(tmp_path)
        _make_manager_mock(mock_mgr_cls)
        with pytest.raises(CliError, match="--target is only supported"):
            call()
