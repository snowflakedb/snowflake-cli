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
import json
import time
from pathlib import Path
from typing import List
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.commands import _effective_variables
from snowflake.cli._plugins.dcm.exceptions import InvalidManifestError
from snowflake.cli._plugins.dcm.fanout import classify
from snowflake.cli._plugins.dcm.fanout.orchestrator import (
    reject_conflicting_options,
    reject_orphaned_options,
)
from snowflake.cli._plugins.dcm.fanout.pacing import (
    ResolvedPacing,
    derive_pace,
    observe_duration,
    resolve_pacing,
)
from snowflake.cli._plugins.dcm.fanout.provision import (
    _container_error,
    validate_project_owner,
)
from snowflake.cli._plugins.dcm.fanout.runner import (
    AttemptResult,
    InvocationTemplate,
    connection_arguments,
    parse_operation_count,
)
from snowflake.cli._plugins.dcm.fanout.scheduler import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    FanoutScheduler,
    backoff_delay,
)
from snowflake.cli._plugins.dcm.fanout.targets import (
    FanoutTarget,
    PacingConfig,
    filter_targets,
    select_targets,
)
from snowflake.cli._plugins.dcm.models import DCMManifest, DCMTarget, TargetContext
from snowflake.cli.api.connections import ConnectionContext
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.connector import ProgrammingError

# The rejection exactly as the CLI renders it: framed in a box that wraps the
# message mid-phrase. Any classifier that searches for a phrase rather than the
# numeric codes fails against this.
BOXED_CAPACITY_REJECTION = (
    "╭─ Error ─────────────────────────────────────────────╮\n"
    "│ 001617 (53400): Too many concurrent calls to the    │\n"
    "│ Jinja2 templating engine: Max number of running     │\n"
    "│ operations (5) exceeded for account                 │\n"
    "╰─────────────────────────────────────────────────────╯\n"
)

BOXED_DEFINITION_ERROR = (
    "╭─ Error ─────────────────────────────────────────────╮\n"
    "│ 002003 (42S02): SQL compilation error: Object       │\n"
    "│ 'MY_DB.MY_SCHEMA.MISSING_TABLE' does not exist or   │\n"
    "│ not authorized.                                     │\n"
    "╰─────────────────────────────────────────────────────╯\n"
)


def _manifest(target_names: List[str], overrides: dict | None = None) -> DCMManifest:
    overrides = overrides or {}
    return DCMManifest.from_dict(
        {
            "manifest_version": 2,
            "type": "dcm_project",
            "targets": {
                name: {
                    "project_name": f"MY_DB.MY_SCHEMA.{name}",
                    "account_identifier": "MY_ORG-MY_ACCOUNT",
                    "project_owner": "MY_ROLE",
                    **(
                        {"variable_overrides": overrides[name]}
                        if name in overrides
                        else {}
                    ),
                }
                for name in target_names
            },
        }
    )


class TestRetryClassifier:
    def test_boxed_capacity_rejection_is_retryable(self):
        assert classify.is_retryable(BOXED_CAPACITY_REJECTION) is True

    def test_boxed_definition_error_is_not_retryable(self):
        assert classify.is_retryable(BOXED_DEFINITION_ERROR) is False

    @pytest.mark.parametrize("text", [None, "", "   "])
    def test_empty_output_is_not_retryable(self, text):
        assert classify.is_retryable(text) is False

    def test_permission_error_is_not_retryable(self):
        text = (
            "003001 (42501): Insufficient privileges to operate on schema 'MY_SCHEMA'"
        )
        assert classify.is_retryable(text) is False

    def test_normalisation_rejoins_a_phrase_the_box_split(self):
        normalized = classify.normalize_error_text(BOXED_CAPACITY_REJECTION)
        assert "Too many concurrent calls to the Jinja2 templating engine" in normalized
        assert "│" not in normalized and "─" not in normalized

    def test_observed_cap_is_parsed_rather_than_assumed(self):
        assert classify.parse_cap_value(BOXED_CAPACITY_REJECTION) == 5

    def test_a_raised_cap_is_reported_as_raised(self):
        text = (
            "001617 (53400): Max number of running operations (20) exceeded for account"
        )
        assert classify.parse_cap_value(text) == 20

    def test_cap_value_absent_when_the_message_states_none(self):
        assert classify.parse_cap_value(BOXED_DEFINITION_ERROR) is None

    def test_a_number_merely_containing_the_code_is_not_a_rejection(self):
        assert classify.is_capacity_rejection("error 1000016170 occurred") is False


class TestTargetSelection:
    def test_a_pattern_selects_the_matching_targets(self):
        manifest = _manifest(["TENANT_A", "TENANT_B", "SHARED"])
        selected = select_targets(manifest, "TENANT_*")
        assert [target.name for target in selected] == ["TENANT_A", "TENANT_B"]

    def test_a_star_selects_every_target(self):
        manifest = _manifest(["TENANT_A", "SHARED"])
        assert len(select_targets(manifest, "*")) == 2

    def test_a_lower_case_pattern_matches(self):
        # Manifest target names are upper-cased on load, so the pattern is too.
        manifest = _manifest(["TENANT_A"])
        assert len(select_targets(manifest, "tenant_*")) == 1

    def test_order_follows_the_manifest(self):
        manifest = _manifest(["TENANT_C", "TENANT_A", "TENANT_B"])
        selected = select_targets(manifest, "*")
        # Declaration order, so a rerun starts targets as the original run did.
        assert [target.name for target in selected] == [
            "TENANT_C",
            "TENANT_A",
            "TENANT_B",
        ]

    def test_a_pattern_matching_nothing_lists_what_exists(self):
        manifest = _manifest(["TENANT_A", "SHARED"])
        with pytest.raises(CliError) as error:
            select_targets(manifest, "GHOST_*")
        message = str(error.value)
        assert "GHOST_*" in message
        assert "TENANT_A" in message and "SHARED" in message

    @pytest.mark.parametrize("pattern", ["", "   "])
    def test_an_empty_pattern_is_rejected(self, pattern):
        with pytest.raises(CliError) as error:
            select_targets(_manifest(["TENANT_A"]), pattern)
        assert "--fanout" in str(error.value)

    def test_overrides_travel_from_the_manifest_target(self):
        manifest = _manifest(
            ["TENANT_A", "TENANT_B"],
            overrides={"TENANT_A": {"env_suffix": "_A"}},
        )
        selected = {target.name: target for target in select_targets(manifest, "*")}
        assert selected["TENANT_A"].variable_arguments() == ["env_suffix='_A'"]
        # A target without overrides is not an error, it simply supplies nothing.
        assert selected["TENANT_B"].variable_arguments() == []


class TestManifestOverrides:
    def test_overrides_are_quoted_as_string_literals(self):
        target = _manifest(["T"], overrides={"T": {"env_suffix": "_0001"}}).targets["T"]
        assert target.variable_arguments() == ["env_suffix='_0001'"]

    def test_a_quote_inside_a_value_is_escaped(self):
        target = _manifest(["T"], overrides={"T": {"label": "O'Brien"}}).targets["T"]
        assert target.variable_arguments() == ["label='O''Brien'"]

    def test_numeric_and_boolean_values_stay_string_literals(self):
        target = _manifest(["T"], overrides={"T": {"count": 3, "flag": True}}).targets[
            "T"
        ]
        assert target.variable_arguments() == ["count='3'", "flag='true'"]

    def test_a_target_without_overrides_has_none(self):
        assert _manifest(["T"]).targets["T"].variable_overrides == {}

    @pytest.mark.parametrize(
        "value,expected",
        [
            (["env_suffix"], "not a mapping"),
            ("env_suffix=_A", "not a mapping"),
            ({"": "_A"}, "non-empty strings"),
            ({3: "_A"}, "non-empty strings"),
        ],
    )
    def test_a_malformed_overrides_block_is_rejected(self, value, expected):
        with pytest.raises(InvalidManifestError) as error:
            _manifest(["T"], overrides={"T": value})
        message = str(error.value)
        assert expected in message
        assert "'T'" in message


class TestOnlyFilter:
    def test_subset_is_selected_in_selection_order(self):
        targets = [FanoutTarget(name=name) for name in ("A", "B", "C")]
        selected = filter_targets(targets, "C,A")
        assert [target.name for target in selected] == ["A", "C"]

    def test_names_are_matched_case_insensitively(self):
        targets = [FanoutTarget(name="TENANT_A")]
        assert len(filter_targets(targets, "tenant_a")) == 1

    def test_unknown_name_is_rejected(self):
        targets = [FanoutTarget(name="A")]
        with pytest.raises(CliError) as error:
            filter_targets(targets, "A,GHOST")
        assert "GHOST" in str(error.value)

    def test_empty_value_is_rejected(self):
        with pytest.raises(CliError):
            filter_targets([FanoutTarget(name="A")], " , ")

    def test_absent_option_keeps_every_target(self):
        targets = [FanoutTarget(name="A"), FanoutTarget(name="B")]
        assert filter_targets(targets, None) == targets


class TestPaceDerivation:
    def test_pace_holds_inflight_at_the_ceiling(self):
        # 100s per invocation across 25 concurrent slots needs a start every 4s.
        assert (
            derive_pace(min_seconds=0.5, max_inflight=25, measured_duration=100) == 4.0
        )

    def test_min_seconds_is_a_floor(self):
        assert derive_pace(min_seconds=0.5, max_inflight=25, measured_duration=5) == 0.5

    def test_deploy_paces_materially_wider_than_plan(self):
        # A deploy measures several times longer than a plan on the same project,
        # so reusing a plan-derived pace for a deploy over-subscribes the runner.
        plan_pace = derive_pace(0.5, 25, measured_duration=58.0)
        deploy_pace = derive_pace(0.5, 25, measured_duration=204.7)
        assert deploy_pace > plan_pace
        assert deploy_pace / plan_pace == pytest.approx(204.7 / 58.0, rel=1e-6)

    def test_zero_ceiling_is_rejected(self):
        with pytest.raises(CliError):
            derive_pace(0.5, 0, 100)

    def test_the_first_measurement_replaces_the_floor_with_the_derived_pace(self):
        pacing = ResolvedPacing(
            min_seconds=0.5, max_inflight=25, calibrate=True, pace=0.5
        )
        observe_duration(pacing, measured_duration=100.0)
        assert pacing.pace == 4.0
        assert pacing.calibrated_from_seconds == 100.0
        assert pacing.is_calibrated
        assert pacing.pace_samples == 1

    def test_the_pace_follows_the_mean_of_the_observed_durations(self):
        pacing = ResolvedPacing(
            min_seconds=0.0, max_inflight=10, calibrate=True, pace=0.0
        )
        for duration in (100.0, 200.0, 300.0):
            observe_duration(pacing, duration)
        # Mean 200s over a ceiling of 10 implies a start every 20s.
        assert pacing.calibrated_from_seconds == 200.0
        assert pacing.pace == 20.0

    def test_only_the_most_recent_durations_count(self):
        pacing = ResolvedPacing(
            min_seconds=0.0, max_inflight=1, calibrate=True, pace=0.0, window=2
        )
        for duration in (10.0, 100.0, 200.0):
            observe_duration(pacing, duration)
        # The 10s sample has aged out of a two-wide window.
        assert pacing.pace_samples == 2
        assert pacing.calibrated_from_seconds == 150.0

    def test_an_unmeasured_invocation_leaves_the_pace_alone(self):
        pacing = ResolvedPacing(
            min_seconds=0.5, max_inflight=25, calibrate=True, pace=0.5
        )
        observe_duration(pacing, measured_duration=0.0)
        assert pacing.pace == 0.5
        assert pacing.is_calibrated is False
        assert pacing.pace_samples == 0


class TestPacingResolution:
    def test_defaults_apply_when_no_flag_is_given(self):
        resolved = resolve_pacing(pace=None, max_inflight=None, calibrate=None)
        defaults = PacingConfig()
        assert (resolved.min_seconds, resolved.max_inflight, resolved.calibrate) == (
            defaults.min_seconds,
            defaults.max_inflight,
            defaults.calibrate,
        )

    def test_each_flag_overrides_only_its_own_default(self):
        resolved = resolve_pacing(pace=0.25, max_inflight=None, calibrate=None)
        assert resolved.min_seconds == 0.25
        assert resolved.max_inflight == PacingConfig().max_inflight

    def test_adjustment_can_be_turned_off_from_the_command_line(self):
        resolved = resolve_pacing(pace=None, max_inflight=None, calibrate=False)
        assert resolved.calibrate is False

    def test_zero_pace_is_allowed_so_the_mechanism_can_be_disabled(self):
        resolved = resolve_pacing(pace=0.0, max_inflight=100, calibrate=False)
        assert resolved.pace == 0.0

    def test_negative_pace_is_rejected(self):
        with pytest.raises(CliError):
            resolve_pacing(pace=-1.0, max_inflight=None, calibrate=None)

    def test_zero_ceiling_is_rejected(self):
        with pytest.raises(CliError):
            resolve_pacing(pace=None, max_inflight=0, calibrate=None)


class TestOptionConflicts:
    def test_positional_identifier_is_rejected(self):
        with pytest.raises(CliError) as error:
            reject_conflicting_options(FQN.from_string("MY_PROJECT"), None, None)
        assert "project identifier" in str(error.value)

    def test_target_is_rejected(self):
        with pytest.raises(CliError) as error:
            reject_conflicting_options(None, "TENANT_A", None)
        assert "--target" in str(error.value)

    def test_variables_are_rejected(self):
        with pytest.raises(CliError) as error:
            reject_conflicting_options(None, None, ["env_suffix='_A'"])
        assert "-D/--variable" in str(error.value)

    def test_every_conflict_is_reported_together(self):
        with pytest.raises(CliError) as error:
            reject_conflicting_options(
                FQN.from_string("P"), "TENANT_A", ["env_suffix='_A'"]
            )
        message = str(error.value)
        assert "--target" in message and "-D/--variable" in message

    def test_honoured_options_do_not_conflict(self):
        reject_conflicting_options(None, None, None)
        reject_conflicting_options(None, None, [])

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            ({"pace": 1.0}, "--pace"),
            ({"max_inflight": 10}, "--max-inflight"),
            ({"calibrate": True}, "--calibrate/--no-calibrate"),
            ({"retries": 3}, "--fanout-retries"),
            ({"only": "A"}, "--only"),
            ({"list_targets": True}, "--list-targets"),
        ],
    )
    def test_fanout_option_without_fanout_is_rejected(self, kwargs, expected):
        arguments = {
            "pace": None,
            "max_inflight": None,
            "calibrate": None,
            "retries": None,
            "only": None,
            "list_targets": False,
        }
        arguments.update(kwargs)
        with pytest.raises(CliError) as error:
            reject_orphaned_options(None, **arguments)
        assert expected in str(error.value)

    def test_no_fanout_options_at_all_is_the_single_tenant_path(self):
        reject_orphaned_options(
            None,
            pace=None,
            max_inflight=None,
            calibrate=None,
            retries=None,
            only=None,
            list_targets=False,
        )


class TestInvocationTemplate:
    def _template(self, **kwargs) -> InvocationTemplate:
        defaults = dict(
            command_name="plan",
            from_location=Path("/projects/tenants"),
            connection_arguments=["--connection", "DCM_DEV"],
        )
        defaults.update(kwargs)
        return InvocationTemplate(**defaults)

    def test_argv_carries_target_project_directory_and_json_format(self):
        argv = self._template().argv_for(
            FanoutTarget(name="TENANT_A", overrides={"env_suffix": "_A"})
        )
        assert argv[-6:] == [
            "-D",
            "env_suffix='_A'",
            "--connection",
            "DCM_DEV",
            "--format",
            "json",
        ]
        assert "--target" in argv and "TENANT_A" in argv
        assert "--from" in argv

    def test_plan_forwards_delta_and_deploy_forwards_alias(self):
        plan_argv = self._template(delta=True).argv_for(FanoutTarget(name="A"))
        assert "--delta" in plan_argv
        deploy_argv = self._template(command_name="deploy", alias="nightly").argv_for(
            FanoutTarget(name="A")
        )
        assert deploy_argv[deploy_argv.index("--alias") + 1] == "nightly"

    def test_listing_renders_a_copy_pasteable_command(self):
        rendered = self._template().display_command_for(
            FanoutTarget(name="TENANT_A", overrides={"env_suffix": "_A"})
        )
        assert rendered.startswith("snow dcm plan --target TENANT_A")
        assert "-D \"env_suffix='_A'\"" in rendered


class TestConnectionForwarding:
    def test_named_connection_and_overrides_are_forwarded(self):
        arguments = connection_arguments(
            ConnectionContext(connection_name="DCM_DEV", role="MY_ROLE")
        )
        assert arguments == ["--connection", "DCM_DEV", "--role", "MY_ROLE"]

    def test_credentials_are_never_placed_on_a_child_command_line(self):
        arguments = connection_arguments(
            ConnectionContext(
                connection_name="DCM_DEV", password="hunter2", token="abc"
            )
        )
        assert "hunter2" not in arguments and "abc" not in arguments

    def test_temporary_connection_is_refused(self):
        with pytest.raises(CliError) as error:
            connection_arguments(ConnectionContext(temporary_connection=True))
        assert "--temporary-connection" in str(error.value)


class TestOperationCount:
    def test_count_is_the_changeset_length(self):
        payload = json.dumps(
            {"version": 2, "metadata": {}, "changeset": [{"a": 1}, {"b": 2}]}
        )
        assert parse_operation_count(json.dumps([{"result": payload}])) == 2

    def test_column_name_is_not_assumed(self):
        # The envelope key comes from the server's cursor description.
        payload = json.dumps({"version": 2, "changeset": []})
        assert parse_operation_count(json.dumps([{"anything": payload}])) == 0

    def test_already_decoded_payload_is_accepted(self):
        assert (
            parse_operation_count(
                json.dumps([{"result": {"version": 2, "changeset": [{"a": 1}]}}])
            )
            == 1
        )

    @pytest.mark.parametrize(
        "stdout", ["", "not json", "[]", "[{}]", json.dumps([{"result": "{}"}])]
    )
    def test_unreadable_output_yields_no_count(self, stdout):
        assert parse_operation_count(stdout) is None


class TestProjectOwnerValidation:
    def _targets(self, owner: str) -> List[DCMTarget]:
        return [
            DCMTarget.from_dict(
                {
                    "name": "TENANT_A",
                    "project_name": "MY_DB.MY_SCHEMA.TENANT_A",
                    "project_owner": owner,
                }
            )
        ]

    def test_matching_role_is_accepted(self):
        validate_project_owner(self._targets("MY_ROLE"), current_role="MY_ROLE")

    def test_mismatched_role_names_both_roles(self):
        with pytest.raises(CliError) as error:
            validate_project_owner(
                self._targets("OWNER_ROLE"), current_role="OTHER_ROLE"
            )
        message = str(error.value)
        # Creating under the wrong role silently changes who can deploy, so both
        # roles have to appear in the error.
        assert "OTHER_ROLE" in message and "OWNER_ROLE" in message

    def test_unknown_current_role_is_refused_rather_than_assumed(self):
        with pytest.raises(CliError):
            validate_project_owner(self._targets("MY_ROLE"), current_role=None)


class TestContainerErrors:
    """The two failures that actually happen at scale get named, not generalised."""

    @pytest.mark.parametrize("errno", [2003, 2043])
    def test_missing_schema_is_named_with_the_reason_it_cannot_be_created(self, errno):
        # SHOW ... IN SCHEMA against a schema that does not exist reports 2043
        # rather than 2003, and both have to reach the same guidance.
        error = _container_error(
            ProgrammingError(msg="Object does not exist", errno=errno),
            "MY_DB",
            "MISSING_SCHEMA",
        )
        message = str(error)
        assert "MY_DB.MISSING_SCHEMA" in message
        assert "cannot create its own parent database or schema" in message

    def test_missing_privilege_names_the_grant_required(self):
        error = _container_error(
            ProgrammingError(msg="Insufficient privileges", errno=3001),
            "MY_DB",
            "MY_SCHEMA",
        )
        assert "CREATE DCM PROJECT ON SCHEMA MY_DB.MY_SCHEMA" in str(error)

    def test_any_other_failure_still_names_the_schema(self):
        error = _container_error(
            ProgrammingError(msg="something else", errno=1234), "MY_DB", "MY_SCHEMA"
        )
        assert "MY_DB.MY_SCHEMA" in str(error)


class TestBackoff:
    def test_delay_grows_with_each_attempt(self):
        assert backoff_delay(1) < backoff_delay(4)

    def test_delay_is_jittered_so_retries_do_not_re_synchronise(self):
        delays = {backoff_delay(1) for _ in range(20)}
        assert len(delays) > 1

    def test_delay_is_capped(self):
        assert backoff_delay(20) <= 61.0


class _FakeRunner:
    """Stands in for the per-tenant subprocess, recording when each start happened."""

    def __init__(self, duration: float = 0.0, failures=None):
        self.duration = duration
        self.failures = failures or {}
        self.starts: List[float] = []
        self.concurrent = 0
        self.peak_concurrent = 0
        self._attempts: dict = {}

    def __call__(self, template, target):
        self.starts.append(time.monotonic())
        self.concurrent += 1
        self.peak_concurrent = max(self.peak_concurrent, self.concurrent)
        try:
            time.sleep(self.duration)
            attempt = self._attempts.get(target.name, 0) + 1
            self._attempts[target.name] = attempt
            planned = self.failures.get(target.name, [])
            if attempt <= len(planned):
                kind = planned[attempt - 1]
                return AttemptResult(
                    exit_code=1,
                    duration_seconds=self.duration,
                    error=kind,
                    retryable=kind == "capacity",
                    observed_cap=5 if kind == "capacity" else None,
                )
            return AttemptResult(
                exit_code=0, duration_seconds=self.duration, operations=3
            )
        finally:
            self.concurrent -= 1


def _scheduler(targets, runner, pace=0.0, max_inflight=5, retries=2, calibrate=False):
    return FanoutScheduler(
        template=InvocationTemplate(command_name="plan", from_location=Path(".")),
        targets=targets,
        pacing=ResolvedPacing(
            min_seconds=pace, max_inflight=max_inflight, calibrate=calibrate, pace=pace
        ),
        retry_attempts=retries,
        project_names={target.name: f"DB.SC.{target.name}" for target in targets},
        attempt_runner=runner,
        progress_interval_seconds=1000,
    )


class TestScheduler:
    def test_every_target_runs_once_and_reports_its_operation_count(self):
        targets = [FanoutTarget(name=f"T{index}") for index in range(5)]
        runner = _FakeRunner()
        result = _scheduler(targets, runner).run()
        assert len(result.succeeded) == 5
        assert {outcome.operations for outcome in result.outcomes} == {3}
        assert len(runner.starts) == 5

    def test_starts_are_spaced_by_the_pace(self):
        targets = [FanoutTarget(name=f"T{index}") for index in range(4)]
        runner = _FakeRunner()
        pace = 0.15
        _scheduler(targets, runner, pace=pace).run()
        gaps = [
            second - first for first, second in zip(runner.starts, runner.starts[1:])
        ]
        # The pace is a floor on every gap, not only on the first few: a pool that
        # refilled the instant a slot freed would show gaps near zero here.
        assert gaps and min(gaps) >= pace * 0.9

    def test_inflight_never_exceeds_the_ceiling(self):
        targets = [FanoutTarget(name=f"T{index}") for index in range(12)]
        runner = _FakeRunner(duration=0.1)
        result = _scheduler(targets, runner, pace=0.0, max_inflight=3).run()
        assert runner.peak_concurrent <= 3
        assert result.peak_inflight <= 3
        assert len(result.succeeded) == 12

    def test_a_ceiling_bound_run_says_so(self):
        targets = [FanoutTarget(name=f"T{index}") for index in range(8)]
        runner = _FakeRunner(duration=0.1)
        result = _scheduler(targets, runner, pace=0.0, max_inflight=2).run()
        assert result.is_inflight_limited

    def test_capacity_rejection_is_retried_until_it_succeeds(self):
        targets = [FanoutTarget(name="T0")]
        runner = _FakeRunner(failures={"T0": ["capacity"]})
        with mock.patch(
            "snowflake.cli._plugins.dcm.fanout.scheduler.backoff_delay",
            return_value=0.01,
        ):
            result = _scheduler(targets, runner).run()
        assert result.outcomes[0].status == STATUS_SUCCESS
        assert result.outcomes[0].attempts == 2
        assert result.capacity_rejections == 1
        assert result.observed_cap == 5

    def test_terminal_error_is_not_retried(self):
        targets = [FanoutTarget(name="T0")]
        runner = _FakeRunner(
            failures={"T0": ["definition", "definition", "definition"]}
        )
        result = _scheduler(targets, runner).run()
        assert result.outcomes[0].status == STATUS_FAILED
        assert result.outcomes[0].attempts == 1
        assert len(runner.starts) == 1

    def test_retries_stop_at_the_configured_attempt_count(self):
        targets = [FanoutTarget(name="T0")]
        runner = _FakeRunner(failures={"T0": ["capacity"] * 10})
        with mock.patch(
            "snowflake.cli._plugins.dcm.fanout.scheduler.backoff_delay",
            return_value=0.01,
        ):
            result = _scheduler(targets, runner, retries=2).run()
        assert result.outcomes[0].status == STATUS_FAILED
        assert result.outcomes[0].attempts == 3

    def test_one_failing_target_does_not_disturb_its_siblings(self):
        targets = [FanoutTarget(name=f"T{index}") for index in range(4)]
        runner = _FakeRunner(failures={"T2": ["definition"]})
        result = _scheduler(targets, runner).run()
        assert len(result.succeeded) == 3
        assert [outcome.name for outcome in result.failed] == ["T2"]

    def test_the_pace_is_derived_from_the_run_without_a_probe_target(self):
        targets = [FanoutTarget(name=f"T{index}") for index in range(3)]
        runner = _FakeRunner(duration=0.05)
        result = _scheduler(
            targets, runner, pace=0.0, max_inflight=1, calibrate=True
        ).run()
        assert result.calibrated_from_seconds is not None
        assert result.pace_seconds > 0
        assert result.pace_samples == 3
        # Every target runs exactly once: no target is measured on its own before
        # the fan-out begins, which on a deploy would serialize minutes of work.
        assert len(runner.starts) == 3
        assert len(result.succeeded) == 3

    def test_the_pace_is_left_at_the_floor_when_adjustment_is_off(self):
        targets = [FanoutTarget(name=f"T{index}") for index in range(3)]
        runner = _FakeRunner(duration=0.05)
        result = _scheduler(
            targets, runner, pace=0.0, max_inflight=1, calibrate=False
        ).run()
        assert result.calibrated_from_seconds is None
        assert result.pace_samples == 0
        assert result.pace_seconds == 0.0


class TestSingleTenantOverrides:
    """A target's variable_overrides apply on the ordinary --target path too.

    Otherwise the same manifest and the same target would render differently
    depending on whether --fanout was used.
    """

    def _context(self, overrides):
        return TargetContext(
            project_identifier=FQN.from_string("MY_DB.MY_SCHEMA.P"),
            variable_overrides=overrides,
        )

    def test_overrides_are_passed_when_the_command_line_gives_nothing(self):
        context = self._context(["env_suffix='_A'"])
        assert _effective_variables(context, None) == ["env_suffix='_A'"]

    def test_a_target_without_overrides_passes_the_command_line_through(self):
        context = self._context([])
        assert _effective_variables(context, ["dt_lag='1 hour'"]) == ["dt_lag='1 hour'"]
        assert _effective_variables(context, None) is None

    def test_both_sources_are_combined(self):
        context = self._context(["env_suffix='_A'"])
        assert _effective_variables(context, ["dt_lag='1 hour'"]) == [
            "env_suffix='_A'",
            "dt_lag='1 hour'",
        ]

    def test_the_command_line_wins_on_the_same_name(self):
        context = self._context(["env_suffix='_A'", "dt_lag='24 hours'"])
        # Merged rather than concatenated: both become named arguments of the
        # VARIABLES clause, where one name cannot appear twice.
        assert _effective_variables(context, ["env_suffix='_OVERRIDDEN'"]) == [
            "dt_lag='24 hours'",
            "env_suffix='_OVERRIDDEN'",
        ]


class TestSingleTenantBehaviourUnchanged:
    """Without --fanout, plan and deploy must behave exactly as they did before."""

    def test_deploy_still_takes_the_single_tenant_path(
        self,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        with mock.patch(
            "snowflake.cli._plugins.dcm.commands.DCMProjectManager"
        ) as manager, mock.patch(
            "snowflake.cli._plugins.dcm.commands.ServerPoll"
        ) as server_poll, mock.patch(
            "snowflake.cli._plugins.dcm.fanout.orchestrator.run_fanout"
        ) as fanout:
            manager().deploy_async.return_value = "sfqid"
            manager().sync_local_files.return_value = "TMP_STAGE"
            server_poll.return_value.run.return_value = mock_cursor(
                rows=[(json.dumps({"version": 2, "changeset": []}),)],
                columns=("result",),
            )
            with project_directory("dcm_project"):
                result = runner.invoke(["dcm", "deploy", "fooBar"])

        assert result.exit_code == 0, result.output
        fanout.assert_not_called()
        manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=[],
            alias=None,
            skip_plan=False,
            env_vars={},
            deployment_name=None,
        )

    def test_plan_still_takes_the_single_tenant_path(
        self,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        with mock.patch(
            "snowflake.cli._plugins.dcm.commands.DCMProjectManager"
        ) as manager, mock.patch(
            "snowflake.cli._plugins.dcm.commands.ServerPoll"
        ) as server_poll, mock.patch(
            "snowflake.cli._plugins.dcm.fanout.orchestrator.run_fanout"
        ) as fanout:
            manager().plan_async.return_value = "sfqid"
            manager().sync_local_files.return_value = "TMP_STAGE"
            server_poll.return_value.run.return_value = mock_cursor(
                rows=[(json.dumps({"version": 2, "changeset": []}),)],
                columns=("result",),
            )
            with project_directory("dcm_project"):
                result = runner.invoke(["dcm", "plan", "fooBar"])

        assert result.exit_code == 0, result.output
        fanout.assert_not_called()
        assert manager().plan_async.call_count == 1
        assert manager().plan_async.call_args.kwargs["variables"] == []
        assert manager().plan_async.call_args.kwargs["delta"] is False
