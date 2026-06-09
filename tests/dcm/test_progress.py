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
"""Unit tests for the DCM deploy/plan live progress tracker rendering."""

from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock

from snowflake.cli._plugins.dcm.progress import (
    _SPINNER_FRAMES,
    UPLOAD_PHASE,
    DeployProgressTracker,
    PhaseStatus,
)


def _stripped_lines(text):
    return [line for line in text.split("\n") if line.strip()]


class TestUploadDetailsLayout:
    """The UPLOAD phase line comes first, followed by dim indented details."""

    def _tracker(self, *, operation="deploy", with_context=True, file_total=3):
        tracker = DeployProgressTracker(conn=MagicMock(), operation=operation)
        if with_context:
            tracker.set_upload_context(
                stage_message="Creating temporary stage inside DCM_DEMO.PROJECTS.",
                file_summaries=[
                    "Upload manifest.yml",
                    "Upload 2 files from sources/definitions",
                ],
            )
        tracker.set_upload_file_total(file_total)
        return tracker

    def test_upload_line_renders_before_details(self):
        tracker = self._tracker()

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert lines[0].startswith(UPLOAD_PHASE)
        assert lines[1].lstrip() == "Creating temporary stage inside DCM_DEMO.PROJECTS."
        assert lines[2].lstrip() == "Upload manifest.yml"
        assert lines[3].lstrip() == "Upload 2 files from sources/definitions"

    def test_details_are_indented_two_spaces(self):
        tracker = self._tracker()

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert lines[0] == lines[0].lstrip()
        for detail_line in lines[1:4]:
            assert detail_line.startswith("  ")
            assert not detail_line.startswith("   ")

    def test_subsequent_phases_render_below_upload_details(self):
        tracker = self._tracker(operation="deploy")

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert lines[0].startswith("UPLOAD")
        assert lines[4].startswith("RENDER")
        assert lines[5].startswith("COMPILE")
        assert lines[6].startswith("PLAN")
        assert lines[7].startswith("DEPLOY")

    def test_plan_mode_uses_plan_phase_list(self):
        tracker = self._tracker(operation="plan")

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert lines[0].startswith("UPLOAD")
        assert lines[4].startswith("RENDER")
        assert lines[5].startswith("COMPILE")
        assert lines[6].startswith("PLAN")
        assert all(not line.startswith("DEPLOY") for line in lines)

    def test_purge_mode_has_no_upload_and_renders_purge_label(self):
        # purge runs entirely server-side: no UPLOAD phase, and the final
        # DEPLOY phase is displayed as PURGE.
        tracker = DeployProgressTracker(conn=MagicMock(), operation="purge")

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert all(not line.startswith("UPLOAD") for line in lines)
        assert lines[0].startswith("RENDER")
        assert lines[1].startswith("COMPILE")
        assert lines[2].startswith("PLAN")
        assert lines[3].startswith("PURGE")
        assert all(not line.startswith("DEPLOY") for line in lines)

    def test_running_progress_phase_shows_pip_style_bar(self):
        """PLAN/DEPLOY phases that report 0–100 progress render a
        heavy-horizontal pip-style bar with a ``╺`` leading edge while in
        progress (no surrounding brackets, no block characters)."""
        tracker = DeployProgressTracker(conn=MagicMock(), operation="deploy")
        tracker.complete_upload()
        # Set DEPLOY mid-progress so we get both filled and empty halves.
        tracker._get_phase("DEPLOY").observe_running(50, datetime.now())  # noqa: SLF001

        rendered = tracker._render().plain  # noqa: SLF001
        deploy_line = next(line for line in rendered.split("\n") if "DEPLOY" in line)

        assert "━" in deploy_line
        assert "╺" in deploy_line  # the leading-edge transition cell
        assert " 50%" in deploy_line
        # Old block-style chars are gone.
        assert "█" not in deploy_line
        assert "░" not in deploy_line

    def test_running_no_progress_phase_shows_spinner_glyph(self):
        """COMPILE (and PLAN in plan mode) have no progress bar — they show
        an animated braille spinner where the running indicator goes."""
        tracker = DeployProgressTracker(conn=MagicMock(), operation="compile")
        tracker.complete_upload()
        # Mark COMPILE running, then verify a spinner glyph appears on its line.
        tracker._get_phase("COMPILE").observe_running(0, datetime.now())  # noqa: SLF001

        rendered = tracker._render().plain  # noqa: SLF001
        compile_line = next(line for line in rendered.split("\n") if "COMPILE" in line)

        # The static "…" placeholder is gone; one of the braille spinner
        # frames is on the line instead.
        assert "…" not in compile_line
        assert any(frame in compile_line for frame in _SPINNER_FRAMES)

    def test_compile_mode_renders_upload_render_compile(self):
        tracker = self._tracker(operation="compile")

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert lines[0].startswith("UPLOAD")
        assert lines[4].startswith("RENDER")
        assert lines[5].startswith("COMPILE")
        # ANALYZE is an implementation detail and is never shown as a phase;
        # neither are the later PLAN/DEPLOY phases.
        assert all(not line.startswith(("PLAN", "DEPLOY", "ANALYZE")) for line in lines)

    def test_no_details_block_when_context_is_unset(self):
        tracker = self._tracker(with_context=False)

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert all(not line.startswith("  ") for line in lines)


class TestFailUpload:
    """``fail_upload`` must only mark UPLOAD failed when UPLOAD itself failed.

    Downstream phase failures (PLAN, DEPLOY, …) bubble up through
    ``session()``'s exception handler, which always calls ``fail_upload``.
    UPLOAD has already been marked ``done`` by that point and must not be
    flipped back to ``failed``.
    """

    def _tracker(self):
        tracker = DeployProgressTracker(conn=MagicMock(), operation="deploy")
        tracker.set_upload_file_total(2)
        return tracker

    def _upload_phase(self, tracker):
        return tracker._get_phase(UPLOAD_PHASE)  # noqa: SLF001

    def test_fail_upload_marks_running_upload_failed(self):
        """If UPLOAD itself is still running, ``fail_upload`` flips it failed."""
        tracker = self._tracker()
        tracker.start_upload()
        assert self._upload_phase(tracker).status == PhaseStatus.RUNNING

        tracker.fail_upload()

        assert self._upload_phase(tracker).status == PhaseStatus.FAILED

    def test_fail_upload_is_noop_when_upload_already_done(self):
        """A downstream PLAN/DEPLOY failure must not flip a finished UPLOAD."""
        tracker = self._tracker()
        tracker.start_upload()
        tracker.complete_upload()
        assert self._upload_phase(tracker).status == PhaseStatus.DONE

        tracker.fail_upload()

        assert self._upload_phase(tracker).status == PhaseStatus.DONE

    def test_fail_upload_is_idempotent_when_already_failed(self):
        tracker = self._tracker()
        tracker.start_upload()
        tracker.fail_upload()
        first_completed_at = self._upload_phase(tracker).completed_at
        assert first_completed_at is not None

        tracker.fail_upload()

        assert self._upload_phase(tracker).status == PhaseStatus.FAILED
        # The original failure timestamp is preserved on repeated calls.
        assert self._upload_phase(tracker).completed_at == first_completed_at


class TestUpdateFromPoll:
    """``_update_from_poll`` must not mutate phase state from junk payloads.

    Regression coverage for an earlier bug where an empty / missing /
    unknown ``phase`` field on the very first poll flipped every server-side
    phase to ``done`` while the query was still running — the UI showed a
    fully-green checklist immediately.
    """

    def _tracker(self):
        t = DeployProgressTracker(conn=MagicMock(), operation="deploy")
        t.complete_upload()
        return t

    def _server_phases(self, tracker):
        return [p for p in tracker._phases if p.name != UPLOAD_PHASE]  # noqa: SLF001

    def test_empty_phase_string_is_ignored(self):
        tracker = self._tracker()

        tracker._update_from_poll({"phase": "", "progress": 0})  # noqa: SLF001

        assert all(
            p.status == PhaseStatus.PENDING for p in self._server_phases(tracker)
        )

    def test_missing_phase_key_is_ignored(self):
        tracker = self._tracker()

        tracker._update_from_poll({"progress": 0})  # noqa: SLF001

        assert all(
            p.status == PhaseStatus.PENDING for p in self._server_phases(tracker)
        )

    def test_unknown_phase_name_is_ignored(self):
        tracker = self._tracker()

        tracker._update_from_poll({"phase": "BUILD", "progress": 50})  # noqa: SLF001

        assert all(
            p.status == PhaseStatus.PENDING for p in self._server_phases(tracker)
        )

    def test_non_string_phase_is_ignored(self):
        tracker = self._tracker()

        tracker._update_from_poll({"phase": 42, "progress": 0})  # noqa: SLF001

        assert all(
            p.status == PhaseStatus.PENDING for p in self._server_phases(tracker)
        )

    def test_known_phase_advances_state(self):
        """Sanity: a well-formed payload still drives the state machine."""
        tracker = self._tracker()

        tracker._update_from_poll({"phase": "PLAN", "progress": 25})  # noqa: SLF001

        statuses = {p.name: p.status for p in self._server_phases(tracker)}
        # Phases earlier than PLAN are auto-completed; PLAN itself runs;
        # later phases remain PENDING.
        assert statuses["RENDER"] == PhaseStatus.DONE
        assert statuses["COMPILE"] == PhaseStatus.DONE
        assert statuses["PLAN"] == PhaseStatus.RUNNING
        assert statuses["DEPLOY"] == PhaseStatus.PENDING


class TestRunLoaderPhaseSilent:
    """In silent mode (JSON/CSV/``--silent``), ``run_loader_phase`` must not
    mutate phase state — there's no UI to drive and stale RUNNING entries
    would mislead any code that introspects the tracker later."""

    def _tracker(self, operation="plan"):
        return DeployProgressTracker(conn=MagicMock(), operation=operation)

    def test_silent_mode_skips_phase_transitions(self):
        tracker = self._tracker(operation="plan")
        before = [(p.name, p.status) for p in tracker._phases]  # noqa: SLF001

        with mock.patch("snowflake.cli._plugins.dcm.progress.get_cli_context") as ctx:
            ctx.return_value.silent = True
            sentinel = object()
            result = tracker.run_loader_phase(
                lambda: sentinel,
                phase_name="PLAN",
                simulated_phases=("RENDER", "COMPILE"),
            )

        assert result is sentinel
        after = [(p.name, p.status) for p in tracker._phases]  # noqa: SLF001
        assert before == after  # no phases touched

    def test_silent_mode_still_propagates_exceptions(self):
        tracker = self._tracker(operation="plan")

        class _BoomError(RuntimeError):
            pass

        def _raise():
            raise _BoomError("server-side failure")

        with mock.patch("snowflake.cli._plugins.dcm.progress.get_cli_context") as ctx:
            ctx.return_value.silent = True
            try:
                tracker.run_loader_phase(_raise, phase_name="PLAN")
            except _BoomError:
                pass
            else:
                raise AssertionError("Expected _BoomError")

        # Even though the call raised, no phase state was mutated.
        assert all(
            p.status == PhaseStatus.PENDING
            for p in tracker._phases  # noqa: SLF001
            if p.name != UPLOAD_PHASE
        )
