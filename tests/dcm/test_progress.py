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

    def test_running_no_progress_phase_shows_spinner_glyph(self):
        """ANALYZE (and PLAN in plan mode) have no progress bar — they show
        an animated braille spinner where the running indicator goes."""
        tracker = DeployProgressTracker(conn=MagicMock(), operation="analyze")
        tracker.complete_upload()
        # Mark ANALYZE running, then verify a spinner glyph appears on its line.
        tracker._get_phase("ANALYZE").observe_running(0, datetime.now())  # noqa: SLF001

        rendered = tracker._render().plain  # noqa: SLF001
        analyze_line = next(line for line in rendered.split("\n") if "ANALYZE" in line)

        # The yellow "…" placeholder is gone; one of the braille spinner
        # frames is on the line instead.
        assert "…" not in analyze_line
        assert any(frame in analyze_line for frame in _SPINNER_FRAMES)

    def test_analyze_mode_renders_upload_then_analyze(self):
        tracker = self._tracker(operation="analyze")

        lines = _stripped_lines(tracker._render().plain)  # noqa: SLF001

        assert lines[0].startswith("UPLOAD")
        assert lines[4].startswith("ANALYZE")
        assert all(
            not line.startswith(("RENDER", "COMPILE", "PLAN", "DEPLOY"))
            for line in lines
        )

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
