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

from tests_e2e.conftest import _clean_output


def test_clean_output_normalizes_square_box_corners():
    """Windows Rich help uses ┌/└; snapshots use ASCII + after normalization."""
    raw = (
        "┌- Options ----------------------------------------------------------------┐\n"
        "│ --help  -h            Show this message and exit.                         │\n"
        "└----------------------------------------------------------------------------──┘"
    )
    assert _clean_output(raw) == (
        "+- Options ----------------------------------------------------------------+\n"
        "| --help  -h            Show this message and exit.                         |\n"
        "+------------------------------------------------------------------------------+"
    )


def test_clean_output_normalizes_rounded_box_corners():
    raw = (
        "╭- Options ----------------------------------------------------------------╮\n"
        "│ --help  -h            Show this message and exit.                         │\n"
        "╰------------------------------------------------------------------------------╯"
    )
    assert _clean_output(raw) == (
        "+- Options ----------------------------------------------------------------+\n"
        "| --help  -h            Show this message and exit.                         |\n"
        "+------------------------------------------------------------------------------+"
    )


def test_clean_output_none():
    assert _clean_output(None) is None
