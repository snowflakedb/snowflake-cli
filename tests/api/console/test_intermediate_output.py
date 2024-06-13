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


@pytest.mark.usefixtures("faker_app")
def test_phase_output(runner):
    result = runner.invoke(("Faker",))
    assert result.exit_code == 0, result.output
    assert "Enter" in result.output, result.output
    assert "Exit" in result.output, result.output


@pytest.mark.usefixtures("faker_app")
def test_phase_output_muted(runner):
    result = runner.invoke(("Faker", "--silent"))
    assert result.exit_code == 0, result.output
    assert "Enter" not in result.output, result.output
    assert "Exit" not in result.output, result.output


@pytest.mark.usefixtures("faker_app")
def test_step_output(runner):
    result = runner.invoke(("Faker",))
    assert result.exit_code == 0, result.output
    assert "Teeny Tiny step: UNO UNO" in result.output, result.output


@pytest.mark.usefixtures("faker_app")
def test_step_output_muted(runner):
    result = runner.invoke(("Faker", "--silent"))
    assert result.exit_code == 0, result.output
    assert "Teeny Tiny step: UNO UNO" not in result.output, result.output
