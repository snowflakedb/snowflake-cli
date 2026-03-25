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
from snowflake.cli.api.project.schemas.scripts import ScriptModel


class TestScriptModel:
    def test_script_with_cmd_only(self):
        script = ScriptModel(cmd="echo hello")
        assert script.cmd == "echo hello"
        assert script.run is None
        assert script.shell is False

    def test_script_with_run_only(self):
        script = ScriptModel(run=["build", "deploy"])
        assert script.cmd is None
        assert script.run == ["build", "deploy"]

    def test_script_with_description(self):
        script = ScriptModel(cmd="echo hello", description="Say hello")
        assert script.description == "Say hello"

    def test_script_with_shell_true(self):
        script = ScriptModel(cmd="echo hello | cat", shell=True)
        assert script.shell is True

    def test_script_with_cwd(self):
        script = ScriptModel(cmd="npm install", cwd="frontend")
        assert script.cwd == "frontend"

    def test_script_with_env(self):
        script = ScriptModel(cmd="node app.js", env={"NODE_ENV": "production"})
        assert script.env == {"NODE_ENV": "production"}

    def test_script_must_have_cmd_or_run(self):
        with pytest.raises(ValueError) as exc_info:
            ScriptModel(description="Missing cmd or run")
        assert "must have either 'cmd' or 'run'" in str(exc_info.value)

    def test_script_cannot_have_both_cmd_and_run(self):
        with pytest.raises(ValueError) as exc_info:
            ScriptModel(cmd="echo hello", run=["build"])
        assert "cannot have both 'cmd' and 'run'" in str(exc_info.value)

    def test_script_all_fields(self):
        script = ScriptModel(
            cmd="npm run build",
            description="Build the frontend",
            shell=True,
            cwd="frontend",
            env={"NODE_ENV": "production", "CI": "true"},
        )
        assert script.cmd == "npm run build"
        assert script.description == "Build the frontend"
        assert script.shell is True
        assert script.cwd == "frontend"
        assert script.env == {"NODE_ENV": "production", "CI": "true"}
