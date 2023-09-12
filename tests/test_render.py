import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from tests.testing_utils.fixtures import *


def test_render_template(runner):
    with NamedTemporaryFile("r") as tmp_file, NamedTemporaryFile("r") as json_file:
        Path(tmp_file.name).write_text(
            "This is my template {{ data.name }} and {{ toBeUpdated }}"
        )
        Path(json_file.name).write_text(
            json.dumps(
                {"data": {"name": "value"}, "toBeUpdated": "should not be in output"}
            )
        )
        result = runner.invoke(
            [
                "render",
                "template",
                tmp_file.name,
                "-d",
                json_file.name,
                "-D",
                "toBeUpdated=ok",
            ]
        )

    assert result.exit_code == 0
    assert result.stdout_bytes.decode() == "This is my template value and ok\n"


def test_render_js_proc(runner):
    with NamedTemporaryFile("r") as tmp_file, NamedTemporaryFile("r") as js_file:
        Path(tmp_file.name).write_text(
            f"CREATE PROCEDURE \n{{{{ '{js_file.name}' | procedure_from_js_file }}}}"
        )
        Path(js_file.name).write_text("function foo() {};\nmodule.exports = foo;")
        result = runner.invoke(["render", "template", tmp_file.name])

    assert result.exit_code == 0
    assert (
        result.stdout_bytes.decode()
        == """\
CREATE PROCEDURE 
var module = {};
var exports = {};
module.exports = exports;
(function() {
function foo() {};
module.exports = foo;
})()
return module.exports.apply(this, arguments);

"""
    )


def test_render_include_file_content(runner):
    with NamedTemporaryFile("r") as tmp_file, NamedTemporaryFile("r") as file:
        Path(tmp_file.name).write_text(f"{{{{ '{file.name}' | read_file_content }}}}")
        Path(file.name).write_text("CONTENT")
        result = runner.invoke(["render", "template", tmp_file.name])

    assert result.exit_code == 0
    assert result.stdout_bytes.decode() == "CONTENT\n"


def test_render_metadata(runner):
    with NamedTemporaryFile("r") as tmp_file, NamedTemporaryFile("r") as meta:
        Path(tmp_file.name).write_text(f"{{{{ '{meta.name}' | render_metadata }}}}")
        Path(meta.name).write_text(
            json.dumps(
                {
                    "procedures": [
                        {
                            "name": "APP.PYTHON_HELLO",
                            "signature": [
                                {"name": "arg1", "type": "STRING"},
                            ],
                            "returns": "STRING",
                            "language": "PYTHON",
                            "runtime_version": "3.8",
                            "packages": "snowflake-snowpark_containers_cmds-python",
                            "imports": [
                                "/module.zip",
                            ],
                            "handler": "module.procedures.python_hello",
                            "grants": [{"role": "APP_ROLE", "grant": "USAGE"}],
                        }
                    ]
                }
            )
        )
        result = runner.invoke(["render", "template", tmp_file.name])

    assert result.exit_code == 0
    assert (
        result.stdout_bytes.decode()
        == """\
CREATE OR REPLACE PROCEDURE APP.PYTHON_HELLO(    
ARG1 STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark_containers_cmds-python')
IMPORTS = ('/module.zip')
HANDLER = 'module.procedures.python_hello'
;
GRANT USAGE ON PROCEDURE APP.PYTHON_HELLO(STRING)
TO DATABASE ROLE APP_ROLE;

"""
    )
