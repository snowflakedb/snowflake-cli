import pytest
import yaml
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)


@pytest.fixture
def make_project(temporary_directory):
    def _make_project(entities=None, env=None, mixins=None):
        env = env or {}
        mixins = mixins or {}
        project = dict(
            definition_version=2,
            entities=entities,
            env=env,
            mixins=mixins,
        )
        with open("snowflake.yml", "w") as f:
            yaml.safe_dump(project, f)

    return _make_project


def test_ws_dump_includes_defaults(make_project, runner):
    pkg = dict(
        type="application package",
        manifest="app/manifest.yml",
        artifacts=[dict(src="app/*", dest="./")],
    )
    make_project(entities=dict(pkg=pkg))

    result = runner.invoke(["ws", "dump"])

    assert result.exit_code == 0, result.output
    dumped = yaml.safe_load(result.output)
    assert (
        dumped["entities"]["pkg"]["stage"]
        == ApplicationPackageEntityModel.model_fields["stage"].default
    )


def test_ws_dump_renders_templates(make_project, runner):
    env_foo = "bar"
    pkg = dict(
        type="application package",
        manifest="app/manifest.yml",
        artifacts=[dict(src="app/*", dest="./")],
        stage="schema_name.my_stage_<% ctx.env.FOO %>",
    )
    make_project(entities=dict(pkg=pkg))

    result = runner.invoke(["ws", "dump"], env={"FOO": env_foo})

    assert result.exit_code == 0, result.output
    dumped = yaml.safe_load(result.output)
    assert dumped["entities"]["pkg"]["stage"] == f"schema_name.my_stage_{env_foo}"
