from textwrap import dedent

import pytest
from snowflake.cli.api.project.definition import load_project


@pytest.mark.parametrize(
    "override, expected",
    [("", ["A", "B", "entity_value"]), ("!override", ["entity_value"])],
)
def test_override_works_for_sequences(named_temporary_file, override, expected):
    text = f"""\
    definition_version: "2"
    mixins:
      mixin_a:
        external_access_integrations:
          - A
          - B
    entities:
      my_function:
       type: "function"
       stage: foo_stage
       returns: string
       handler: foo.baz
       signature: ""
       artifacts: []
       external_access_integrations: {override}
         - entity_value 
       meta:
        use_mixins: ["mixin_a"]
    """

    with named_temporary_file(suffix=".yml") as p:
        p.write_text(dedent(text))
        result = load_project([p])

    pd = result.project_definition
    assert pd.entities["my_function"].external_access_integrations == expected


@pytest.mark.parametrize(
    "override, expected",
    [("", {"A": "a", "B": "b", "entity": "value"}), ("!override", {"entity": "value"})],
)
def test_override_works_for_mapping(named_temporary_file, override, expected):
    text = f"""\
    definition_version: "2"
    mixins:
      mixin_a:
        secrets:
          A: a
      mixin_b:
        secrets:
          B: b
    entities:
      my_function:
       type: "function"
       stage: foo_stage
       returns: string
       handler: foo.baz
       signature: ""
       artifacts: []
       secrets: {override}
         entity: value 
       meta:
        use_mixins: ["mixin_a", "mixin_b"]
    """

    with named_temporary_file(suffix=".yml") as p:
        p.write_text(dedent(text))
        result = load_project([p])

    pd = result.project_definition
    assert pd.entities["my_function"].secrets == expected
