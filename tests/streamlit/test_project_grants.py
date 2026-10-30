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
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
from snowflake.cli._plugins.streamlit.project_grants import add_grants
from snowflake.cli.api.project.schemas.entities.common import Grant
from snowflake.cli.api.secure_path import SecurePath

WITH_GRANTS = dedent(
    """\
    definition_version: 2
    entities:
      my_app:
        type: streamlit
        # keep this comment
        main_file: streamlit_app.py
        grants:
          - privilege: USAGE
            role: ANALYST
    """
)

WITHOUT_GRANTS = dedent(
    """\
    definition_version: 2
    entities:
      my_app:
        type: streamlit
        main_file: streamlit_app.py
      other_app:
        type: streamlit
        main_file: other.py
    """
)


@pytest.fixture
def project_file(tmp_path: Path):
    def _write(content: str) -> SecurePath:
        path = tmp_path / "snowflake.yml"
        path.write_text(content)
        return SecurePath(path)

    return _write


def _usage(user: str, with_grant_option: bool = False) -> Grant:
    return Grant(privilege="USAGE", user=user, with_grant_option=with_grant_option)


def test_appends_to_an_existing_grants_list(project_file):
    path = project_file(WITH_GRANTS)

    assert add_grants(path, "my_app", [_usage("AAMADHAVAN")]) is None
    assert path.path.read_text() == dedent(
        """\
        definition_version: 2
        entities:
          my_app:
            type: streamlit
            # keep this comment
            main_file: streamlit_app.py
            grants:
              - privilege: USAGE
                role: ANALYST
              - privilege: USAGE
                user: AAMADHAVAN
        """
    )


def test_creates_the_grants_list_when_absent(project_file):
    path = project_file(WITHOUT_GRANTS)

    assert add_grants(path, "my_app", [_usage("AAMADHAVAN")]) is None
    assert path.path.read_text() == dedent(
        """\
        definition_version: 2
        entities:
          my_app:
            type: streamlit
            main_file: streamlit_app.py
            grants:
              - privilege: USAGE
                user: AAMADHAVAN
          other_app:
            type: streamlit
            main_file: other.py
        """
    )


def test_records_the_grant_option(project_file):
    path = project_file(WITHOUT_GRANTS)

    add_grants(path, "my_app", [_usage("AAMADHAVAN", with_grant_option=True)])

    assert "with_grant_option: true" in path.path.read_text()


def test_comments_and_formatting_survive(project_file):
    path = project_file(WITH_GRANTS)

    add_grants(path, "my_app", [_usage("AAMADHAVAN")])

    assert "# keep this comment" in path.path.read_text()


def test_an_already_recorded_grant_is_left_alone(project_file):
    path = project_file(WITH_GRANTS)

    assert (
        add_grants(path, "my_app", [Grant(privilege="usage", role="analyst")]) is None
    )
    assert path.path.read_text() == WITH_GRANTS


@pytest.mark.parametrize(
    "content, entity_id, expected_reason",
    [
        (WITHOUT_GRANTS, "missing_app", "has no 'missing_app' entity"),
        # A v1 project file has no 'entities' at all.
        (
            "definition_version: 1\nstreamlit:\n  name: my_app\n",
            "my_app",
            "has no 'my_app' entity",
        ),
        ("definition_version: 2\nentities: [", "my_app", "not readable as YAML"),
    ],
)
def test_unrecognized_files_are_reported_not_edited(
    project_file, content, entity_id, expected_reason
):
    path = project_file(content)

    reason = add_grants(path, entity_id, [_usage("AAMADHAVAN")])

    assert reason is not None and expected_reason in reason
    assert path.path.read_text() == content


def test_a_missing_file_is_reported(tmp_path):
    path = SecurePath(tmp_path / "snowflake.yml")

    assert "does not exist" in str(add_grants(path, "my_app", [_usage("A")]))


@pytest.mark.parametrize(
    "grants_key",
    [
        # A `grants:` key is still that key when something trails it. Taking
        # these for "no grants key" inserted a second one, which PyYAML resolves
        # last-key-wins, silently dropping every grant already recorded.
        "grants: &shares",
        "grants:  # the people this app is shared with",
        "grants: !!seq",
    ],
)
def test_a_trailing_anchor_or_comment_is_still_the_grants_key(project_file, grants_key):
    path = project_file(WITH_GRANTS.replace("grants:", grants_key))

    assert add_grants(path, "my_app", [_usage("AAMADHAVAN")]) is None
    assert path.path.read_text() == dedent(
        f"""\
        definition_version: 2
        entities:
          my_app:
            type: streamlit
            # keep this comment
            main_file: streamlit_app.py
            {grants_key}
              - privilege: USAGE
                role: ANALYST
              - privilege: USAGE
                user: AAMADHAVAN
        """
    )


def test_an_inline_grants_value_is_reported_not_appended_to(project_file):
    content = WITHOUT_GRANTS.replace(
        "    main_file: streamlit_app.py\n",
        "    main_file: streamlit_app.py\n    grants: []\n",
    )
    path = project_file(content)

    reason = add_grants(path, "my_app", [_usage("AAMADHAVAN")])

    assert reason is not None and "could not be edited" in reason
    assert path.path.read_text() == content


def test_only_a_direct_child_of_entities_is_the_entity(project_file):
    # `my_app` also names a key nested under another entity. Matching that one
    # would record the share against `other_app`.
    path = project_file(
        dedent(
            """\
            definition_version: 2
            entities:
              other_app:
                type: streamlit
                main_file: other.py
                imports:
                  my_app:
                    stage: other_stage
              my_app:
                type: streamlit
                main_file: streamlit_app.py
            """
        )
    )

    assert add_grants(path, "my_app", [_usage("AAMADHAVAN")]) is None
    assert path.path.read_text() == dedent(
        """\
        definition_version: 2
        entities:
          other_app:
            type: streamlit
            main_file: other.py
            imports:
              my_app:
                stage: other_stage
          my_app:
            type: streamlit
            main_file: streamlit_app.py
            grants:
              - privilege: USAGE
                user: AAMADHAVAN
        """
    )


def test_the_grant_option_is_recorded_over_a_plain_entry(project_file):
    # The share was issued WITH GRANT OPTION, so a plain entry does not record
    # it: leaving the file alone would have the next deploy re-apply plain USAGE.
    path = project_file(
        WITH_GRANTS.replace("    role: ANALYST\n", "    user: AAMADHAVAN\n")
    )

    assert (
        add_grants(path, "my_app", [_usage("AAMADHAVAN", with_grant_option=True)])
        is None
    )
    assert path.path.read_text().endswith(
        "    grants:\n"
        "      - privilege: USAGE\n"
        "        user: AAMADHAVAN\n"
        "      - privilege: USAGE\n"
        "        user: AAMADHAVAN\n"
        "        with_grant_option: true\n"
    )


def test_a_plain_share_does_not_weaken_a_recorded_grant_option(project_file):
    # The other direction: the file already records the stronger grant, so a
    # plain re-share adds nothing.
    content = WITH_GRANTS.replace(
        "    role: ANALYST\n",
        "    user: AAMADHAVAN\n    with_grant_option: true\n",
    )
    path = project_file(content)

    assert add_grants(path, "my_app", [_usage("AAMADHAVAN")]) is None
    assert path.path.read_text() == content


def test_a_trailing_comment_on_entities_is_still_the_entities_key(project_file):
    path = project_file(WITHOUT_GRANTS.replace("entities:", "entities:  # the apps"))

    assert add_grants(path, "my_app", [_usage("AAMADHAVAN")]) is None
    assert "        user: AAMADHAVAN\n" in path.path.read_text()
