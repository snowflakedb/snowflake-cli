import typing as t
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.models import ProjectEnvironment


@pytest.fixture(name="cli_context_for_sql_compilation")
def make_cli_context():
    with mock.patch(
        "snowflake.cli.api.rendering.sql_templates.get_cli_context"
    ) as cli_context:
        cli_context().template_context = {
            "ctx": {"env": ProjectEnvironment(default_env={}, override_env={})}
        }
        yield cli_context()


@pytest.fixture(name="no_command_files")
def make_no_commands_files(
    tmp_path_factory,
) -> t.Generator[tuple[Path, Path], None, None]:
    """f1 does not include any other files. f2 does not include any other files."""
    tdf = tmp_path_factory.mktemp("no_commands")

    fh1 = tdf / "f1.sql"
    fh2 = tdf / "f2.sql"

    fh1.write_text(
        dedent(
            """
            f1: select 1;
            f1: select 2;
        """
        )
    )
    fh2.write_text(
        dedent(
            """
            f2: select 1;
            f2: select 2;
        """
        )
    )

    yield (fh1, fh2)


@pytest.fixture(name="recursive_source_includes")
def make_recursive_source_includes(
    tmp_path_factory,
) -> t.Generator[SecurePath, None, None]:
    """f1 includes f2. f2 includes f3. f3 includes f1."""
    f1 = tmp_path_factory.mktemp("data") / ("f1.txt")
    f2 = tmp_path_factory.mktemp("data") / ("f2.txt")
    f3 = tmp_path_factory.mktemp("data") / ("f3.txt")

    f1.write_text(f"1; !source {f2};")
    f2.write_text(f"2; !source {f3};")
    f3.write_text(f"3; !source {f1}; FINAL;")

    yield SecurePath(f1)


@pytest.fixture(name="no_recursion_includes")
def make_no_recursion_includes(tmp_path_factory):
    """f1 includes f2."""
    f1 = tmp_path_factory.mktemp("data") / ("f1.txt")
    f2 = tmp_path_factory.mktemp("data") / ("f2.txt")

    f1.write_text(f"select 1; !source {f2}; FINAL;")
    f2.write_text(f"select 2;")

    yield (f1,)


@pytest.fixture(name="single_select_1_file")
def make_single_select_1_file(tmp_path_factory) -> t.Generator[Path, None, None]:
    fh = tmp_path_factory.mktemp("data") / "single_select_1.sql"
    fh.write_text("select 1;")
    yield fh
