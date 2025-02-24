import typing as t
from textwrap import dedent

import pytest
from snowflake.cli.api.secure_path import SecurePath

from tests.sql.sql_types import SqlFiles


@pytest.fixture(name="no_command_files")
def make_no_commands_files(tmp_path_factory) -> t.Generator[SqlFiles, None, None]:
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


@pytest.fixture(name="nested_includes")
def make_nested_includes(tmp_path_factory) -> t.Generator[SqlFiles, None, None]:
    """f1 includes f2. f2 includes f3."""
    tdf = tmp_path_factory.mktemp("nested_includes")

    fh1 = tdf / "f1.sql"
    fh2 = tdf / "f2.sql"
    fh3 = tdf / "f3.sql"

    fh1.write_text(
        dedent(
            f"""
            f1: select 1;
            !source {fh2.as_posix()};
            f1: select 2;
        """
        )
    )
    fh2.write_text(
        dedent(
            f"""
            f2: select 1;
            !source {fh3.as_posix()};
            f2: select 2;
        """
        )
    )
    fh3.write_text(
        dedent(
            """
            f3: select 1;
            f3: select 2;
        """
        )
    )

    yield (fh1,)


@pytest.fixture(name="recursive_nested_includes")
def make_recursive_nested_includes(
    tmp_path_factory,
) -> t.Generator[SqlFiles, None, None]:
    """f1 includes f2. f2 includes f3. f3 includes f1."""
    tdf = tmp_path_factory.mktemp("recurive_nested_includes")

    fh1 = tdf / "f1.sql"
    fh2 = tdf / "f2.sql"
    fh3 = tdf / "f3.sql"

    fh1.write_text(
        dedent(
            f"""
            f1: select 1;
            !source {fh2.as_posix()};
            f1: select 2;
        """
        )
    )
    fh2.write_text(
        dedent(
            f"""
            f2: select 1;
            !source {fh3.as_posix()};
            f2: select 2;
        """
        )
    )
    fh3.write_text(
        dedent(
            f"""
            f3: select 1;
            !source {fh1.as_posix()};
            f3: select 2;
        """
        )
    )

    yield (fh1,)


# To be
@pytest.fixture(name="recursive_source_includes")
def make_recursive_source_includes(
    tmp_path_factory,
) -> t.Generator[SecurePath, None, None]:
    f1 = tmp_path_factory.mktemp("data") / ("f1.txt")
    f2 = tmp_path_factory.mktemp("data") / ("f2.txt")
    f3 = tmp_path_factory.mktemp("data") / ("f3.txt")

    f1.write_text(f"1\n!source {f2}")
    f2.write_text(f"2\n!source {f3}")
    f3.write_text(f"3\n!source {f1}")

    yield SecurePath(f1)


@pytest.fixture(name="single_select_1_file")
def make_single_select_1_file(tmp_path_factory):
    fh = tmp_path_factory.mktemp("data") / "single_select_1.sql"
    fh.write_text("select 1;")
    yield fh
