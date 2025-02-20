import typing as t
from textwrap import dedent

import pytest

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
            f1: line 1;
            f1: line 2;
        """
        )
    )
    fh2.write_text(
        dedent(
            """
            f2: line 1;
            f2: line 2;
        """
        )
    )

    yield (fh1.as_posix(), fh2.as_posix())


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
            f1: line 1;
            !source {fh2.as_posix()};
            f1: line 2;
        """
        )
    )
    fh2.write_text(
        dedent(
            f"""
            f2: line 1;
            !source {fh3.as_posix()};
            f2: line 2;
        """
        )
    )
    fh3.write_text(
        dedent(
            """
            f3: line 1;
            f3: line 2;
        """
        )
    )

    yield (fh1.as_posix(),)


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
            f1: line 1;
            !source {fh2.as_posix()};
            f1: line 2;
        """
        )
    )
    fh2.write_text(
        dedent(
            f"""
            f2: line 1;
            !source {fh3.as_posix()};
            f2: line 2;
        """
        )
    )
    fh3.write_text(
        dedent(
            f"""
            f3: line 1;
            !source {fh1.as_posix()};
            f3: line 2;
        """
        )
    )

    yield (fh1.as_posix(),)
