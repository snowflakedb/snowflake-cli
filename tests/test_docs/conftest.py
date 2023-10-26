import os
import pytest
import subprocess

from sybil import Sybil
from sybil.parsers.codeblock import CodeBlockParser
from tests.testing_utils.fixtures import temp_dir
from textwrap import dedent


def setup(temp_dir):
    os.chdir(temp_dir)


class SnowCLIDocParser(CodeBlockParser):
    language = "bash"
    pytestmark = pytest.mark.doctest
    # TODO: this still doesn`t seem to work. Check if we can

    def evaluate(self, example):
        values = dedent(example.parsed).strip().split("\n")

        if len(values) >= 2 and values[0].startswith("$ snow"):
            command = values[0].split(" ")
            result = subprocess.run(command[1:], capture_output=True)
            assert result.returncode == 0
            assert values[1] in str(result.stdout)
        assert True, "Not a snowcli command"


pytest_collect_file = Sybil(
    parsers=[SnowCLIDocParser()], patterns=["*.rst"], fixtures=["temp_dir"]
).pytest()
