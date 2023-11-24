import re
import codecs

from pathlib import Path
from typing import List, Union

from click import ClickException
from snowcli.cli.nativeapp.artifacts import NotInDeployRootError, resolve_without_follow
from strictyaml import YAML, load

SINGLE_QUOTED_STRING_REGEX = r"'((?:\\.|[^'\n])+?)'"

EXECUTE_IMMEDIATE_FROM_REGEX = re.compile(
    rf"execute\s+immediate\s+from\s+{SINGLE_QUOTED_STRING_REGEX}", re.IGNORECASE
)

# See https://docs.snowflake.com/en/sql-reference/data-types-text#escape-sequences-in-single-quoted-string-constants
# and https://stackoverflow.com/a/24519338 (based on; modified)
SINGLE_QUOTE_ESCAPE_SEQUENCES_RE = re.compile(
    r"""
    ( \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\[\\'"abfnrtv]  # Single-character escapes
    )""",
    re.UNICODE | re.VERBOSE,
)


class ManifestNotFoundError(ClickException):
    """
    The application manifest was not found in the deploy root.
    """

    def __init__(self):
        super().__init__(self.__doc__)


class SetupScriptNotFoundError(ClickException):
    """
    A setup script was referenced but found to be missing in the deploy root.
    """

    def __init__(self, file: Union[Path, str]):
        super().__init__(f"{self.__doc__}\nExpected path: {file}")


def apply_single_quote_escapes(literal: str) -> str:
    """
    Applies the Snowflake single-quoting escape sequences in a given
    "raw" string, returning the resulting (processed) string.
    """

    def decode_match(match):
        return codecs.decode(match.group(0), "unicode-escape")

    return SINGLE_QUOTE_ESCAPE_SEQUENCES_RE.sub(decode_match, literal)


def extract_execute_immediate_relpaths(sqltext: str) -> List[str]:
    return [
        apply_single_quote_escapes(relpath)
        for relpath in EXECUTE_IMMEDIATE_FROM_REGEX.findall(sqltext)
    ]


def load_manifest(deploy_root: Path) -> dict:
    """
    Parses the manifest as found in the deploy root.
    """
    manifest_path = deploy_root / "manifest.yml"
    if not manifest_path.is_file():
        raise ManifestNotFoundError()

    with open(manifest_path, "r") as f:
        return load(f.read()).data


def find_setup_scripts(deploy_root: Path) -> List[Path]:
    """
    Returns a list of paths to all setup scripts (either executed directly by the
    Native Applications Framework via the manifest or indirectly via EXECUTE IMMEDIATE
    FROM) relative to the deploy root.
    """
    manifest = load_manifest(deploy_root)
    base_script: Path = resolve_without_follow(
        deploy_root / manifest["artifacts"]["setup_script"]
    )
    if not base_script.is_file():
        raise SetupScriptNotFoundError(base_script)

    all_scripts: List[Path] = [base_script]
    queued_scripts = list(all_scripts)

    while len(queued_scripts) > 0:
        script = queued_scripts.pop()
        with open(script, "r") as f:
            script_content = f.read()
            for relpath in extract_execute_immediate_relpaths(script_content):
                referenced_script = resolve_without_follow(script.parent / relpath)

                if deploy_root.resolve() not in referenced_script.parents:
                    raise NotInDeployRootError(None, referenced_script, deploy_root)

                if not referenced_script.is_file():
                    raise SetupScriptNotFoundError(referenced_script)

                if not referenced_script in all_scripts:
                    all_scripts.append(referenced_script)
                    queued_scripts.append(referenced_script)

    return [x.relative_to(deploy_root) for x in all_scripts]
