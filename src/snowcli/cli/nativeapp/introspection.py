import re
from strictyaml import load, YAML
from typing import List
from pathlib import Path
from click import ClickException

from snowcli.cli.nativeapp.artifacts import resolve_without_follow

EXECUTE_IMMEDIATE_FROM_REGEX = re.compile(
    r"execute\s+immediate\s+from\s+'([^']+?)'", re.IGNORECASE
)


class ManifestNotFoundError(ClickException):
    """
    The application manifest was not found in the deploy root.
    """

    def __init__(self):
        super().__init__(self.__doc__.strip())


class SetupScriptNotFoundError(ClickException):
    """
    A setup script was referenced but found to be missing in the deploy root.
    """

    def __init__(self, file: Path | str):
        super().__init__(f"{self.__doc__.strip()}\nExpected path: {file}")


def load_manifest(deploy_root: Path) -> dict:
    """
    Parses the manifest as found in the deploy root.
    """
    with open(deploy_root / "manifest.yml", "r") as f:
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
            for relpath in EXECUTE_IMMEDIATE_FROM_REGEX.findall(script_content):
                referenced_script = resolve_without_follow(script.parent / relpath)

                if not referenced_script.is_file():
                    raise SetupScriptNotFoundError(referenced_script)

                if not referenced_script in all_scripts:
                    all_scripts.append(referenced_script)
                    queued_scripts.append(referenced_script)

    return [x.relative_to(deploy_root) for x in all_scripts]
