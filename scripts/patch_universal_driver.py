"""Patch pyproject.toml to use the Snowflake universal driver instead of snowflake-connector-python.

Usage:
    python scripts/patch_universal_driver.py <git-ref>

Removes snowflake-connector-python from [project].dependencies and injects
a pip install of the universal driver into [tool.hatch.envs.ud] pre-install-commands.
Also patches test plugin pyproject.toml files to remove the snowflake-cli dependency,
preventing pip from re-resolving the CLI's dependency tree during plugin installation.
"""

import glob
import re
import sys


def patch_pyproject(path="pyproject.toml", *, ud_ref: str):
    with open(path) as f:
        content = f.read()

    # 1. Remove snowflake-connector-python from [project].dependencies
    original = content
    content = re.sub(
        r'^\s*["\']snowflake-connector-python[^"\']*["\'],?\n',
        "",
        content,
        flags=re.MULTILINE,
    )
    if content == original:
        print("WARNING: snowflake-connector-python not found in dependencies")

    # 2. Add UD pip install to [tool.hatch.envs.ud] pre-install-commands
    ud_pip = f'pip install "git+https://github.com/snowflakedb/universal-driver@{ud_ref}#subdirectory=python"'
    content = content.replace(
        "[tool.hatch.envs.ud]\n"
        'template = "ud"\n'
        "pre-install-commands = [\n"
        '  "pip install pytest-xdist",\n'
        "]",
        "[tool.hatch.envs.ud]\n"
        'template = "ud"\n'
        "pre-install-commands = [\n"
        '  "pip install pytest-xdist",\n'
        f"  '{ud_pip}',\n"
        "]",
    )

    # 3. Simplify [tool.hatch.envs.ud.scripts] check — keep only the pytest command.
    #    The old script uninstalls connector and installs UD, but that's now handled
    #    by pre-install-commands (and uninstalling would remove UD itself).
    content = re.sub(
        r"(\[tool\.hatch\.envs\.ud\.scripts\]\ncheck = \[)\n"
        r".*?"
        r"\n(\s*'pytest )",
        r"\1\n\2",
        content,
        flags=re.DOTALL,
    )

    with open(path, "w") as f:
        f.write(content)


def patch_plugins(plugin_dir="test_external_plugins"):
    """Remove snowflake-cli dependency from test plugin packages.

    This prevents pip from re-resolving the CLI's (and transitively the
    connector's) dependency tree when plugins are installed during tests.
    The CLI is already present in the hatch environment.
    """
    patched = []
    for path in glob.glob(f"{plugin_dir}/*/pyproject.toml"):
        with open(path) as f:
            content = f.read()
        original = content
        content = re.sub(
            r'dependencies\s*=\s*\[\s*\n\s*"snowflake-cli[^"]*"\s*\n\s*\]',
            "dependencies = []",
            content,
        )
        if content != original:
            with open(path, "w") as f:
                f.write(content)
            patched.append(path)
    if patched:
        print(f"Patched {len(patched)} plugin(s): {', '.join(patched)}")
    else:
        print("WARNING: No plugin pyproject.toml files were patched")


def show_results(path="pyproject.toml"):
    with open(path) as f:
        content = f.read()

    print("=== [project].dependencies (filtered) ===")
    in_deps = False
    for line in content.splitlines():
        if line.strip().startswith("dependencies") and "[" in line:
            in_deps = True
        if in_deps:
            print(line)
        if in_deps and line.strip().endswith("]"):
            in_deps = False
            break

    print("\n=== [tool.hatch.envs.ud] section ===")
    in_ud = False
    for line in content.splitlines():
        if "[tool.hatch.envs.ud]" in line:
            in_ud = True
        elif in_ud and line.startswith("[") and "ud" not in line:
            break
        if in_ud:
            print(line)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <universal-driver-git-ref>", file=sys.stderr)
        sys.exit(1)

    ref = sys.argv[1]
    patch_pyproject(ud_ref=ref)
    patch_plugins()
    show_results()
