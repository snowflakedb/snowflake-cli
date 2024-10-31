import pathlib as p

import tomlkit as tk
from snowflake.cli.__about__ import VERSION

PROJECT_TOML_FILE = p.Path(__file__).parent.joinpath(
    "snowflake-cli-labs",
    "pyproject.toml",
)


def sync_dependecies_version(toml_file: p.Path = PROJECT_TOML_FILE):
    project_toml = tk.loads(toml_file.read_text())

    dependencies = project_toml.get("project", {}).get("dependencies", [])
    updated_dependencies = [f"snowflake-cli=={VERSION}"]
    print(f"Updating `{dependencies}` to `{updated_dependencies}`")

    dependencies.clear()
    dependencies.extend(updated_dependencies)

    toml_file.write_text(tk.dumps(project_toml))


if __name__ == "__main__":
    sync_dependecies_version()
