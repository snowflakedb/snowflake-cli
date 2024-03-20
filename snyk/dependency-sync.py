from pathlib import Path

import tomlkit


def sync():
    pyproject = tomlkit.loads(Path("pyproject.toml").read_text())
    dependencies = pyproject["project"]["dependencies"]
    dev_dependencies = pyproject["project"]["optional-dependencies"]["development"]
    with open("snyk/requirements.txt", "w") as req:
        for dep in dependencies:
            req.write(f"{dep}\n")
        for dep in dev_dependencies:
            req.write(f"{dep}\n")


if __name__ == "__main__":
    sync()
