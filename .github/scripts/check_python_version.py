#!/usr/bin/env python3
"""
Compare the Python version pinned in scripts/packaging/build_python.sh
against the latest published patch in that major.minor series on python.org.
"""

import os
import re
import sys

import requests

BUILD_SCRIPT_PATH = os.environ.get(
    "BUILD_SCRIPT_PATH", "scripts/packaging/build_python.sh"
)
PYTHON_RELEASES_API = (
    "https://www.python.org/api/v2/downloads/release/"
    "?is_published=true&pre_release=false"
)

VERSION_RE = re.compile(r"^PYTHON_VERSION=(\d+)\.(\d+)\.(\d+)\s*$", re.MULTILINE)
RELEASE_NAME_RE = re.compile(r"^Python (\d+)\.(\d+)\.(\d+)$")


def read_pinned_version(build_script_path: str) -> tuple[int, int, int]:
    """Extract the (major, minor, patch) PYTHON_VERSION pinned in the build script."""
    with open(build_script_path, "r") as f:
        contents = f.read()

    match = VERSION_RE.search(contents)
    if not match:
        print(f"❌ Could not find PYTHON_VERSION in {build_script_path}")
        sys.exit(1)

    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def latest_patch_for_series(major: int, minor: int) -> int:
    """Query python.org for the highest published patch in the major.minor series."""
    try:
        response = requests.get(PYTHON_RELEASES_API, timeout=30)
        response.raise_for_status()
        releases = response.json()
    except requests.RequestException as e:
        print(f"❌ Failed to query python.org for releases: {e}")
        sys.exit(1)

    prefix = f"Python {major}.{minor}."
    best_patch = None
    for release in releases:
        name = release.get("name", "")
        if not name.startswith(prefix):
            continue
        match = RELEASE_NAME_RE.match(name)
        if not match:
            continue
        patch = int(match.group(3))
        if best_patch is None or patch > best_patch:
            best_patch = patch

    if best_patch is None:
        print(f"❌ No published releases found for series {major}.{minor} on python.org")
        sys.exit(1)

    return best_patch


def main():
    major, minor, current_patch = read_pinned_version(BUILD_SCRIPT_PATH)
    current_version = f"{major}.{minor}.{current_patch}"
    print(f"📌 Pinned version in {BUILD_SCRIPT_PATH}: {current_version}")

    print(f"🔍 Checking python.org for the latest {major}.{minor}.x release...")
    latest_patch = latest_patch_for_series(major, minor)
    latest_version = f"{major}.{minor}.{latest_patch}"
    print(f"📦 Latest published release: {latest_version}")

    update_available = latest_patch > current_patch
    if update_available:
        print(f"🚨 A newer patch is available: {current_version} -> {latest_version}")
    else:
        print("✅ Pinned version is already the latest published patch")

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"current_version={current_version}\n")
            f.write(f"latest_version={latest_version}\n")
            f.write(f"update_available={'true' if update_available else 'false'}\n")


if __name__ == "__main__":
    main()
