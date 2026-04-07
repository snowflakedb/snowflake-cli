from __future__ import annotations

import fnmatch
import os
import shutil
from pathlib import Path
from typing import List


def load_dockerignore_patterns(build_context_dir: Path) -> List[str]:
    """Load patterns from .dockerignore if it exists. Returns a list of glob patterns."""
    dockerignore_path = build_context_dir / ".dockerignore"
    if not dockerignore_path.is_file():
        return []
    patterns = []
    for line in dockerignore_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns


def is_ignored(rel_path: str, patterns: List[str]) -> bool:
    """Check if a relative path matches any of the ignore patterns.

    Matches against both the full relative path and individual path components,
    similar to how .dockerignore works.
    """
    rel_path_posix = rel_path.replace(os.sep, "/")
    for pattern in patterns:
        if fnmatch.fnmatch(rel_path_posix, pattern):
            return True
        if fnmatch.fnmatch(rel_path_posix, pattern + "/**"):
            return True
        for component in Path(rel_path_posix).parts:
            if fnmatch.fnmatch(component, pattern):
                return True
    return False


def copy_filtered_build_context(
    build_context_dir: Path, dest_dir: Path, patterns: List[str]
) -> None:
    """Copy build context to dest_dir, excluding files matching ignore patterns.

    The .dockerignore file itself is always included so BuildKit can use it.
    """
    for root, dirs, files in os.walk(build_context_dir):
        rel_root = os.path.relpath(root, build_context_dir)

        if rel_root != "." and is_ignored(rel_root, patterns):
            dirs.clear()
            continue

        dirs[:] = [
            d
            for d in dirs
            if not is_ignored(
                os.path.join(rel_root, d) if rel_root != "." else d, patterns
            )
        ]

        for fname in files:
            rel_file = os.path.join(rel_root, fname) if rel_root != "." else fname
            if fname == ".dockerignore" or not is_ignored(rel_file, patterns):
                dest_file = dest_dir / rel_file
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(os.path.join(root, fname), dest_file)
