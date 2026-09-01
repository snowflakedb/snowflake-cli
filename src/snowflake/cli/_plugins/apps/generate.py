# Copyright (c) 2026 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, Optional, cast


def _yaml_str(v: str) -> str:
    # YAML treats bare double quotes as string delimiters and strips them on
    # round-trip, turning '"lower_db"' into 'lower_db' (then uppercased by
    # Snowflake).  Wrapping in YAML single quotes preserves embedded double
    # quotes as literal data.  Single quotes inside the value are escaped by
    # doubling them, per the YAML 1.1 single-quoted scalar spec.
    if '"' in v:
        return "'" + v.replace("'", "''") + "'"
    return v


def _generate_app_yml(
    app_id: str,
    resolved: Dict[str, Optional[str]],
    *,
    use_workspace: bool,
) -> str:
    """Generate ``app.yml`` (Snowflake App Runtime v2) content.

    Emits the flat ``app.yml`` v2 manifest that the ``snow app`` commands read
    instead of ``snowflake.yml``. Required keys: ``database``, ``schema``,
    ``warehouse``. Optional key: ``build_eai`` (omitted when ``None``).

    The manifest carries a single top-level *baseline* (no ``targets``), so
    ``snow app deploy`` deploys it directly. ``package_name`` and
    ``artifact_repo`` are omitted; they default to ``name`` and ``<name>_repo``
    at deploy time. Code storage (``code_workspace`` / ``code_stage``) is also
    omitted: ``snow app deploy`` picks the backend at deploy time — a workspace
    when the destination is a personal database or the role can create one,
    otherwise a ``<name>_CODE`` stage — so the manifest stays minimal and the
    decision follows the role's actual privileges. The builder ``install`` /
    ``build`` / ``run`` phases are likewise omitted; they default to the Node
    conventions at deploy time.

    ``use_workspace`` is accepted but unused — the code-storage backend is
    resolved at deploy time rather than baked into ``app.yml``.
    """
    database = cast(str, resolved["database"])
    schema = cast(str, resolved["schema"])
    warehouse = cast(str, resolved["warehouse"])
    build_eai = resolved.get("build_eai")

    lines = [
        "version: 2",
        "",
        f"name: {app_id.upper()}",
        f"database: {_yaml_str(database)}",
        f"schema: {_yaml_str(schema)}",
        f"query_warehouse: {_yaml_str(warehouse)}",
    ]
    if build_eai:
        lines.append(f"build_eai: {_yaml_str(build_eai)}")
    lines += [
        "",
        "ignore:",
        "  - node_modules",
        "  - .env*",
        "  - __pycache__",
        '  - "*.pyc"',
        "  - .next",
        "  - .git",
        "  - snowflake.log",
    ]
    return "\n".join(lines) + "\n"
