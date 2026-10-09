import json
import logging
from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional

from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.nativeapp.artifacts import build_bundle
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli._plugins.streamlit.manager import StreamlitManager
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    SPCS_RUNTIME_V2_NAME,
    WAREHOUSE_RUNTIME_NAME,
    StreamlitEntityModel,
    is_spcs_container_runtime,
)
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.constants import DEFAULT_ENV_FILE, DEFAULT_PAGES_DIR
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.entities.utils import EntityActions, sync_deploy_root_with_stage
from snowflake.cli.api.errno import DOES_NOT_EXIST_OR_NOT_AUTHORIZED
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.project_paths import bundle_root
from snowflake.cli.api.project.schemas.entities.common import Identifier, PathMapping
from snowflake.cli.api.project.util import (
    to_identifier,
    to_string_literal,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

log = logging.getLogger(__name__)

# Snowflake errno / SQLSTATE for "live version already exists" (same codes as
# SnowflakeAppManager.ensure_workspace_live_version).
_LIVE_VERSION_EXISTS_ERRNO = 99106
_RESTART_STREAMLIT_FUNCTION = "SYSTEM$RESTART_STREAMLIT"


def _is_live_version_already_exists_error(exc: ProgrammingError) -> bool:
    """Return True when ADD LIVE VERSION failed because a live version exists.

    Matches errno/SQLSTATE first, with a message-text fallback for older
    connectors that omit errno.
    """
    error_text = str(exc)
    if getattr(exc, "errno", None) == _LIVE_VERSION_EXISTS_ERRNO or (
        "099106" in error_text and "42710" in error_text
    ):
        return True
    return "There is already a live version" in error_text


def _describe_row_is_spcs_v2(current: Dict[str, Any]) -> bool:
    """True when DESCRIBE says the live object is an SPCS container runtime.

    Uses the live row, not snowflake.yml: a content-only project file can omit
    ``runtime_name`` while the object already runs on
    ``SYSTEM$ST_CONTAINER_RUNTIME_*``. Matches the whole container-runtime
    family, so later Python runtimes still restart. Warehouse runtimes copy source per viewer and do not keep a
    process-global ``ScriptCache``, so they do not need a restart.
    """
    return is_spcs_container_runtime(current.get("runtime_name"))


class _TagRef(NamedTuple):
    name: str
    fqn: str


def _main_file_covered_by_artifacts(
    main_file: str, artifacts: list[PathMapping], project_root: Path
) -> bool:
    """Return True if main_file would already be deployed by an existing artifact.

    Coverage means main_file equals an artifact's src, or is a descendant of
    an artifact's src directory and the artifact's destination matches its
    source (so the directory walk lands main_file at the same canonical path
    that the auto-insert would). Artifacts with a different ``dest`` deploy
    main_file to a different location, so the auto-insert is still needed.
    """
    return _path_covered_by_artifacts(main_file, artifacts, project_root)


def _path_covered_by_artifacts(
    target: str, artifacts: list[PathMapping], project_root: Path
) -> bool:
    """True if auto-inserting ``target`` would be a no-op given ``artifacts``."""
    target_path = Path(target)
    return any(
        _artifact_covers_path(artifact, target_path, project_root)
        for artifact in artifacts
    )


def _artifact_covers_path(
    artifact: PathMapping, target_path: Path, project_root: Path
) -> bool:
    """True if target_path would land at the same deploy path via this artifact
    as it would via a standalone auto-insert.

    Directory and exact-file srcs use ``relative_to``. Glob-style srcs
    (e.g. ``pages/*.py``) are expanded with ``project_root.glob``, the same
    way :class:`BundleMap` expands them. :meth:`Path.match` is not enough: it
    is right-anchored, so ``src/app.py`` "matches" ``*.py`` even though the
    bundle never picks it up. Dest-level collisions that still slip through
    are handled by :class:`_ArtifactPathMap`.
    """
    if not artifact.src:
        return False

    if _src_is_glob(artifact.src):
        return _glob_artifact_covers_path(artifact, target_path, project_root)

    src_path = Path(artifact.src)
    try:
        relative_target = target_path.relative_to(src_path)
    except ValueError:
        return False

    return _artifact_dest_root(artifact, src_path) / relative_target == target_path


def _src_is_glob(src: str) -> bool:
    return any(ch in src for ch in "*?[]")


def _glob_artifact_covers_path(
    artifact: PathMapping, target_path: Path, project_root: Path
) -> bool:
    if (project_root / target_path) not in project_root.glob(artifact.src):
        return False
    if not artifact.dest:
        return True
    # Mirrors BundleMap._add_mapping: a trailing "/" maps each match into
    # dest by name; otherwise the match is copied to dest itself.
    dest_path = Path(artifact.dest.rstrip("/"))
    if artifact.dest.endswith("/"):
        return dest_path / target_path.name == target_path
    return dest_path == target_path


def _artifact_dest_root(artifact: PathMapping, src_path: Path) -> Path:
    """Return the deploy-root path that the artifact's src maps to.

    Trailing-slash semantics mirror :func:`bundle_map._specifies_directory`:
    a trailing ``/`` on dest means "copy src into dest as a child."
    """
    if not artifact.dest:
        return src_path

    dest_path = Path(artifact.dest.rstrip("/"))
    if artifact.dest.endswith("/"):
        return dest_path / src_path.name
    return dest_path


class StreamlitEntity(EntityBase[StreamlitEntityModel]):
    """
    A Streamlit app.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def root(self):
        return self._workspace_ctx.project_root

    @property
    def artifacts(self):
        return self._entity_model.artifacts

    def action_bundle(self, action_ctx: ActionContext, *args, **kwargs):
        return self.bundle()

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        return self.deploy(action_ctx, *args, **kwargs)

    def action_describe(self, action_ctx: ActionContext, *args, **kwargs):
        return self.describe()

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._execute_query(self.get_drop_sql())

    def action_execute(
        self, action_ctx: ActionContext, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._execute_query(self.get_execute_sql())

    def action_get_url(
        self, action_ctx: ActionContext, *args, **kwargs
    ):  # maybe this should be a property
        name = self._entity_model.fqn.using_connection(self._conn)
        return make_snowsight_url(
            self._conn, f"/#/streamlit-apps/{name.url_identifier}"
        )

    def _compute_pool_applies(self) -> bool:
        """Whether COMPUTE_POOL is meaningful for the configured runtime.

        Snowflake ignores COMPUTE_POOL when RUNTIME_NAME is the warehouse runtime,
        so the clause is left out rather than sent and discarded.
        """
        return (
            bool(self.model.compute_pool)
            and self.model.runtime_name != WAREHOUSE_RUNTIME_NAME
        )

    def _resolved_query_warehouse(self, live_warehouse: Optional[str] = None) -> str:
        """Warehouse for CREATE: YAML, then the live object's, then the connection.

        The historical fallback identifier ``streamlit`` is not a real
        warehouse in most accounts and made first deploys fail after a
        deprecation warning. Prefer the connection warehouse so a typical
        ``snowflake.yml`` that omits ``query_warehouse`` still works.
        ``live_warehouse`` is set when CREATE OR REPLACE converts an existing
        app, so the connection does not silently swap the app's warehouse.
        ALTER does not call this; see :meth:`get_alter_sql`.
        """
        if self.model.query_warehouse:
            return self.model.query_warehouse
        if live_warehouse and live_warehouse.strip():
            return live_warehouse
        warehouse = self._workspace_ctx.default_warehouse
        if isinstance(warehouse, str) and warehouse.strip():
            return warehouse
        raise CliError(
            "query_warehouse is not set in snowflake.yml and the current "
            "connection has no warehouse. Set query_warehouse on the Streamlit "
            "entity, or configure a warehouse on the connection."
        )

    def bundle(self, output_dir: Optional[Path] = None) -> BundleMap:
        artifacts = list(self._entity_model.artifacts or [])

        # Ensure main_file is included in artifacts
        main_file = self._entity_model.main_file
        if main_file and not _main_file_covered_by_artifacts(
            main_file, artifacts, self.root
        ):
            artifacts.insert(0, PathMapping(src=main_file))

        # Native v2 projects used to require listing these. Docs and v1→v2
        # conversion already treated them as default uploads; include them
        # when they exist so `artifacts: [streamlit_app.py]` still ships a
        # working multi-page app.
        env_file = DEFAULT_ENV_FILE
        if (self.root / env_file).exists() and not _path_covered_by_artifacts(
            env_file, artifacts, self.root
        ):
            artifacts.append(PathMapping(src=env_file))

        pages_dir = self._entity_model.pages_dir or DEFAULT_PAGES_DIR
        if (self.root / pages_dir).exists() and not _path_covered_by_artifacts(
            pages_dir, artifacts, self.root
        ):
            artifacts.append(PathMapping(src=pages_dir))

        return build_bundle(
            self.root,
            output_dir or bundle_root(self.root, "streamlit") / self.entity_id,
            [
                PathMapping(
                    src=artifact.src, dest=artifact.dest, processors=artifact.processors
                )
                for artifact in artifacts
            ],
        )

    def deploy(
        self,
        action_context: ActionContext,
        _open: bool,
        replace: bool,
        prune: bool = False,
        bundle_map: Optional[BundleMap] = None,
        legacy: bool = False,
        *args,
        **kwargs,
    ):
        if (
            bundle_map is None
        ):  # TODO: maybe we could hold bundle map as a cached property?
            bundle_map = self.bundle()

        console = self._workspace_ctx.console
        console.step(f"Checking if object exists")
        object_exists = self._object_exists()

        if object_exists and not replace:
            raise CliError(
                f"Streamlit {self.model.fqn.sql_identifier} already exists. "
                "Re-run with --replace to update the existing app."
            )

        if legacy and self.model.runtime_name == SPCS_RUNTIME_V2_NAME:
            # A legacy ROOT_LOCATION deployment cannot carry RUNTIME_NAME at all, so
            # this would silently produce a warehouse-backed app. That is a materially
            # different app from the one requested, hence an error rather than a warning.
            raise CliError(
                f"runtime_name {SPCS_RUNTIME_V2_NAME} is not compatible with the "
                "--legacy flag, which cannot set RUNTIME_NAME. Remove --legacy to use "
                "versioned deployment, or remove runtime_name and compute_pool from "
                "your snowflake.yml to use legacy deployment."
            )
        elif legacy and self.model.runtime_name:
            # Dropping the warehouse runtime is closer to a no-op, since a legacy app
            # is warehouse-backed anyway, so this warns instead of failing. Staying
            # silent is the failure this deploy path is otherwise fixing.
            console.warning(
                f"runtime_name {self.model.runtime_name} is ignored for --legacy "
                "deployments, which cannot set RUNTIME_NAME. Remove --legacy to deploy "
                "on the requested runtime."
            )

        if self.model.compute_pool and not self._compute_pool_applies():
            console.warning(
                f"compute_pool {sanitize_for_terminal(self.model.compute_pool)} is "
                f"ignored because runtime_name is {self.model.runtime_name}, which does "
                "not run on a compute pool. Remove compute_pool, or set runtime_name "
                f"to {SPCS_RUNTIME_V2_NAME}."
            )

        # Warn if replacing with a different deployment style
        if object_exists and replace:
            existing_is_legacy = self._is_legacy_deployment()
            if existing_is_legacy and not legacy:
                console.warning(
                    "Replacing legacy ROOT_LOCATION deployment with versioned deployment. "
                    "Files from the old stage location will be copied onto the new "
                    "versioned stage when possible. Review the new stage if the copy fails."
                )
            elif not existing_is_legacy and legacy:
                console.warning(
                    "Deployment style is changing from versioned to legacy. "
                    "Your existing files will remain in the versioned stage. "
                    "If needed, manually copy any additional files to the legacy stage after deployment."
                )

        # --replace only acts on an object DESCRIBE could see. When it could
        # not, the create paths below never use CREATE OR REPLACE.
        if legacy:
            self._deploy_legacy(
                bundle_map=bundle_map,
                prune=prune,
                object_exists=object_exists,
            )
        else:
            self._deploy_versioned(
                bundle_map=bundle_map,
                prune=prune,
                object_exists=object_exists,
            )

        return self.perform(EntityActions.GET_URL, action_context, *args, **kwargs)

    def describe(self) -> SnowflakeCursor:
        return self._execute_query(self.get_describe_sql(), cursor_class=DictCursor)

    def action_share(
        self, action_ctx: ActionContext, to_role: str, *args, **kwargs
    ) -> SnowflakeCursor:
        return self._execute_query(self.get_share_sql(to_role))

    def get_add_live_version_sql(
        self, schema: Optional[str] = None, database: Optional[str] = None
    ):
        # ADD LIVE VERSION rejects IDENTIFIER('db.schema.name'). Quote each
        # part the same way as the rest of the entity SQL so hyphenated or
        # reserved names still parse. Qualifiers mirror FQN.prefix: a database
        # without a schema means PUBLIC, since "db.name" would read as
        # schema.name.
        fqn = self._get_fqn(schema, database)
        if fqn.database:
            qualifiers = [fqn.database, fqn.schema or "PUBLIC"]
        else:
            qualifiers = [fqn.schema] if fqn.schema else []
        parts = [to_identifier(part) for part in (*qualifiers, fqn.name)]
        return f"ALTER STREAMLIT {'.'.join(parts)} ADD LIVE VERSION FROM LAST;"

    def get_restart_sql(self) -> str:
        """Return the restart CALL with the identifier left as a bind placeholder.

        The identifier is bound rather than quoted into the SQL text, matching
        ``SYSTEM$GET_APPLICATION_SERVICE_LOGS`` in apps/manager.py and
        ``SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN`` in log_streaming.py. Qmark
        style because :meth:`SqlExecutor.execute_query_with_params` forces it.
        """
        return f"CALL {_RESTART_STREAMLIT_FUNCTION}(?);"

    def get_alter_sql(
        self,
        from_stage_name: Optional[str] = None,
        schema: Optional[str] = None,
        database: Optional[str] = None,
        legacy: bool = False,
        current: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        def _str(v: Any) -> str:
            return (v or "").strip()

        def _id(v: Any) -> str:
            return (v or "").strip().upper()

        def _list(v: Any) -> list:
            if not v:
                return []
            if isinstance(v, str):
                try:
                    v = json.loads(v)
                except (ValueError, TypeError):
                    return []
            return sorted(s.upper() for s in v)

        cur = current or {}
        clauses = []

        if from_stage_name:
            clauses.append(f"ROOT_LOCATION = {to_string_literal(from_stage_name)}")

        if legacy:
            desired = _str(self._entity_model.main_file)
            if not current or _str(cur.get("main_file")) != desired:
                clauses.append(f"MAIN_FILE = {to_string_literal(desired)}")

        # Only snowflake.yml changes the warehouse of an existing app. Falling
        # back to the connection here would rewrite a live warehouse, or fail
        # a --replace when the connection has none.
        desired_wh = self.model.query_warehouse
        if desired_wh and (
            not current or _id(cur.get("query_warehouse")) != _id(desired_wh)
        ):
            clauses.append(f"QUERY_WAREHOUSE = {to_identifier(desired_wh)}")

        unset_clauses: list[str] = []

        desired_title = _str(self.model.title)
        current_title = _str(cur.get("title")) if current else ""
        if desired_title and (not current or current_title != desired_title):
            clauses.append(f"TITLE = {to_string_literal(desired_title)}")
        elif current and current_title and not desired_title:
            unset_clauses.append("TITLE")

        desired_comment = _str(self.model.comment)
        current_comment = _str(cur.get("comment")) if current else ""
        if desired_comment and (not current or current_comment != desired_comment):
            clauses.append(f"COMMENT = {to_string_literal(desired_comment)}")
        elif current and current_comment and not desired_comment:
            unset_clauses.append("COMMENT")

        desired_eais = _list(self.model.external_access_integrations)
        current_eais = _list(cur.get("external_access_integrations")) if current else []
        if desired_eais and (not current or current_eais != desired_eais):
            clauses.append(self.model.get_external_access_integrations_sql())
        elif current and current_eais and not desired_eais:
            unset_clauses.append("EXTERNAL_ACCESS_INTEGRATIONS")

        if not legacy:
            desired_secrets = self.model.secrets or {}
            cur_secrets = cur.get("external_access_secrets") or {}
            if isinstance(cur_secrets, str):
                try:
                    cur_secrets = json.loads(cur_secrets)
                except (ValueError, TypeError):
                    cur_secrets = {}
            if desired_secrets and (not current or cur_secrets != desired_secrets):
                clauses.append(self.model.get_secrets_sql())
            elif current and cur_secrets and not desired_secrets:
                unset_clauses.append("SECRETS")

        desired_imports = _list(self.model.imports)
        current_imports = _list(cur.get("import_urls")) if current else []
        if desired_imports != current_imports or not current:
            if self.model.imports:
                clauses.append(self.model.get_imports_sql())
            elif current and current_imports:
                # ALTER STREAMLIT UNSET does not list IMPORTS; clear via SET.
                clauses.append("IMPORTS = ()")

        if not from_stage_name and not legacy:
            runtime_changing = bool(self.model.runtime_name) and (
                not current
                or _id(cur.get("runtime_name")) != _id(self.model.runtime_name)
            )
            if runtime_changing:
                clauses.append(
                    f"RUNTIME_NAME = {to_string_literal(self.model.runtime_name)}"
                )
                live_runtime = cur.get("runtime_name")
                if live_runtime:
                    # Moving a running app between runtimes changes how it executes,
                    # which is a bigger deal than the rest of this property diff. Name
                    # it when it happens; the release note is not in front of the user
                    # at the moment of the deploy.
                    self._workspace_ctx.console.warning(
                        f"Moving Streamlit {self.model.fqn.sql_identifier} from runtime "
                        f"{sanitize_for_terminal(str(live_runtime))} to "
                        f"{self.model.runtime_name}. This changes how the app runs."
                    )
            if self._compute_pool_applies() and (
                not current
                or _id(cur.get("compute_pool")) != _id(self.model.compute_pool)
            ):
                clauses.append(
                    f"COMPUTE_POOL = {to_string_literal(self.model.compute_pool)}"
                )
            elif (
                current
                and _str(cur.get("compute_pool"))
                and not self._compute_pool_applies()
            ):
                # ALTER STREAMLIT UNSET does not list COMPUTE_POOL. Snowflake
                # ignores a leftover pool on the warehouse runtime; say so
                # rather than emit an UNSET the server may reject.
                self._workspace_ctx.console.warning(
                    "COMPUTE_POOL remains set on "
                    f"{self.model.fqn.sql_identifier} but is ignored for "
                    f"{self.model.runtime_name or WAREHOUSE_RUNTIME_NAME}."
                )

        if not clauses and not unset_clauses:
            return None

        identifier = self._get_sql_identifier(schema, database)
        statements = []
        if clauses:
            statements.append(
                f"ALTER STREAMLIT {identifier} SET\n" + "\n".join(clauses) + ";"
            )
        if unset_clauses:
            statements.append(
                f"ALTER STREAMLIT {identifier} UNSET {', '.join(unset_clauses)};"
            )
        return "\n".join(statements)

    def get_set_tag_sql(self) -> Optional[str]:
        if not self.model.tags:
            return None
        tag_list = Tag.to_sql_tag_list(self.model.tags)
        return f"ALTER STREAMLIT {self._get_sql_identifier()} SET TAG {tag_list};"

    def get_unset_tag_sql(self, tag_fqns: list[str]) -> str:
        return (
            f"ALTER STREAMLIT {self._get_sql_identifier()} UNSET TAG "
            + ",".join(tag_fqns)
            + ";"
        )

    def _get_current_tags(self) -> list[_TagRef]:
        """Return one _TagRef per tag directly set on this object.

        Filtering by LEVEL = 'STREAMLIT' excludes schema- and database-inherited
        tags, which the deploying role may not have APPLY privilege on and
        therefore cannot UNSET.
        """
        fqn = self._get_fqn()
        db_prefix = f"{to_identifier(fqn.database)}." if fqn.database else ""
        rows = self._execute_query(
            f"SELECT TAG_DATABASE, TAG_SCHEMA, TAG_NAME "
            f"FROM TABLE({db_prefix}information_schema.tag_references("
            f"{to_string_literal(fqn.identifier)}, 'STREAMLIT')) "
            f"WHERE LEVEL = 'STREAMLIT'"
        ).fetchall()
        return [
            _TagRef(
                name=row[2].upper(),
                fqn=f"{to_identifier(row[0])}.{to_identifier(row[1])}.{to_identifier(row[2])}",
            )
            for row in rows
        ]

    def _sync_tags(self) -> None:
        if self.model.tags is None:
            return
        current = self._get_current_tags()
        desired = {t.name.upper() for t in self.model.tags}
        to_unset_fqns = [ref.fqn for ref in current if ref.name not in desired]
        if to_unset_fqns:
            self._execute_query(self.get_unset_tag_sql(to_unset_fqns))
        set_tag_sql = self.get_set_tag_sql()
        if set_tag_sql:
            self._execute_query(set_tag_sql)

    def get_deploy_sql(
        self,
        if_not_exists: bool = False,
        replace: bool = False,
        from_stage_name: Optional[str] = None,
        artifacts_dir: Optional[Path] = None,
        schema: Optional[str] = None,
        database: Optional[str] = None,
        legacy: bool = False,
        live_query_warehouse: Optional[str] = None,
        *args,
        **kwargs,
    ) -> str:
        if replace:
            query = "CREATE OR REPLACE STREAMLIT"
        elif if_not_exists:
            query = "CREATE STREAMLIT IF NOT EXISTS"
        else:
            query = "CREATE STREAMLIT"

        query += f" {self._get_sql_identifier(schema, database)}"

        if from_stage_name:
            query += f"\nROOT_LOCATION = {to_string_literal(from_stage_name)}"
        elif artifacts_dir:
            query += f"\nFROM {to_string_literal(str(artifacts_dir))}"

        query += f"\nMAIN_FILE = {to_string_literal(self._entity_model.main_file)}"

        if self.model.imports:
            query += "\n" + self.model.get_imports_sql()

        warehouse = self._resolved_query_warehouse(live_query_warehouse)
        query += f"\nQUERY_WAREHOUSE = {to_identifier(warehouse)}"

        if self.model.title:
            query += f"\nTITLE = {to_string_literal(self.model.title)}"

        if self.model.comment:
            query += f"\nCOMMENT = {to_string_literal(self.model.comment)}"

        if self.model.external_access_integrations:
            query += "\n" + self.model.get_external_access_integrations_sql()

        if self.model.secrets:
            query += "\n" + self.model.get_secrets_sql()

        # Runtime fields are only supported for FBE/versioned streamlits (FROM syntax)
        # Never add these fields for stage-based deployments (ROOT_LOCATION syntax)
        # Each field is gated on itself so neither can be dropped because of the other.
        if not from_stage_name and not legacy:
            if self.model.runtime_name:
                query += (
                    f"\nRUNTIME_NAME = {to_string_literal(self.model.runtime_name)}"
                )
            if self._compute_pool_applies():
                query += (
                    f"\nCOMPUTE_POOL = {to_string_literal(self.model.compute_pool)}"
                )

        if self.model.tags:
            query += f"\n{Tag.to_sql_clause(self.model.tags)}"

        return query + ";"

    def get_describe_sql(self) -> str:
        return f"DESCRIBE STREAMLIT {self._get_sql_identifier()};"

    def get_share_sql(self, to_role: str) -> str:
        return (
            f"GRANT USAGE ON STREAMLIT {self._get_sql_identifier()} TO ROLE {to_role};"
        )

    def get_execute_sql(self):
        return f"EXECUTE STREAMLIT {self._get_sql_identifier()}();"

    def get_usage_grant_sql(self, app_role: str, schema: Optional[str] = None) -> str:
        entity_id = self.entity_id
        streamlit_name = f"{schema}.{entity_id}" if schema else entity_id
        return (
            f"GRANT USAGE ON STREAMLIT {streamlit_name} TO APPLICATION ROLE {app_role};"
        )

    def _object_exists(self) -> bool:
        try:
            self.describe()
            return True
        except ProgrammingError as exc:
            # 2003 is Snowflake's "does not exist or not authorized". Any other
            # ProgrammingError (syntax, warehouse, network-wrapped) must not be
            # treated as a missing object — that used to fall through to
            # CREATE STREAMLIT IF NOT EXISTS and hide the real failure.
            # Snowflake cannot tell missing from unauthorized, so callers never
            # follow a False here with CREATE OR REPLACE.
            if getattr(exc, "errno", None) == DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                return False
            raise

    def _is_legacy_deployment(self) -> bool:
        """Check if the existing streamlit uses legacy ROOT_LOCATION deployment."""
        try:
            result = self.describe().fetchone()
            # Versioned deployments have live_version_location_uri, legacy ones don't
            return result.get("live_version_location_uri") is None
        except (ProgrammingError, AttributeError, KeyError):
            # If we can't determine, assume it doesn't exist or is inaccessible
            return False

    def _copy_legacy_stage_if_present(
        self, old_root: Optional[str], new_stage_root: str
    ) -> None:
        """Copy leftover files from a ROOT_LOCATION stage onto the versioned stage.

        CREATE OR REPLACE does not migrate the old stage. Extra files that lived
        only there would otherwise vanish. Failure is a warning: the new bundle
        upload still runs and is the source of truth for files in snowflake.yml.
        """
        if not old_root:
            self._workspace_ctx.console.warning(
                "The previous Streamlit app had no ROOT_LOCATION, so extra files "
                "from the old stage could not be copied. Add any missing files to "
                "artifacts in snowflake.yml."
            )
            return
        try:
            # path_for_sql leaves a plain @stage/path bare and makes anything
            # else (quotes, spaces, ";") a string literal, so the DESCRIBE
            # value can never end the statement.
            source = StagePath.from_stage_str(old_root).path_for_sql()
        except (ValueError, IndexError) as exc:
            self._workspace_ctx.console.warning(
                f"Could not copy files from legacy stage "
                f"{sanitize_for_terminal(old_root)}: {sanitize_for_terminal(str(exc))}. "
                "Files listed in artifacts will still be uploaded."
            )
            return
        dest_uri = new_stage_root.rstrip("/") + "/"
        copy_sql = f"COPY FILES INTO {to_string_literal(dest_uri)} FROM {source}"
        try:
            self._execute_query(copy_sql)
            self._workspace_ctx.console.warning(
                f"Copied files from legacy stage {sanitize_for_terminal(old_root)} "
                f"to {sanitize_for_terminal(new_stage_root)}."
            )
        except Exception as exc:  # noqa: BLE001 — copy is best-effort
            self._workspace_ctx.console.warning(
                f"Could not copy files from legacy stage "
                f"{sanitize_for_terminal(old_root)}: {sanitize_for_terminal(str(exc))}. "
                "Files listed in artifacts will still be uploaded."
            )

    def _deploy_legacy(
        self,
        bundle_map: BundleMap,
        prune: bool = False,
        object_exists: bool = False,
    ):
        console = self._workspace_ctx.console
        console.step(f"Uploading artifacts to stage {self.model.stage}")

        # We use a static method from StageManager here, but maybe this logic could be implemented elswhere, as we implement entities?
        name = (
            self.model.identifier.name
            if isinstance(self.model.identifier, Identifier)
            else self.model.identifier or self.entity_id
        )
        stage_root = StageManager.get_standard_stage_prefix(
            f"{FQN.from_string(self.model.stage).using_connection(self._conn)}/{name}"
        )
        sync_deploy_root_with_stage(
            console=self._workspace_ctx.console,
            deploy_root=bundle_map.deploy_root(),
            bundle_map=bundle_map,
            prune=prune,
            recursive=True,
            stage_path_parts=StageManager().stage_path_parts_from_str(stage_root),
            print_diff=True,
        )

        console.step(f"Creating Streamlit object {self.model.fqn.sql_identifier}")

        if object_exists:
            current = self.describe().fetchone()
            alter_sql = self.get_alter_sql(
                from_stage_name=stage_root, legacy=True, current=current
            )
            if alter_sql:
                self._execute_query(alter_sql)
            self._sync_tags()
        else:
            # Plain CREATE: an app DESCRIBE could not see fails here as
            # "already exists" instead of being replaced.
            self._execute_query(
                self.get_deploy_sql(
                    from_stage_name=stage_root,
                    legacy=True,
                )
            )

        StreamlitManager(connection=self._conn).grant_privileges(self.model)

    def _ensure_live_version_location_uri(self) -> str:
        """Return the live-version stage URI, creating a live version if needed.

        Called after a versioned CREATE / CREATE OR REPLACE so the object is
        already on the embedded-stage (non-ROOT_LOCATION) path. Legacy
        ROOT_LOCATION apps are converted via CREATE OR REPLACE first — we do
        not issue ADD LIVE VERSION against a ROOT_LOCATION object.
        """
        try:
            self._execute_query(self.get_add_live_version_sql())
        except ProgrammingError as e:
            if _is_live_version_already_exists_error(e):
                log.info("Live version already exists, continuing")
            else:
                raise

        row = self.describe().fetchone()
        stage_root = row.get("live_version_location_uri") if row else None
        if not stage_root:
            raise CliError(
                "Snowflake did not report a versioned stage location for Streamlit "
                f"{self.model.fqn}, so the app files could not be uploaded "
                "(DESCRIBE STREAMLIT returned an empty live_version_location_uri). "
                "Please re-run with --legacy to deploy using the legacy ROOT_LOCATION "
                "layout, or check with your account administrator that versioned "
                "Streamlit apps are enabled for this account."
            )
        return stage_root

    def _restart_running_app(self) -> None:
        """Bounce the SPCS service so uploaded files replace cached bytecode.

        A content-only ``--replace`` no longer issues ``CREATE OR REPLACE``, so
        the SPCS process survives. Streamlit's ``ScriptCache`` is process-global
        and is only cleared by a live browser watcher; without a restart the
        container keeps serving what it already compiled. First create and
        ``CREATE OR REPLACE`` conversion skip this — they start a new process.
        """
        console = self._workspace_ctx.console
        identifier = self._get_identifier()
        restart_sql = self.get_restart_sql()
        console.step(
            f"Restarting Streamlit app {sanitize_for_terminal(identifier)} "
            "so the new files take effect"
        )
        self._sql_executor.execute_query_with_params(restart_sql, (identifier,))

    def _deploy_versioned(
        self,
        bundle_map: BundleMap,
        prune: bool = False,
        object_exists: bool = False,
    ):
        restart_after_upload = False
        if object_exists:
            current = self.describe().fetchone() or {}
            stage_root = current.get("live_version_location_uri")
            if not stage_root:
                # Legacy ROOT_LOCATION app: recreate as versioned rather than
                # ALTER + ADD LIVE VERSION on an object that still has
                # ROOT_LOCATION (unverified / leaves a hybrid state).
                old_root = current.get("root_location")
                self._execute_query(
                    self.get_deploy_sql(
                        replace=True,
                        legacy=False,
                        live_query_warehouse=current.get("query_warehouse"),
                    )
                )
                stage_root = self._ensure_live_version_location_uri()
                self._copy_legacy_stage_if_present(old_root, stage_root)
            else:
                # Already versioned — update properties in place and upload to
                # the existing live URI (do not re-issue ADD LIVE VERSION).
                alter_sql = self.get_alter_sql(current=current)
                if alter_sql:
                    self._execute_query(alter_sql)
                self._sync_tags()
                # DESCRIBE, not snowflake.yml: a content-only project can omit
                # runtime_name while the live object is already SPCS v2.
                restart_after_upload = _describe_row_is_spcs_v2(current)
        else:
            # IF NOT EXISTS, never OR REPLACE: an app DESCRIBE could not see is
            # left alone, and ADD LIVE VERSION below fails on it instead.
            self._execute_query(
                self.get_deploy_sql(
                    if_not_exists=True,
                    legacy=False,
                )
            )
            try:
                stage_root = self._ensure_live_version_location_uri()
            except ProgrammingError as exc:
                if getattr(exc, "errno", None) != DOES_NOT_EXIST_OR_NOT_AUTHORIZED:
                    raise
                raise CliError(
                    f"Streamlit {self.model.fqn.sql_identifier} was not created, "
                    "or the current role cannot access it. It may already exist "
                    "and be owned by another role. Deploy with a role that owns "
                    "the app, or choose a different identifier."
                ) from exc
        stage_path_parts = StageManager().stage_path_parts_from_str(stage_root)

        diff = sync_deploy_root_with_stage(
            console=self._workspace_ctx.console,
            deploy_root=bundle_map.deploy_root(),
            bundle_map=bundle_map,
            prune=prune,
            recursive=True,
            stage_path_parts=stage_path_parts,
            print_diff=True,
            force_overwrite=True,  # files copied to streamlit vstage need to be overwritten
        )

        StreamlitManager(connection=self._conn).grant_privileges(self.model)
        # Property-only ALTER does not clear ScriptCache. Skip when the stage
        # already matches (no-op --replace / CI with unchanged artifacts).
        if restart_after_upload and diff.has_changes():
            self._restart_running_app()
