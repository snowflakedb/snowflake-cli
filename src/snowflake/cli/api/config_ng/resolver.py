# Copyright (c) 2024 Snowflake Inc.
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

"""
Configuration resolver with resolution history tracking.

This module implements:
- ResolutionHistoryTracker: Tracks configuration value discoveries and precedence
- ConfigurationResolver: Orchestrates sources and resolves configuration values
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionHistory,
    SourceDiagnostic,
    SourceType,
)
from snowflake.cli.api.config_ng.observers import (
    ResolutionHistoryTracker,
    ResolutionObserver,
    TelemetryObserver,
    create_observer_bundle,
)
from snowflake.cli.api.sanitizers import sanitize_source_error

if TYPE_CHECKING:
    from snowflake.cli.api.config_ng.core import ValueSource

log = logging.getLogger(__name__)


class ConfigurationResolver:
    """
    Orchestrates configuration sources with resolution history tracking.

    This is the main entry point for configuration resolution. It:
    - Manages multiple configuration sources in precedence order
    - Applies precedence rules based on source list order
    - Tracks complete resolution history

    Sources should be provided in precedence order (lowest to highest priority).
    Later sources in the list override earlier sources.

    For presentation/formatting of resolution data, use ResolutionPresenter
    from the presentation module.

    Example:
        from snowflake.cli.api.config_ng import ConfigurationResolver
        from snowflake.cli.api.config_ng.presentation import ResolutionPresenter

        resolver = ConfigurationResolver(
            sources=[
                snowsql_config,     # Lowest priority
                cli_config,
                env_source,
                cli_arguments,      # Highest priority
            ]
        )

        # Resolve all configuration
        config = resolver.resolve()

        # For debugging/presentation, use the presenter
        presenter = ResolutionPresenter(resolver)
        presenter.print_resolution_chain("account")
        presenter.export_history(Path("debug_config.json"))
    """

    def __init__(
        self,
        sources: Optional[List["ValueSource"]] = None,
        observers: Optional[List[ResolutionObserver]] = None,
        enable_history: bool = True,
    ):
        """
        Initialize resolver with sources and optional observers.

        Args:
            sources: Configuration sources in precedence order
            observers: Optional list of observers to attach
            enable_history: Whether to attach the history tracker by default
        """
        self._sources = sources or []
        (
            self._observers,
            self._telemetry_observer,
            self._history_observer,
        ) = self._initialize_observers(observers, enable_history)
        self._source_diagnostics: List[SourceDiagnostic] = []

    def _initialize_observers(
        self,
        observers: Optional[List[ResolutionObserver]],
        enable_history: bool,
    ) -> tuple[
        list[ResolutionObserver], TelemetryObserver, Optional[ResolutionHistoryTracker]
    ]:
        if observers is None:
            return create_observer_bundle(enable_history=enable_history)

        observer_list = list(observers)
        telemetry = next(
            (obs for obs in observer_list if isinstance(obs, TelemetryObserver)), None
        )
        history = next(
            (obs for obs in observer_list if isinstance(obs, ResolutionHistoryTracker)),
            None,
        )

        if telemetry is None:
            telemetry = TelemetryObserver()
            observer_list.append(telemetry)

        if history is None and enable_history:
            history = ResolutionHistoryTracker()
            observer_list.append(history)

        return observer_list, telemetry, history

    def attach_observer(self, observer: ResolutionObserver) -> None:
        """Attach a new observer at runtime."""
        self._observers.append(observer)
        if isinstance(observer, TelemetryObserver):
            self._telemetry_observer = observer
        if isinstance(observer, ResolutionHistoryTracker):
            self._history_observer = observer

    def ensure_history_tracking(self) -> bool:
        """
        Ensure a history tracker is attached.

        Returns:
            True if a new tracker was attached and observers should be reset.
        """
        if self._history_observer is not None:
            return False
        history = ResolutionHistoryTracker()
        self.attach_observer(history)
        return True

    def _reset_observers(self) -> None:
        for observer in self._observers:
            observer.reset()

    def _notify(self, method_name: str, *args, **kwargs) -> None:
        for observer in self._observers:
            getattr(observer, method_name)(*args, **kwargs)

    def get_sources(self) -> List["ValueSource"]:
        """Get list of all sources in precedence order (for inspection)."""
        return self._sources.copy()

    def _parse_connection_key(self, key: str) -> Optional[Tuple[str, str]]:
        """
        Parse a connection key into (connection_name, parameter).

        Args:
            key: Configuration key (e.g., "connections.prod.account")

        Returns:
            Tuple of (connection_name, parameter) or None if not a connection key
        """
        if not key.startswith("connections."):
            return None

        parts = key.split(".", 2)
        if len(parts) != 3:
            return None

        return parts[1], parts[2]  # (conn_name, param)

    def _get_sources_by_type(self, source_type: SourceType) -> List["ValueSource"]:
        """
        Get all sources matching the specified type.

        Args:
            source_type: Type of source to filter by

        Returns:
            List of sources matching the type
        """
        return [s for s in self._sources if s.source_type is source_type]

    def _record_discoveries(self, source_values: Dict[str, ConfigValue]) -> None:
        """
        Record all discovered values in history tracker.

        Args:
            source_values: Dictionary of discovered configuration values
        """
        if self._history_observer is None:
            return
        for k, config_value in source_values.items():
            self._history_observer.record_discovery(k, config_value)

    def _collect_source_diagnostics(self, source: "ValueSource") -> None:
        diagnostics = []
        consumer = getattr(source, "consume_diagnostics", None)
        if callable(consumer):
            diagnostics = consumer()
        elif hasattr(source, "get_diagnostics"):
            getter = getattr(source, "get_diagnostics")
            diagnostics = getter() if callable(getter) else getter  # type: ignore[misc]

        if not diagnostics:
            return

        for diagnostic in diagnostics:
            if isinstance(diagnostic, SourceDiagnostic):
                entry = diagnostic
            elif isinstance(diagnostic, dict):
                entry = SourceDiagnostic(
                    source_name=diagnostic.get("source_name", source.source_name),
                    level=diagnostic.get("level", "info"),
                    message=diagnostic.get("message", ""),
                )
            else:
                entry = SourceDiagnostic(
                    source_name=source.source_name,
                    level="info",
                    message=str(diagnostic),
                )
            self._source_diagnostics.append(entry)

    def _finalize_history(self, all_values: Dict[str, ConfigValue]) -> None:
        """
        Mark which values were selected in resolution history.

        Args:
            all_values: Final dictionary of selected configuration values
        """
        if self._history_observer is None:
            return
        for k, config_value in all_values.items():
            self._history_observer.mark_selected(k, config_value.source_name)

    def _apply_default(
        self, resolved: Dict[str, Any], key: str, default: Any
    ) -> Dict[str, Any]:
        """
        Apply default value for a specific key if provided.

        Args:
            resolved: Current resolved configuration dictionary
            key: Configuration key
            default: Default value to apply

        Returns:
            Updated resolved dictionary
        """
        if default is not None:
            resolved[key] = default
            self._notify("mark_default_used", key, default)
        return resolved

    def _group_by_connection(
        self, source_values: Dict[str, ConfigValue]
    ) -> Tuple[Dict[str, Dict[str, ConfigValue]], set[str]]:
        """
        Group connection parameters by connection name.

        Args:
            source_values: All values discovered from a source

        Returns:
            Tuple of (per_conn, empty_connections):
            - per_conn: Dict mapping connection name to its ConfigValue parameters
            - empty_connections: Set of connection names that are empty
        """
        per_conn: Dict[str, Dict[str, ConfigValue]] = defaultdict(dict)
        empty_connections: set[str] = set()

        for k, config_value in source_values.items():
            parsed = self._parse_connection_key(k)
            if parsed is None:
                continue

            conn_name, param = parsed

            # Track empty connection markers
            if param == "_empty_connection":
                empty_connections.add(conn_name)
            else:
                per_conn[conn_name][k] = config_value

        return per_conn, empty_connections

    def _extract_flat_values(
        self, source_values: Dict[str, ConfigValue]
    ) -> Dict[str, ConfigValue]:
        """
        Extract non-connection (flat) configuration values.

        Args:
            source_values: All values discovered from a source

        Returns:
            Dictionary of flat configuration values (non-connection keys)
        """
        return {
            k: v for k, v in source_values.items() if not k.startswith("connections.")
        }

    def _replace_connections(
        self,
        file_connections: Dict[str, Dict[str, ConfigValue]],
        per_conn: Dict[str, Dict[str, ConfigValue]],
        empty_connections: set[str],
        source: "ValueSource",
    ) -> None:
        """
        Replace entire connections with new definitions from source.

        This implements connection-level replacement: when a FILE source defines
        a connection, it completely replaces any previous definition.

        Args:
            file_connections: Accumulator for all file-based connections
            per_conn: New connection definitions from current source
            empty_connections: Set of empty connection names from current source
            source: The source providing these connections
        """
        all_conn_names = set(per_conn.keys()) | empty_connections

        for conn_name in all_conn_names:
            conn_params = per_conn.get(conn_name, {})
            log.debug(
                "Connection %s replaced by file source %s (%d params)",
                conn_name,
                source.source_name,
                len(conn_params),
            )
            file_connections[conn_name] = conn_params

    def _resolve_file_sources(self, key: Optional[str] = None) -> Dict[str, Any]:
        """
        Process FILE sources with connection-level replacement semantics.

        FILE sources replace entire connections rather than merging fields.
        Later FILE sources override earlier ones completely.

        Args:
            key: Specific key to resolve (None = all keys)

        Returns:
            Nested dict with merged file source data
        """
        result: Dict[str, Any] = {}

        for source in self._get_sources_by_type(SourceType.FILE):
            try:
                source_data = source.discover(key)

                self._notify("record_nested_discovery", source_data, source.source_name)
                self._collect_source_diagnostics(source)

                if "connections" in source_data:
                    if "connections" not in result:
                        result["connections"] = {}

                    for conn_name, conn_data in source_data["connections"].items():
                        result["connections"][conn_name] = conn_data

                for k, v in source_data.items():
                    if k != "connections":
                        result[k] = v

            except Exception as exc:
                sanitized_error = sanitize_source_error(exc)
                log.warning(
                    "Error from source %s: %s", source.source_name, sanitized_error
                )
                log.debug(
                    "Error from source %s (full details hidden in warnings)",
                    source.source_name,
                    exc_info=exc,
                )

        return result

    def _merge_file_results(
        self,
        file_connections: Dict[str, Dict[str, ConfigValue]],
        file_flat_values: Dict[str, ConfigValue],
    ) -> Dict[str, ConfigValue]:
        """
        Merge file connections and flat values into single dictionary.

        Args:
            file_connections: Connection parameters from file sources
            file_flat_values: Flat configuration values from file sources

        Returns:
            Merged dictionary of all file-based configuration values
        """
        all_values: Dict[str, ConfigValue] = {}

        for conn_params in file_connections.values():
            all_values.update(conn_params)

        all_values.update(file_flat_values)

        return all_values

    def _apply_overlay_sources(
        self, base: Dict[str, Any], key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Apply OVERLAY sources with field-level merging.

        OVERLAY sources (env vars, CLI args) add or override individual fields
        without replacing entire connections. General params are merged into
        each existing connection.

        Args:
            base: Base configuration (typically from file sources)
            key: Specific key to resolve (None = all keys)

        Returns:
            Updated dictionary with overlay values applied
        """
        from snowflake.cli.api.config_ng.dict_utils import deep_merge
        from snowflake.cli.api.config_ng.merge_operations import (
            extract_root_level_connection_params,
            merge_params_into_connections,
        )

        result = base.copy()

        for source in self._get_sources_by_type(SourceType.OVERLAY):
            try:
                source_data = source.discover(key)

                self._notify("record_nested_discovery", source_data, source.source_name)
                self._collect_source_diagnostics(source)

                general_params, other_data = extract_root_level_connection_params(
                    source_data
                )

                result = deep_merge(result, other_data)

                if general_params and "connections" in result and result["connections"]:
                    connection_names = [
                        name
                        for name in result["connections"]
                        if isinstance(result["connections"][name], dict)
                    ]

                    self._notify(
                        "record_general_params_merged_to_connections",
                        general_params,
                        connection_names,
                        source.source_name,
                    )

                    result["connections"] = merge_params_into_connections(
                        result["connections"], general_params
                    )
                elif general_params:
                    result = deep_merge(result, general_params)

            except Exception as exc:
                sanitized_error = sanitize_source_error(exc)
                log.warning(
                    "Error from source %s: %s", source.source_name, sanitized_error
                )
                log.debug(
                    "Error from source %s (full details hidden in warnings)",
                    source.source_name,
                    exc_info=exc,
                )

        if "connections" in result and result["connections"]:
            remaining_general_params, _ = extract_root_level_connection_params(result)

            if remaining_general_params:
                for conn_name in result["connections"]:
                    if isinstance(result["connections"][conn_name], dict):
                        result["connections"][conn_name] = deep_merge(
                            remaining_general_params, result["connections"][conn_name]
                        )

                for key in remaining_general_params:
                    if key in result:
                        result.pop(key)

        return result

    def _ensure_default_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure a default connection exists when general connection params are present.

        Border conditions for creating default connection:
        1. No connections exist in config (empty or missing "connections" key)
        2. At least one general connection parameter exists at root level
        3. General params are NOT internal CLI parameters or variables

        This allows users to set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER etc. without
        needing --temporary-connection flag or defining connections in config files.

        Args:
            config: Resolved configuration dictionary

        Returns:
            Configuration with default connection created if conditions are met
        """
        from snowflake.cli.api.config_ng.constants import INTERNAL_CLI_PARAMETERS

        connections = config.get("connections", {})
        if connections:
            return config

        general_params = {}
        for key, value in config.items():
            if (
                key not in ("connections", "variables")
                and key not in INTERNAL_CLI_PARAMETERS
            ):
                general_params[key] = value

        if not general_params:
            return config

        result = config.copy()
        result["connections"] = {"default": general_params.copy()}

        self._notify(
            "replicate_root_level_discoveries_to_connection",
            list(general_params.keys()),
            "default",
        )

        for key in general_params:
            result.pop(key, None)

        return result

    def resolve(self, key: Optional[str] = None, default: Any = None) -> Dict[str, Any]:
        """
        Resolve configuration to nested dict.

        Resolution Process (Four-Phase):

        Phase A - File Sources (Connection-Level Replacement):
        - Process FILE sources in precedence order (lowest to highest priority)
        - For each connection, later FILE sources completely REPLACE earlier ones
        - Fields from earlier file sources are NOT inherited

        Phase B - Overlay Sources (Field-Level Overlay):
        - Start with the file-derived configuration
        - Process OVERLAY sources (env vars, CLI args) in precedence order
        - These add/override individual fields without replacing entire connections
        - Uses deep merge for nested structures

        Phase C - Default Connection Creation:
        - If no connections exist but general params present, create "default" connection
        - Allows env-only configuration without --temporary-connection flag

        Phase D - Resolution History Finalization:
        - Mark which values were selected in the final configuration
        - Enables debugging and diagnostics

        Args:
            key: Specific key to resolve (None = all keys)
            default: Default value if key not found

        Returns:
            Nested dictionary of resolved configuration
        """
        self._reset_observers()
        self._source_diagnostics.clear()
        result = self._resolve_file_sources(key)

        result = self._apply_overlay_sources(result, key)

        result = self._ensure_default_connection(result)

        self._finalize_resolution_history(result)

        return result

    def get_tracker(self) -> Optional[ResolutionHistoryTracker]:
        """
        Get the history tracker for direct access to resolution data.

        Returns:
            ResolutionHistoryTracker instance
        """
        return self._history_observer

    def get_source_diagnostics(self) -> List[SourceDiagnostic]:
        """Diagnostics captured during source discovery."""
        return self._source_diagnostics.copy()

    def get_resolution_summary(self) -> Dict[str, Any]:
        """
        Get resolution summary from the history tracker or telemetry observer.
        """
        if self._history_observer:
            return self._history_observer.get_summary()
        return self._telemetry_observer.get_summary()

    def _finalize_resolution_history(self, final_config: Dict[str, Any]) -> None:
        """
        Mark which values were selected in final configuration.

        Delegates to the history tracker which handles all history-related logic.

        Args:
            final_config: The final resolved configuration (nested dict)
        """
        self._notify("finalize_with_result", final_config)

    def get_resolution_history(self, key: str) -> Optional[ResolutionHistory]:
        """
        Get complete resolution history for a key.

        Supports both formats:
        - Flat: "connections.test.account"
        - Root-level: "account" (checks connections for this key)

        Args:
            key: Configuration key (flat or simple)

        Returns:
            ResolutionHistory showing the full precedence chain
        """
        tracker = self._history_observer
        if not tracker:
            return None

        history = tracker.get_history(key)
        if history:
            return history

        if "." not in key:
            all_histories = tracker.get_all_histories()
            for hist_key, hist in all_histories.items():
                if hist_key.endswith(f".{key}"):
                    return hist

        return None

    def get_all_histories(self) -> Dict[str, ResolutionHistory]:
        """Get resolution histories for all keys."""
        tracker = self._history_observer
        return tracker.get_all_histories() if tracker else {}
