"""Observer abstractions for configuration resolution."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionEntry,
    ResolutionHistory,
)


def flatten_nested_dict(nested: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Flatten nested dictionaries using dot-separated keys."""

    flat: Dict[str, Any] = {}
    for key, value in nested.items():
        dotted_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict) and value:
            flat.update(flatten_nested_dict(value, dotted_key))
        else:
            flat[dotted_key] = value
    return flat


class ResolutionObserver:
    """Base class with no-op hooks for resolution events."""

    def reset(self) -> None:
        """Reset any cached state."""
        return None

    def record_nested_discovery(
        self, nested_data: Dict[str, Any], source_name: str
    ) -> None:
        """Record discoveries from a nested dict source."""
        return None

    def record_discovery(self, key: str, config_value: ConfigValue) -> None:
        """Record a flattened discovery."""
        return None

    def mark_selected(self, key: str, source_name: str) -> None:
        """Record the source that won a key."""
        return None

    def mark_default_used(self, key: str, default_value: Any) -> None:
        """Record that a default value was used."""
        return None

    def finalize_with_result(self, final_config: Dict[str, Any]) -> None:
        """Finalize observer state with the resolved configuration."""
        return None

    def record_general_params_merged_to_connections(
        self,
        general_params: Dict[str, Any],
        connection_names: List[str],
        source_name: str,
    ) -> None:
        """Record when root-level params are merged into specific connections."""
        return None

    def replicate_root_level_discoveries_to_connection(
        self, param_keys: List[str], connection_name: str
    ) -> None:
        """Replicate prior discoveries into a default connection."""
        return None


class TelemetryObserver(ResolutionObserver):
    """Lightweight observer that tracks summary statistics."""

    def __init__(self) -> None:
        self._discovery_counts: Dict[str, int] = defaultdict(int)
        self._source_usage: Dict[str, int] = defaultdict(int)
        self._source_wins: Dict[str, int] = defaultdict(int)
        self._latest_assignments: Dict[str, str] = {}
        self._default_keys: set[str] = set()
        self._final_keys: set[str] = set()

    def reset(self) -> None:
        self._discovery_counts.clear()
        self._source_usage.clear()
        self._source_wins.clear()
        self._latest_assignments.clear()
        self._default_keys.clear()
        self._final_keys.clear()

    def record_nested_discovery(
        self, nested_data: Dict[str, Any], source_name: str
    ) -> None:
        flat = flatten_nested_dict(nested_data)
        for key in flat:
            self._discovery_counts[key] += 1
            self._source_usage[source_name] += 1
            self._latest_assignments[key] = source_name

    def record_general_params_merged_to_connections(
        self,
        general_params: Dict[str, Any],
        connection_names: List[str],
        source_name: str,
    ) -> None:
        for param_key, param_value in general_params.items():
            flattened_param = flatten_nested_dict({param_key: param_value})
            for flattened_key in flattened_param:
                for connection_name in connection_names:
                    full_key = f"connections.{connection_name}.{flattened_key}"
                    self._discovery_counts[full_key] += 1
                    self._source_usage[source_name] += 1
                    self._latest_assignments[full_key] = source_name

    def replicate_root_level_discoveries_to_connection(
        self, param_keys: List[str], connection_name: str
    ) -> None:
        for param_key in param_keys:
            source_name = self._latest_assignments.get(param_key)
            if source_name is None:
                continue
            conn_key = f"connections.{connection_name}.{param_key}"
            self._discovery_counts[conn_key] += 1
            self._source_usage[source_name] += 1
            self._latest_assignments[conn_key] = source_name

    def mark_default_used(self, key: str, default_value: Any) -> None:
        self._default_keys.add(key)
        self._latest_assignments[key] = "default"

    def finalize_with_result(self, final_config: Dict[str, Any]) -> None:
        flat_final = flatten_nested_dict(final_config)
        self._final_keys = set(flat_final.keys())
        for key in list(self._latest_assignments.keys()):
            if key not in self._final_keys:
                del self._latest_assignments[key]
                continue
            source_name = self._latest_assignments[key]
            self._source_wins[source_name] += 1

    def get_summary(self) -> Dict[str, Any]:
        total_keys = len(self._final_keys) or len(self._latest_assignments)
        keys_with_overrides = sum(
            1
            for key, count in self._discovery_counts.items()
            if (not self._final_keys or key in self._final_keys) and count > 1
        )
        return {
            "total_keys_resolved": total_keys,
            "keys_with_overrides": keys_with_overrides,
            "keys_using_defaults": len(self._default_keys),
            "source_usage": dict(self._source_usage),
            "source_wins": dict(self._source_wins),
        }


class ResolutionHistoryTracker(ResolutionObserver):
    """
    Tracks the complete resolution process for all configuration keys.
    """

    def __init__(self) -> None:
        self._histories: Dict[str, ResolutionHistory] = {}
        self._discoveries: Dict[str, List[tuple[ConfigValue, datetime]]] = defaultdict(
            list
        )

    def reset(self) -> None:
        self.clear()

    def clear(self) -> None:
        self._histories.clear()
        self._discoveries.clear()

    def record_nested_discovery(
        self, nested_data: Dict[str, Any], source_name: str
    ) -> None:
        flat_data = flatten_nested_dict(nested_data)
        timestamp = datetime.now()
        for flat_key, value in flat_data.items():
            config_value = ConfigValue(
                key=flat_key, value=value, source_name=source_name
            )
            self._discoveries[flat_key].append((config_value, timestamp))

    def record_discovery(self, key: str, config_value: ConfigValue) -> None:
        timestamp = datetime.now()
        self._discoveries[key].append((config_value, timestamp))

    def mark_selected(self, key: str, source_name: str) -> None:
        if key not in self._discoveries:
            return

        entries: List[ResolutionEntry] = []
        selected_value = None

        for config_value, timestamp in self._discoveries[key]:
            was_selected = config_value.source_name == source_name
            overridden_by = source_name if not was_selected else None

            entry = ResolutionEntry(
                config_value=config_value,
                timestamp=timestamp,
                was_used=was_selected,
                overridden_by=overridden_by,
            )
            entries.append(entry)

            if was_selected:
                selected_value = config_value.value

        self._histories[key] = ResolutionHistory(
            key=key, entries=entries, final_value=selected_value, default_used=False
        )

    def mark_default_used(self, key: str, default_value: Any) -> None:
        if key in self._histories:
            self._histories[key].default_used = True
            self._histories[key].final_value = default_value
        else:
            self._histories[key] = ResolutionHistory(
                key=key, entries=[], final_value=default_value, default_used=True
            )

    def get_history(self, key: str) -> Optional[ResolutionHistory]:
        return self._histories.get(key)

    def get_all_histories(self) -> Dict[str, ResolutionHistory]:
        return self._histories.copy()

    def finalize_with_result(self, final_config: Dict[str, Any]) -> None:
        flat_final = flatten_nested_dict(final_config)
        for flat_key, final_value in flat_final.items():
            if flat_key not in self._discoveries:
                continue
            discoveries = self._discoveries[flat_key]
            for config_value, _timestamp in reversed(discoveries):
                if config_value.value == final_value:
                    self.mark_selected(flat_key, config_value.source_name)
                    break

    def record_general_params_merged_to_connections(
        self,
        general_params: Dict[str, Any],
        connection_names: List[str],
        source_name: str,
    ) -> None:
        timestamp = datetime.now()
        for param_key, param_value in general_params.items():
            for conn_name in connection_names:
                flat_key = f"connections.{conn_name}.{param_key}"
                config_value = ConfigValue(
                    key=flat_key, value=param_value, source_name=source_name
                )
                self._discoveries[flat_key].append((config_value, timestamp))

    def replicate_root_level_discoveries_to_connection(
        self, param_keys: List[str], connection_name: str
    ) -> None:
        for param_key in param_keys:
            if param_key in self._discoveries:
                conn_key = f"connections.{connection_name}.{param_key}"
                for config_value, timestamp in self._discoveries[param_key]:
                    self._discoveries[conn_key].append((config_value, timestamp))

    def get_summary(self) -> dict:
        total_keys = len(self._histories)
        keys_with_overrides = sum(
            1 for h in self._histories.values() if len(h.overridden_entries) > 0
        )
        keys_using_defaults = sum(1 for h in self._histories.values() if h.default_used)

        source_usage: Dict[str, int] = defaultdict(int)
        source_wins: Dict[str, int] = defaultdict(int)

        for history in self._histories.values():
            for entry in history.entries:
                source_usage[entry.config_value.source_name] += 1
                if entry.was_used:
                    source_wins[entry.config_value.source_name] += 1

        return {
            "total_keys_resolved": total_keys,
            "keys_with_overrides": keys_with_overrides,
            "keys_using_defaults": keys_using_defaults,
            "source_usage": dict(source_usage),
            "source_wins": dict(source_wins),
        }


def create_observer_bundle(
    enable_history: bool = True,
) -> tuple[
    list[ResolutionObserver], TelemetryObserver, Optional[ResolutionHistoryTracker]
]:
    """Convenience helper to create default observer lists."""

    telemetry = TelemetryObserver()
    observers: list[ResolutionObserver] = [telemetry]
    history: Optional[ResolutionHistoryTracker] = None
    if enable_history:
        history = ResolutionHistoryTracker()
        observers.append(history)
    return observers, telemetry, history
