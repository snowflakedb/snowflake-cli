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

from __future__ import annotations

from snowflake.cli._plugins.sql.completion.catalog import ObjectCatalog, SessionScope
from snowflake.cli._plugins.sql.completion.context import CompletionKind
from snowflake.cli._plugins.sql.completion.introspection import (
    MetadataError,
    SnowflakeMetadataProvider,
)

from tests.sql.test_completion_introspection import FakeConnection, FakeCursor

_SCOPE = SessionScope(
    database="DB",
    schema="SCH",
    primary_role="R1",
    session_id="sess-1",
)


class RecordingProvider:
    def __init__(self, names=None, error=None):
        self.calls: list[tuple] = []
        self.names = list(names or ("ALPHA", "BETA"))
        self.error = error
        self.catalog = None

    def lookup(self, kind, path, prefix):
        self.calls.append((kind, path, prefix))
        if self.error is not None:
            raise self.error
        needle = prefix.lower()
        return [name for name in self.names if name.lower().startswith(needle)]


class ReentrantProvider(RecordingProvider):
    def lookup(self, kind, path, prefix):
        self.calls.append((kind, path, prefix))
        if self.catalog is not None and len(self.calls) == 1:
            self.catalog.lookup(kind, path, prefix)
        return ["JOINED"]


def _catalog(provider, scope=_SCOPE, clock=None) -> ObjectCatalog:
    return ObjectCatalog(provider, scope=scope, clock=clock)


def test_empty_prefix_relation_does_not_call_provider_when_cold():
    provider = RecordingProvider()
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert provider.calls == []


def test_prefix_like_is_not_full_warm_snapshot():
    provider = RecordingProvider(names=("ALPHA", "BETA", "ALSO"))
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ("ALPHA", "ALSO")
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert catalog.lookup(CompletionKind.TABLE, (), "ALS") == ("ALSO",)
    assert provider.calls == [(CompletionKind.TABLE, (), "AL")]


def test_nonempty_length_one_prefix_calls_provider():
    provider = RecordingProvider(names=("ALPHA", "BETA"))
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.TABLE, (), "A") == ("ALPHA",)
    assert provider.calls == [(CompletionKind.TABLE, (), "A")]


def test_nonempty_prefix_miss_calls_provider_and_caches():
    provider = RecordingProvider(names=("ALPHA", "BETA"))
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ("ALPHA",)
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ("ALPHA",)
    assert len(provider.calls) == 1


def test_timeout_is_not_negative_cached_and_retries():
    provider = RecordingProvider(error=MetadataError("timeout"))
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert len(provider.calls) == 2


def test_inflight_skip_is_not_cached_and_retries():
    busy = {"on": True}
    cursor = FakeCursor(
        rows=[("CUSTOMERS", "VIEW")],
        description=(("name",), ("kind",)),
    )
    connection = FakeConnection(cursor)
    provider = SnowflakeMetadataProvider(
        connection_factory=lambda: connection,
        is_busy=lambda: busy["on"],
    )
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.VIEW, (), "c") == ()
    assert cursor.execute_calls == []
    busy["on"] = False
    assert catalog.lookup(CompletionKind.VIEW, (), "c") == ("CUSTOMERS",)
    assert len(cursor.execute_calls) == 1
    assert "SHOW OBJECTS" in cursor.execute_calls[0][0].upper()


def test_network_error_retries_on_next_lookup():
    provider = RecordingProvider(error=MetadataError("network"))
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert len(provider.calls) == 2


def test_privilege_is_negative_cached_for_30s():
    now = {"t": 0.0}
    provider = RecordingProvider(error=MetadataError("privilege"))
    catalog = _catalog(provider, clock=lambda: now["t"])
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert len(provider.calls) == 1
    now["t"] = 29.9
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert len(provider.calls) == 1
    now["t"] = 30.0
    assert catalog.lookup(CompletionKind.TABLE, (), "AL") == ()
    assert len(provider.calls) == 2


def test_empty_provider_result_is_success_cache():
    provider = RecordingProvider(names=())
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.TABLE, (), "ZZ") == ()
    assert catalog.lookup(CompletionKind.TABLE, (), "ZZ") == ()
    assert len(provider.calls) == 1


def test_create_adds_name_only_when_snapshot_is_warm():
    provider = RecordingProvider(names=("OLD",))
    catalog = _catalog(provider)
    catalog.add(CompletionKind.TABLE, "NEW")
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert provider.calls == []

    catalog.lookup(CompletionKind.TABLE, (), "OL")
    catalog.add(CompletionKind.TABLE, "OLDER")
    assert catalog.lookup(CompletionKind.TABLE, (), "OL") == ("OLD", "OLDER")
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert len(provider.calls) == 1


def test_drop_removes_name_from_warm_snapshot():
    provider = RecordingProvider(names=("TABLE_A", "TABLE_B"))
    catalog = _catalog(provider)
    catalog.lookup(CompletionKind.TABLE, (), "TA")
    catalog.remove(CompletionKind.TABLE, "TABLE_B")
    assert catalog.lookup(CompletionKind.TABLE, (), "TA") == ("TABLE_A",)
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert len(provider.calls) == 1


def test_drop_all_clears_snapshots_and_privilege_negatives():
    now = {"t": 0.0}
    provider = RecordingProvider(error=MetadataError("privilege"))
    catalog = _catalog(provider, clock=lambda: now["t"])
    catalog.lookup(CompletionKind.TABLE, (), "AL")
    catalog.drop_all()
    provider.error = None
    provider.names = ["AFTER"]
    assert catalog.lookup(CompletionKind.TABLE, (), "AF") == ("AFTER",)
    assert len(provider.calls) == 2


def test_session_id_change_drops_cache():
    provider = RecordingProvider(names=("ONE",))
    catalog = _catalog(provider)
    catalog.lookup(CompletionKind.TABLE, (), "ON")
    catalog.set_scope(
        SessionScope(
            database="DB",
            schema="SCH",
            primary_role="R1",
            session_id="sess-2",
        )
    )
    provider.names = ["TWO"]
    assert catalog.lookup(CompletionKind.TABLE, (), "TW") == ("TWO",)
    assert len(provider.calls) == 2


def test_role_is_part_of_cache_key():
    provider = RecordingProvider(names=("T1",))
    catalog = _catalog(provider)
    catalog.lookup(CompletionKind.TABLE, (), "T1")
    catalog.set_scope(
        SessionScope(
            database="DB",
            schema="SCH",
            primary_role="R2",
            session_id="sess-1",
        )
    )
    assert catalog.lookup(CompletionKind.TABLE, (), "T1") == ("T1",)
    assert len(provider.calls) == 2


def test_inflight_dedupe_does_not_call_provider_twice():
    provider = ReentrantProvider()
    catalog = _catalog(provider)
    provider.catalog = catalog
    assert catalog.lookup(CompletionKind.TABLE, (), "JO") == ("JOINED",)
    assert len(provider.calls) == 1


def test_column_empty_prefix_calls_provider_only_when_allowed():
    provider = RecordingProvider(names=("COL_A",))
    catalog = _catalog(provider)
    assert catalog.lookup(CompletionKind.COLUMN, ("REL",), "") == ()
    assert provider.calls == []
    assert catalog.lookup(
        CompletionKind.COLUMN, ("REL",), "", allow_empty_prefix=True
    ) == ("COL_A",)
    assert len(provider.calls) == 1


def _warm_tables(catalog, provider):
    catalog.lookup(CompletionKind.TABLE, (), "OL")
    return catalog


def test_on_success_create_table_patches_warm_snapshot():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("CREATE TABLE OLDER")
    assert catalog.lookup(CompletionKind.TABLE, (), "OL") == ("OLD", "OLDER")
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert len(provider.calls) == 1


def test_on_success_create_or_replace_and_if_not_exists():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("CREATE OR REPLACE TABLE IF NOT EXISTS OLD_T")
    assert "OLD_T" in catalog.lookup(CompletionKind.TABLE, (), "OL")
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()


def test_on_success_drop_removes_from_warm_snapshot():
    provider = RecordingProvider(names=("TABLE_A", "TABLE_B"))
    catalog = _catalog(provider)
    catalog.lookup(CompletionKind.TABLE, (), "TA")
    catalog.on_success("DROP TABLE TABLE_B")
    assert catalog.lookup(CompletionKind.TABLE, (), "TA") == ("TABLE_A",)
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()


def test_on_success_use_role_drops_all():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("USE ROLE ANALYST")
    provider.names = ["AFTER"]
    assert catalog.lookup(CompletionKind.TABLE, (), "AF") == ("AFTER",)
    assert len(provider.calls) == 2


def test_on_success_use_database_drops_relation_keys():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("USE DATABASE OTHER_DB")
    provider.names = ["AFTER"]
    assert catalog.lookup(CompletionKind.TABLE, (), "AF") == ("AFTER",)
    assert len(provider.calls) == 2


def test_on_success_use_warehouse_is_noop():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("USE WAREHOUSE COMPUTE_WH")
    assert catalog.lookup(CompletionKind.TABLE, (), "OL") == ("OLD",)
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert len(provider.calls) == 1


def test_on_success_rename_patches_warm_snapshot():
    provider = RecordingProvider(names=("OLD_T",))
    catalog = _catalog(provider)
    catalog.lookup(CompletionKind.TABLE, (), "OL")
    catalog.on_success("ALTER TABLE OLD_T RENAME TO OLD_U")
    names = catalog.lookup(CompletionKind.TABLE, (), "OL")
    assert "OLD_U" in names
    assert "OLD_T" not in names
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()


def test_on_success_create_does_not_warm_cold_snapshot():
    provider = RecordingProvider()
    catalog = _catalog(provider)
    catalog.on_success("CREATE TABLE T_RENDERED")
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert provider.calls == []


def test_on_success_other_sql_is_noop():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("SELECT 1")
    assert catalog.lookup(CompletionKind.TABLE, (), "OL") == ("OLD",)
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert len(provider.calls) == 1


def test_on_success_drop_schema_invalidates_child_tables():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("DROP SCHEMA SCH")
    provider.names = ["AFTER"]
    assert catalog.lookup(CompletionKind.TABLE, (), "AF") == ("AFTER",)
    assert len(provider.calls) == 2


def test_on_success_drop_database_invalidates_child_tables():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("DROP DATABASE DB")
    provider.names = ["AFTER"]
    assert catalog.lookup(CompletionKind.TABLE, (), "AF") == ("AFTER",)
    assert len(provider.calls) == 2


def test_on_success_rename_schema_invalidates_child_tables():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("ALTER SCHEMA SCH RENAME TO SCH2")
    provider.names = ["AFTER"]
    assert catalog.lookup(CompletionKind.TABLE, (), "AF") == ("AFTER",)
    assert len(provider.calls) == 2


def _warm_columns(catalog, provider, names=("ID", "NAME")):
    provider.names = list(names)
    catalog.lookup(
        CompletionKind.COLUMN, ("DB", "SCH", "T"), "", allow_empty_prefix=True
    )
    return catalog


def test_create_or_replace_table_drops_column_snapshot():
    provider = RecordingProvider()
    catalog = _warm_columns(_catalog(provider), provider)
    catalog.on_success("CREATE OR REPLACE TABLE T (ID INT)")
    provider.names = ["NEW_COL"]
    assert catalog.lookup(
        CompletionKind.COLUMN, ("T",), "", allow_empty_prefix=True
    ) == ("NEW_COL",)
    assert len(provider.calls) == 2


def test_drop_table_drops_column_snapshot():
    provider = RecordingProvider()
    catalog = _warm_columns(_catalog(provider), provider)
    catalog.on_success("DROP TABLE T")
    provider.names = ["AFTER"]
    assert catalog.lookup(
        CompletionKind.COLUMN, ("T",), "", allow_empty_prefix=True
    ) == ("AFTER",)
    assert len(provider.calls) == 2


def test_alter_table_add_column_drops_column_snapshot():
    provider = RecordingProvider()
    catalog = _warm_columns(_catalog(provider), provider)
    catalog.on_success("ALTER TABLE T ADD COLUMN EXTRA INT")
    provider.names = ["ID", "NAME", "EXTRA"]
    assert catalog.lookup(
        CompletionKind.COLUMN, ("T",), "", allow_empty_prefix=True
    ) == ("ID", "NAME", "EXTRA")
    assert len(provider.calls) == 2


def test_rename_table_drops_old_column_snapshot():
    provider = RecordingProvider()
    catalog = _warm_columns(_catalog(provider), provider)
    catalog.on_success("ALTER TABLE T RENAME TO U")
    provider.names = ["AFTER"]
    assert catalog.lookup(
        CompletionKind.COLUMN, ("T",), "", allow_empty_prefix=True
    ) == ("AFTER",)
    assert len(provider.calls) == 2


def test_create_transient_table_patches_warm_snapshot():
    provider = RecordingProvider(names=("OLD",))
    catalog = _warm_tables(_catalog(provider), provider)
    catalog.on_success("CREATE TRANSIENT TABLE OLDER")
    assert "OLDER" in catalog.lookup(CompletionKind.TABLE, (), "OL")
    assert catalog.lookup(CompletionKind.TABLE, (), "") == ()
    assert len(provider.calls) == 1
