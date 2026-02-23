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
import pytest
from snowflake.cli._plugins.dcm.reporters.refresh import (
    NewFormatExtractor,
    OldFormatExtractor,
    RefreshReporter,
    RefreshRow,
)

from tests.dcm.test_reporters.utils import FakeCursor, capture_reporter_output


class TestRefreshReporter:
    # TODO: remove once backend moves to new api response format

    def test_single_refreshed_table(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.CUSTOMERS",
                    "statistics": '{"insertedRows": 1500, "deletedRows": 200}',
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_single_up_to_date_table(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.ORDERS",
                    "statistics": "No new data",
                }
            ]
        }
        output = capture_reporter_output(RefreshReporter(), FakeCursor(data))
        assert output == snapshot

    def test_multiple_tables_mixed_status(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.TABLE_A",
                    "statistics": '{"insertedRows": 50000, "deletedRows": 1000}',
                },
                {
                    "dt_name": "DB.SCHEMA.TABLE_B",
                    "statistics": "No new data",
                },
                {
                    "dt_name": "DB.SCHEMA.TABLE_C",
                    "statistics": '{"insertedRows": 0, "deletedRows": 0}',
                },
                {
                    "dt_name": "DB.SCHEMA.TABLE_D",
                    "statistics": '{"insertedRows": 999999, "deletedRows": 500}',
                },
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_large_numbers(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.BILLIONS",
                    "statistics": '{"insertedRows": 1500000000, "deletedRows": 999999999}',
                },
                {
                    "dt_name": "DB.SCHEMA.TRILLIONS",
                    "statistics": '{"insertedRows": 2500000000000, "deletedRows": 100000000000}',
                },
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_statistics_as_dict(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.DICT_STATS",
                    "statistics": {"insertedRows": 100, "deletedRows": 50},
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_only_insertions(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.INSERTS_ONLY",
                    "statistics": '{"insertedRows": 5000}',
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_only_deletions(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.DELETES_ONLY",
                    "statistics": '{"deletedRows": 3000}',
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_empty_cursor(self, snapshot):
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(None),
        )
        assert output == snapshot

    def test_no_dynamic_tables(self, snapshot):
        data = {"refreshed_tables": []}
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_missing_refreshed_tables_key(self, snapshot):
        data = {"some_other_key": "value"}
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_malformed_statistics_json(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.BAD_JSON",
                    "statistics": "{invalid_json",
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_unknown_statistics_format(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.WEIRD_STATS",
                    "statistics": "some unexpected string",
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_null_values_in_statistics(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.NULL_STATS",
                    "statistics": '{"insertedRows": null, "deletedRows": null}',
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_missing_dt_name(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "statistics": '{"insertedRows": 100}',
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_non_dict_table_entries(self, snapshot):
        data = {
            "refreshed_tables": [
                "not_a_dict",
                {"dt_name": "DB.SCHEMA.VALID", "statistics": "No new data"},
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_string_numbers_in_statistics(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.STRING_NUMS",
                    "statistics": '{"insertedRows": "500", "deletedRows": "100"}',
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_ansi_codes_in_table_name(self, snapshot):
        data = {
            "refreshed_tables": [
                {
                    "dt_name": "DB.SCHEMA.\x1b[31mRED_TABLE\x1b[0m",
                    "statistics": "No new data",
                }
            ]
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot


class TestRefreshRowFormatNumber:
    """Unit tests for number formatting edge cases."""

    @pytest.mark.parametrize(
        "input_num, expected",
        [
            (0, "0"),
            (999, "999"),
            (1000, "1k"),
            (1500, "1.5k"),
            (999999, "999k"),
            (1000000, "1M"),
            (999999999, "999M"),
            (1000000000, "1B"),
            (999999999999, "999B"),
            (1000000000000, "1T"),
            (999999999999999, "999T"),
            (1000000000000000, "1P"),
            (1000000000000000000, "1E"),
        ],
    )
    def test_format_number_boundaries(self, input_num, expected):
        assert RefreshRow._format_number(input_num) == expected  # noqa: SLF001


class TestNewRefreshReporter:
    """Tests for RefreshReporter with new response format (to keep after migration)."""

    def test_single_refreshed_table(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.CUSTOMERS",
                        "statistics": {"inserted_rows": 1500, "deleted_rows": 200},
                        "data_timestamp": "2026-02-05T12:53:13.464Z",
                    }
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_single_up_to_date_table(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.ORDERS",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 0},
                        "data_timestamp": "2026-02-05T12:53:13.464Z",
                    }
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_multiple_tables_mixed_status(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.TABLE_A",
                        "statistics": {"inserted_rows": 50000, "deleted_rows": 1000},
                    },
                    {
                        "table_name": "DB.SCHEMA.TABLE_B",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 0},
                    },
                    {
                        "table_name": "DB.SCHEMA.TABLE_C",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 0},
                    },
                    {
                        "table_name": "DB.SCHEMA.TABLE_D",
                        "statistics": {"inserted_rows": 999999, "deleted_rows": 500},
                    },
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_large_numbers(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.BILLIONS",
                        "statistics": {
                            "inserted_rows": 1500000000,
                            "deleted_rows": 999999999,
                        },
                    },
                    {
                        "table_name": "DB.SCHEMA.TRILLIONS",
                        "statistics": {
                            "inserted_rows": 2500000000000,
                            "deleted_rows": 100000000000,
                        },
                    },
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_only_insertions(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.INSERTS_ONLY",
                        "statistics": {"inserted_rows": 5000, "deleted_rows": 0},
                    }
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_only_deletions(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.DELETES_ONLY",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 3000},
                    }
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_empty_cursor(self, snapshot):
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(None),
        )
        assert output == snapshot

    def test_no_dynamic_tables(self, snapshot):
        data = {"dts_refresh_result": {"refreshed_tables": []}}
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_missing_statistics(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [{"table_name": "DB.SCHEMA.NO_STATS"}]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_missing_table_name(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {"statistics": {"inserted_rows": 100, "deleted_rows": 0}}
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_ansi_codes_in_table_name(self, snapshot):
        data = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.\x1b[31mRED_TABLE\x1b[0m",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 0},
                    }
                ]
            }
        }
        output = capture_reporter_output(
            RefreshReporter(),
            FakeCursor(data),
        )
        assert output == snapshot

    def test_selects_correct_extractor_for_new_format(self):
        data = {"dts_refresh_result": {"refreshed_tables": []}}
        reporter = RefreshReporter()
        extractor_cls = reporter._get_extractor_cls(data)  # noqa: SLF001

        assert extractor_cls is NewFormatExtractor

    def test_selects_correct_extractor_for_old_format(self):
        data = {"refreshed_tables": []}
        reporter = RefreshReporter()
        extractor_cls = reporter._get_extractor_cls(data)  # noqa: SLF001

        assert extractor_cls is OldFormatExtractor
