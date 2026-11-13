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
from pydantic import ValidationError
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.common import Identifier, MetaField
from snowflake.cli.api.project.schemas.v1.native_app.native_app import NativeApp

_SQL_PAYLOAD = "x'); GRANT ROLE ACCOUNTADMIN TO USER alice; --"
_SQL_WITH_IDENTIFIER_SUFFIX = "; GRANT ROLE ACCOUNTADMIN TO USER alice; --x"
_STAGE_SQL = "app_src.stage/x'); GRANT ROLE ACCOUNTADMIN TO USER alice; --"
_QUOTED_STAGE = '"MySourceSchema"."SRC_Stage"'


def test_identifier_field_rejects_sql_that_contains_an_identifier():
    with pytest.raises(ValidationError):
        MetaField(role=_SQL_PAYLOAD)


def test_identifier_field_rejects_sql_with_only_a_trailing_identifier():
    with pytest.raises(ValidationError):
        MetaField(warehouse=_SQL_WITH_IDENTIFIER_SUFFIX)


def test_identifier_field_accepts_unquoted_and_quoted_identifiers():
    assert MetaField(role="MY_ROLE").role == "MY_ROLE"
    assert MetaField(warehouse='"My Warehouse"').warehouse == '"My Warehouse"'


def test_stage_defaults_remain_valid():
    model = ApplicationPackageEntityModel(type="application package", artifacts=[])
    assert model.stage == "app_src.stage"
    assert model.scratch_stage == "app_src.stage_snowflake_cli_scratch"


def test_stage_rejects_a_single_identifier():
    with pytest.raises(ValidationError) as exc_info:
        ApplicationPackageEntityModel(
            type="application package", artifacts=[], stage="just_stage"
        )
    assert exc_info.value.errors()[0]["loc"] == ("stage",)

    with pytest.raises(ValidationError) as source_error:
        NativeApp(name="app", artifacts=[], source_stage="just_stage")
    assert source_error.value.errors()[0]["loc"] == ("source_stage",)


def test_scratch_stage_accepts_one_or_two_part_names():
    single = ApplicationPackageEntityModel(
        type="application package", artifacts=[], scratch_stage="scratch_stage"
    )
    two_part = ApplicationPackageEntityModel(
        type="application package",
        artifacts=[],
        scratch_stage="app_src.stage",
    )
    assert single.scratch_stage == "scratch_stage"
    assert two_part.scratch_stage == "app_src.stage"


def test_stage_accepts_quoted_schema_and_name():
    quoted = ApplicationPackageEntityModel(
        type="application package",
        artifacts=[],
        stage=_QUOTED_STAGE,
    )
    assert quoted.stage == _QUOTED_STAGE


@pytest.mark.parametrize("field_name", ["stage", "scratch_stage"])
def test_stage_fields_reject_path_and_sql(field_name):
    with pytest.raises(ValidationError) as exc_info:
        ApplicationPackageEntityModel(
            type="application package",
            artifacts=[],
            **{field_name: _STAGE_SQL},
        )
    assert exc_info.value.errors()[0]["loc"] == (field_name,)


def test_v1_native_app_stages_reject_sql_suffix_and_accept_defaults():
    with pytest.raises(ValidationError) as source_error:
        NativeApp(name="app", artifacts=[], source_stage=_STAGE_SQL)
    assert source_error.value.errors()[0]["loc"] == ("source_stage",)

    with pytest.raises(ValidationError) as scratch_error:
        NativeApp(name="app", artifacts=[], scratch_stage=_STAGE_SQL)
    assert scratch_error.value.errors()[0]["loc"] == ("scratch_stage",)

    model = NativeApp(name="app", artifacts=[])
    assert model.source_stage == "app_src.stage"
    assert model.scratch_stage == "app_src.stage_snowflake_cli_scratch"


def test_identifier_schema_and_database_reject_sql():
    with pytest.raises(ValidationError):
        Identifier(name="svc", schema="S; GRANT ROLE ACCOUNTADMIN TO USER alice; --")
    with pytest.raises(ValidationError):
        Identifier(
            name="svc",
            database=(
                "D MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY=CPU_X64_XS; "
                "GRANT ROLE ACCOUNTADMIN TO USER alice; --"
            ),
        )


def test_identifier_schema_and_database_accept_identifiers():
    model = Identifier(name="svc", schema="MY_SCHEMA", database='"My DB"')
    assert model.schema_ == "MY_SCHEMA"
    assert model.database == '"My DB"'
