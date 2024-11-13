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

from typing import List, Optional

from click import ClickException
from pydantic import Field, field_validator, model_validator
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class EventSharingTelemetry(UpdatableModel):
    share_mandatory_events: Optional[bool] = Field(
        title="Indicates whether Snowflake is authorized to share application usage data with the application package provider. When enabled, mandatory events will be shared automatically.",
        default=None,
    )
    optional_shared_events: Optional[List[str]] = Field(
        title="A list of optional telemetry events that the application owner consents to share with the application package provider.",
        default=None,
    )

    @model_validator(mode="after")
    @classmethod
    def validate_telemetry_event_sharing(
        cls, value: "EventSharingTelemetry"
    ) -> "EventSharingTelemetry":
        if value.optional_shared_events and not value.share_mandatory_events:
            raise ClickException(
                "'telemetry.share_mandatory_events' must be set to 'true' when sharing optional events through 'telemetry.optional_shared_events'."
            )
        return value

    @field_validator("optional_shared_events")
    @classmethod
    def validate_optional_shared_events(
        cls, original_shared_events: Optional[List[str]]
    ) -> Optional[List[str]]:
        if original_shared_events is None:
            return None

        # make sure each event is non-empty String:
        for event in original_shared_events:
            if not event:
                raise ClickException(
                    "Events listed in 'telemetry.optional_shared_events' must not be blank"
                )

        return original_shared_events
