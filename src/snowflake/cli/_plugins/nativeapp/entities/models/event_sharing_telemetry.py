from typing import List, Optional

from click import ClickException
from pydantic import Field, field_validator, model_validator
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class EventSharingTelemetry(UpdatableModel):
    authorize_event_sharing: Optional[bool] = Field(
        title="Whether to authorize Snowflake to share application usage data with application package provider. This automatically enables the sharing of required telemetry events.",
        default=None,
    )
    shared_events: Optional[List[str]] = Field(
        title="List of optional telemetry events that application owner would like to share with application package provider.",
        default=None,
    )

    @model_validator(mode="after")
    @classmethod
    def validate_authorize_event_sharing(cls, value):
        if value.shared_events and value.authorize_event_sharing is False:
            raise ClickException(
                "telemetry.authorize_event_sharing cannot be disabled when sharing events through telemetry.shared_events."
            )
        return value

    @field_validator("shared_events")
    @classmethod
    def validate_shared_events(
        cls, original_shared_events: Optional[List[str]]
    ) -> Optional[List[str]]:
        if original_shared_events is None:
            return None

        # make sure that each event is made of letters and underscores:
        for event in original_shared_events:
            if not event.isalpha() and not event.replace("_", "").isalpha():
                raise ClickException(
                    f"Event {event} from telemetry.shared_events field is not a valid event name."
                )

        # make sure events are unique:
        if len(original_shared_events) != len(set(original_shared_events)):
            raise ClickException(
                "Events in telemetry.shared_events field must be unique."
            )

        return original_shared_events
