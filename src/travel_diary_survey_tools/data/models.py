"""Data models for trip linking and tour building.

This module uses Pydantic for data validation. Models represent individual records
(rows) rather than entire DataFrames. Use the validate_* functions to validate
Polars DataFrames by iterating through rows.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# Helper Functions -------------------------------------------------------------
def step_field(
    required_in_steps: list[str] | str | None = None,
    **field_kwargs: Any,  # noqa: ANN401
) -> Any:  # noqa: ANN401
    """Create a Field with step metadata.

    This is a wrapper function used to annotate fields in data models
    with metadata indicating in which processing steps the field is required.

    Args:
        required_in_steps: List of step names where this field is required,
                          or the string "all" to require in all steps.
                          If None/empty, field is NOT required in any step.
        **field_kwargs: All other Field parameters (ge, le, default, etc.)

    Returns:
        Field instance with step metadata attached

    Example:
        >>> # Required in all steps
        >>> person_id: int = step_field(ge=1)

        >>> # Required only in specific steps
        >>> age: int | None = step_field(
        ...     required_in_steps=["imputation", "tour_building"],
        ...     ge=0,
        ...     default=None
        ... )

        >>> # Not required in any step (optional everywhere)
        >>> notes: str | None = step_field(default=None)
    """
    if "json_schema_extra" not in field_kwargs:
        field_kwargs["json_schema_extra"] = {}

    # Handle "all" string for required in all steps
    if required_in_steps == "all":
        field_kwargs["json_schema_extra"]["required_in_all_steps"] = True
    elif required_in_steps and len(required_in_steps) > 0:
        # Specific steps list
        field_kwargs["json_schema_extra"]["required_in_steps"] = (
            required_in_steps
        )
    # If None or empty, don't add any metadata (not required anywhere)

    return Field(**field_kwargs)


# Data Models ------------------------------------------------------------------
class HouseholdModel(BaseModel):
    """Household attributes (minimal for tour building)."""

    hh_id: int = step_field(ge=1, required_in_steps="all")
    home_lat: float = step_field(ge=-90, le=90)
    home_lon: float = step_field(ge=-180, le=180)


class PersonModel(BaseModel):
    """Person attributes for tour building."""

    person_id: int = step_field(ge=1, required_in_steps="all")
    hh_id: int = step_field(ge=1, required_in_steps="all")
    age: int | None = step_field(ge=0, default=None)
    work_lat: float | None = step_field(ge=-90, le=90, default=None)
    work_lon: float | None = step_field(ge=-180, le=180, default=None)
    school_lat: float | None = step_field(ge=-90, le=90, default=None)
    school_lon: float | None = step_field(ge=-180, le=180, default=None)
    person_type: int = step_field(ge=1, default=None)


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = step_field(ge=1, required_in_steps="all")
    day_id: int = step_field(ge=1, required_in_steps="all")
    hh_id: int = step_field(ge=1, required_in_steps="all")
    travel_dow: int = step_field(ge=1, le=7)

# Minimal data schema for trip linking
class UnlinkedTripModel(BaseModel):
    """Trip data model for validation."""

    trip_id: int = step_field(ge=1, required_in_steps="all")
    day_id: int = step_field(ge=1, required_in_steps="all")
    person_id: int = step_field(ge=1, required_in_steps="all")
    hh_id: int = step_field(ge=1, required_in_steps="all")
    linked_trip_id: int = step_field(ge=1, required_in_steps=["extract_tours"])
    tour_id: int = step_field(ge=1, required_in_steps=["extract_tours"])
    depart_date: str
    depart_hour: int = step_field(ge=0, le=23)
    depart_minute: int = step_field(ge=0, le=59)
    depart_seconds: int = step_field(ge=0, le=59)
    arrive_date: str
    arrive_hour: int = step_field(ge=0, le=23)
    arrive_minute: int = step_field(ge=0, le=59)
    arrive_seconds: int = step_field(ge=0, le=59)
    o_purpose_category: int
    d_purpose_category: int
    mode_type: int
    duration_minutes: float = step_field(ge=0)
    distance_miles: float = step_field(ge=0)

    depart_time: datetime = step_field(required_in_steps=["link_trip"])
    arrive_time: datetime = step_field(required_in_steps=["link_trip"])


# Subclassing allows you to extend TripModel cleanly
class LinkedTripModel(BaseModel):
    """Linked Trip data model for validation."""

    day_id: int = step_field(ge=1)
    person_id: int = step_field(ge=1)
    hh_id: int = step_field(ge=1)

    linked_trip_id: int = step_field(ge=1, required_in_steps=["link_trip"])
    tour_id: int = step_field(ge=1, required_in_steps=["extract_tours"])

    depart_date: str
    depart_hour: int = step_field(ge=0, le=23)
    depart_minute: int = step_field(ge=0, le=59)
    depart_seconds: int = step_field(ge=0, le=59)
    arrive_date: str
    arrive_hour: int = step_field(ge=0, le=23)
    arrive_minute: int = step_field(ge=0, le=59)
    arrive_seconds: int = step_field(ge=0, le=59)
    o_purpose_category: int
    d_purpose_category: int
    mode_type: int
    duration_minutes: float = step_field(ge=0)
    distance_miles: float = step_field(ge=0)
    depart_time: datetime = step_field(required_in_steps=["link_trip"])
    arrive_time: datetime = step_field(required_in_steps=["link_trip"])

    # Tour level assignment fields
    is_primary_dest_trip: bool = step_field(required_in_steps=["extract_tours"])

class TourModel(BaseModel):
    """Tour-level records with clear, descriptive step_field names."""

    tour_id: int = step_field(ge=1, required_in_steps="all")
    person_id: int = step_field(ge=1, required_in_steps="all")
    day_id: int = step_field(ge=1, required_in_steps="all")
    tour_sequence_num: int = step_field(ge=1)
    tour_category: str  # 'home_based' or 'work_based'
    parent_tour_id: int = step_field(ge=1, required_in_steps=["extract_tours"])

    # Purpose and priority
    primary_purpose: int = step_field(ge=1)
    primary_dest_purpose: int = step_field(ge=1)
    purpose_priority: int = step_field(ge=1)

    # Timing
    origin_depart_time: datetime
    dest_arrive_time: datetime
    dest_depart_time: datetime
    origin_arrive_time: datetime

    # Locations
    o_lat: float = step_field(ge=-90, le=90)
    o_lon: float = step_field(ge=-180, le=180)
    d_lat: float = step_field(ge=-90, le=90)
    d_lon: float = step_field(ge=-180, le=180)
    o_location_type: str  # 'home', 'work', 'school', 'other'
    d_location_type: str

    # Mode hierarchical
    tour_mode: int = step_field(ge=1)
    outbound_mode: int = step_field(ge=1)
    inbound_mode: int = step_field(ge=1)

    # Stops
    num_outbound_stops: int = step_field(ge=0)
    num_inbound_stops: int = step_field(ge=0)

    # Flags
    is_primary_tour: bool
    tour_starts_at_origin: bool
    tour_ends_at_origin: bool
