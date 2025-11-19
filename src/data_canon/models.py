"""Data models for trip linking and tour building.

This module uses Pydantic for data validation.

Models represent individual records (rows) rather than entire DataFrames.
Use the validate_* functions to validate Polars DataFrames by iterating
through rows.
"""

from datetime import datetime

from pydantic import BaseModel

from data_canon.core.step_field import step_field

from .codebook.days import TravelDow
from .codebook.persons import AgeCategory
from .codebook.trips import ModeType, Purpose, PurposeCategory


# Data Models ------------------------------------------------------------------
class HouseholdModel(BaseModel):
    """Household attributes (minimal for tour building)."""

    hh_id: int = step_field(ge=1, unique=True, required_in_steps="all")
    home_lat: float = step_field(ge=-90, le=90)
    home_lon: float = step_field(ge=-180, le=180)


class PersonModel(BaseModel):
    """Person attributes for tour building."""

    person_id: int = step_field(ge=1, unique=True, required_in_steps="all")
    hh_id: int = step_field(ge=1, required_in_steps="all")
    age: AgeCategory
    work_lat: float | None = step_field(ge=-90, le=90, default=None)
    work_lon: float | None = step_field(ge=-180, le=180, default=None)
    school_lat: float | None = step_field(ge=-90, le=90, default=None)
    school_lon: float | None = step_field(ge=-180, le=180, default=None)
    person_type: int | None = step_field(default=None)


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = step_field(ge=1, required_in_steps="all")
    day_id: int = step_field(ge=1, unique=True, required_in_steps="all")
    hh_id: int = step_field(ge=1, required_in_steps="all")
    travel_dow: TravelDow

# Minimal data schema for trip linking
class UnlinkedTripModel(BaseModel):
    """Trip data model for validation."""

    trip_id: int = step_field(ge=1, unique=True, required_in_steps="all")
    day_id: int = step_field(ge=1, required_in_steps="all")
    person_id: int = step_field(ge=1, required_in_steps="all")
    hh_id: int = step_field(ge=1, required_in_steps="all")
    linked_trip_id: int | None = step_field(
        ge=1, required_in_steps=["extract_tours"], default=None
    )
    tour_id: int | None = step_field(
        ge=1, required_in_steps=["extract_tours"], default=None
    )
    depart_date: datetime | str
    depart_hour: int = step_field(ge=0, le=23)
    depart_minute: int = step_field(ge=0, le=59)
    depart_seconds: int = step_field(ge=0, le=59)
    arrive_date: datetime | str
    arrive_hour: int = step_field(ge=0, le=23)
    arrive_minute: int = step_field(ge=0, le=59)
    arrive_seconds: int = step_field(ge=0, le=59)
    o_purpose: Purpose
    d_purpose: Purpose
    o_purpose_category: PurposeCategory
    d_purpose_category: PurposeCategory
    mode_type: ModeType
    duration_minutes: float = step_field(ge=0)
    distance_miles: float = step_field(ge=0)

    depart_time: datetime | None = step_field(
        required_in_steps=["link_trip"], default=None
    )
    arrive_time: datetime | None = step_field(
        required_in_steps=["link_trip"], default=None
    )


# Subclassing allows you to extend TripModel cleanly
class LinkedTripModel(BaseModel):
    """Linked Trip data model for validation."""

    day_id: int = step_field(ge=1, created_in_step="link_trip")
    person_id: int = step_field(ge=1, created_in_step="link_trip")
    hh_id: int = step_field(ge=1, created_in_step="link_trip")

    linked_trip_id: int | None = step_field(
        ge=1, unique=True, created_in_step=["link_trip"], default=None
    )
    tour_id: int | None = step_field(
        ge=1, created_in_step=["extract_tours"], default=None
    )

    depart_date: str | datetime = step_field(created_in_step="link_trip")
    depart_hour: int = step_field(ge=0, le=23, created_in_step="link_trip")
    depart_minute: int = step_field(ge=0, le=59, created_in_step="link_trip")
    depart_seconds: int = step_field(ge=0, le=59, created_in_step="link_trip")
    arrive_date: datetime | str = step_field(created_in_step="link_trip")
    arrive_hour: int = step_field(ge=0, le=23, created_in_step="link_trip")
    arrive_minute: int = step_field(ge=0, le=59, created_in_step="link_trip")
    arrive_seconds: int = step_field(ge=0, le=59, created_in_step="link_trip")
    o_purpose_category: int = step_field(created_in_step="link_trip")
    d_purpose_category: int = step_field(created_in_step="link_trip")
    mode_type: ModeType = step_field(created_in_step="link_trip")
    duration_minutes: float = step_field(ge=0, created_in_step="link_trip")
    distance_miles: float = step_field(ge=0, created_in_step="link_trip")
    depart_time: datetime | None = step_field(
        created_in_step=["link_trip"], default=None
    )
    arrive_time: datetime | None = step_field(
        created_in_step=["link_trip"], default=None
    )

    # Tour level assignment fields
    is_primary_dest_trip: bool | None = step_field(
        created_in_step=["extract_tours"], default=None
    )

class TourModel(BaseModel):
    """Tour-level records with clear, descriptive step_field names."""

    tour_id: int = step_field(
        ge=1, unique=True, created_in_step="extract_tours"
    )
    person_id: int = step_field(ge=1, created_in_step="extract_tours")
    day_id: int = step_field(ge=1, created_in_step="extract_tours")
    tour_sequence_num: int = step_field(ge=1)
    tour_category: str  # 'home_based' or 'work_based'
    parent_tour_id: int | None = step_field(
        ge=1, created_in_step="extract_tours", default=None
    )

    # Purpose and priority
    primary_purpose: int = step_field(ge=1, created_in_step="extract_tours")
    primary_dest_purpose: int = step_field(ge=1, created_in_step="extract_tours")  # noqa: E501
    purpose_priority: int = step_field(ge=1, created_in_step="extract_tours")

    # Timing
    origin_depart_time: datetime = step_field(created_in_step="extract_tours")
    dest_arrive_time: datetime = step_field(created_in_step="extract_tours")
    dest_depart_time: datetime = step_field(created_in_step="extract_tours")
    origin_arrive_time: datetime = step_field(created_in_step="extract_tours")

    # Locations
    o_lat: float = step_field(ge=-90, le=90, created_in_step="extract_tours")
    o_lon: float = step_field(ge=-180, le=180, created_in_step="extract_tours")
    d_lat: float = step_field(ge=-90, le=90, created_in_step="extract_tours")
    d_lon: float = step_field(ge=-180, le=180, created_in_step="extract_tours")
    o_location_type: str = step_field(created_in_step="extract_tours")  # 'home', 'work', 'school', 'other'  # noqa: E501
    d_location_type: str = step_field(created_in_step="extract_tours")

    # Mode hierarchical
    tour_mode: ModeType = step_field(created_in_step="extract_tours")
    outbound_mode: int = step_field(ge=1, created_in_step="extract_tours")
    inbound_mode: int = step_field(ge=1, created_in_step="extract_tours")

    # Stops
    num_outbound_stops: int = step_field(ge=0, created_in_step="extract_tours")
    num_inbound_stops: int = step_field(ge=0, created_in_step="extract_tours")

    # Flags
    is_primary_tour: bool = step_field(created_in_step="extract_tours")
    tour_starts_at_origin: bool = step_field(created_in_step="extract_tours")
    tour_ends_at_origin: bool = step_field(created_in_step="extract_tours")
