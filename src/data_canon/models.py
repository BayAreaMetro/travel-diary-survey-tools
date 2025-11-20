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
from .codebook.persons import (
    AgeCategory,
    Employment,
    PersonType,
    SchoolType,
    Student,
)
from .codebook.trips import (
    ModeType,
    Purpose,
    PurposeCategory,
)


# Data Models ------------------------------------------------------------------
class HouseholdModel(BaseModel):
    """Household attributes (minimal for tour building)."""

    hh_id: int = step_field(
        ge=1, unique=True,
        required_in_steps=["extract_tours"]
        )
    home_lat: float = step_field(
        ge=-90, le=90,
        required_in_steps=["extract_tours"]
    )
    home_lon: float = step_field(
        ge=-180, le=180,
        required_in_steps=["extract_tours"]
    )


class PersonModel(BaseModel):
    """Person attributes for tour building."""

    person_id: int = step_field(
        ge=1, unique=True,
        required_in_steps=["extract_tours"]
    )
    hh_id: int = step_field(
        ge=1,
        fk_to="households.hh_id",
        required_child=True,
    )
    age: AgeCategory = step_field(required_in_steps=["extract_tours"])
    work_lat: float | None = step_field(
        ge=-90, le=90,
        required_in_steps=["extract_tours"]
    )
    work_lon: float | None = step_field(
        ge=-180, le=180,
        required_in_steps=["extract_tours"]
    )
    school_lat: float | None = step_field(
        ge=-90, le=90,
        required_in_steps=["extract_tours"]
    )
    school_lon: float | None = step_field(
        ge=-180, le=180,
        required_in_steps=["extract_tours"]
    )
    person_type: PersonType = step_field(required_in_steps=["extract_tours"])
    employment: Employment = step_field(required_in_steps=["extract_tours"])
    student: Student = step_field(required_in_steps=["extract_tours"])
    school_type: SchoolType | None = step_field(
        required_in_steps=["extract_tours"],
    )


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = step_field(
        ge=1,
        fk_to="persons.person_id",
        required_child=True,
    )
    day_id: int = step_field(ge=1, unique=True)
    hh_id: int = step_field(ge=1, fk_to="households.hh_id")
    travel_dow: TravelDow


class UnlinkedTripModel(BaseModel):
    """Trip data model for validation."""

    trip_id: int = step_field(ge=1, unique=True)
    day_id: int = step_field(
        ge=1, fk_to="days.day_id"
    )
    person_id: int = step_field(
        ge=1, fk_to="persons.person_id"
    )
    hh_id: int = step_field(
        ge=1, fk_to="households.hh_id"
    )
    linked_trip_id: int = step_field(
        ge=1,
        fk_to="linked_trips.linked_trip_id",
        required_in_steps=["extract_tours"],
    )
    tour_id: int = step_field(
        ge=1,
        fk_to="tours.tour_id",
        required_in_steps=["extract_tours"],
    )
    depart_date: datetime
    depart_hour: int = step_field(ge=0, le=23)
    depart_minute: int = step_field(ge=0, le=59)
    depart_seconds: int = step_field(ge=0, le=59)
    arrive_date: datetime
    arrive_hour: int = step_field(ge=0, le=23)
    arrive_minute: int = step_field(ge=0, le=59)
    arrive_seconds: int = step_field(ge=0, le=59)
    o_lon: float = step_field(ge=-180, le=180, required_in_steps=["link_trip"])
    o_lat: float = step_field(ge=-90, le=90, required_in_steps=["link_trip"])
    d_lon: float = step_field(ge=-180, le=180, required_in_steps=["link_trip"])
    d_lat: float = step_field(ge=-90, le=90, required_in_steps=["link_trip"])
    o_purpose: Purpose
    d_purpose: Purpose
    o_purpose_category: PurposeCategory = step_field(
        required_in_steps=["link_trip"]
    )
    d_purpose_category: PurposeCategory = step_field(
        required_in_steps=["link_trip"]
    )
    mode_type: ModeType = step_field(required_in_steps=["link_trip"])
    duration_minutes: float = step_field(ge=0)
    distance_miles: float = step_field(ge=0)

    depart_time: datetime | None = step_field(
        required_in_steps=["link_trip", "extract_tours"]
    )
    arrive_time: datetime | None = step_field(
        required_in_steps=["link_trip", "extract_tours"]
    )


class LinkedTripModel(BaseModel):
    """Linked Trip data model for validation."""

    day_id: int = step_field(
        ge=1, fk_to="days.day_id",
        required_in_steps=["extract_tours"]
    )
    person_id: int = step_field(
        ge=1, fk_to="persons.person_id"
    )
    hh_id: int = step_field(
        ge=1, fk_to="households.hh_id"
    )

    linked_trip_id: int = step_field(ge=1, unique=True)
    tour_id: int = step_field(
        ge=1,
        fk_to="tours.tour_id"
    )

    depart_date: datetime = step_field()
    depart_hour: int = step_field(ge=0, le=23)
    depart_minute: int = step_field(ge=0, le=59)
    depart_seconds: int = step_field(ge=0, le=59)
    arrive_date: datetime = step_field()
    arrive_hour: int = step_field(ge=0, le=23)
    arrive_minute: int = step_field(ge=0, le=59)
    arrive_seconds: int = step_field(ge=0, le=59)
    o_purpose_category: int = step_field()
    d_purpose_category: int = step_field(required_in_steps=["extract_tours"])
    mode_type: ModeType = step_field(required_in_steps=["extract_tours"])
    duration_minutes: float = step_field(ge=0)
    distance_miles: float = step_field(ge=0)
    depart_time: datetime = step_field()
    arrive_time: datetime = step_field()


class TourModel(BaseModel):
    """Tour-level records with clear, descriptive step_field names."""

    tour_id: int = step_field(ge=1, unique=True)
    person_id: int = step_field(ge=1, fk_to="persons.person_id")
    day_id: int = step_field(ge=1, fk_to="days.day_id")
    tour_sequence_num: int = step_field(ge=1)
    tour_category: str  # 'home_based' or 'work_based'
    parent_tour_id: int | None = step_field(
        ge=1,
        fk_to="tours.tour_id",
        default=None
    )

    # Purpose and priority
    primary_purpose: int = step_field(ge=1)
    primary_dest_purpose: int = step_field(ge=1)
    purpose_priority: int = step_field(ge=1)

    # Timing
    origin_depart_time: datetime = step_field()
    dest_arrive_time: datetime = step_field()
    dest_depart_time: datetime = step_field()
    origin_arrive_time: datetime = step_field()

    # Locations
    o_lat: float = step_field(ge=-90, le=90)
    o_lon: float = step_field(ge=-180, le=180)
    d_lat: float = step_field(ge=-90, le=90)
    d_lon: float = step_field(ge=-180, le=180)
    o_location_type: str = step_field()  # 'home', 'work', 'school', 'other'
    d_location_type: str = step_field()

    # Mode hierarchical
    tour_mode: ModeType = step_field()
    outbound_mode: int = step_field(ge=1)
    inbound_mode: int = step_field(ge=1)

    # Stops
    num_outbound_stops: int = step_field(ge=0)
    num_inbound_stops: int = step_field(ge=0)

    # Flags
    is_primary_tour: bool = step_field()
    tour_starts_at_origin: bool = step_field()
    tour_ends_at_origin: bool = step_field()
