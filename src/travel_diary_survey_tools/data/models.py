"""Data models for trip linking and tour building.

This module uses Pydantic for data validation. Models represent individual records
(rows) rather than entire DataFrames. Use the validate_* functions to validate
Polars DataFrames by iterating through rows.
"""

from datetime import datetime
from typing import Annotated

import polars as pl
from pydantic import BaseModel, Field, field_validator, model_validator


# Data Models ------------------------------------------------------------------
class CoordModel(BaseModel):
    """Coordinate data model for validation."""

    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)


class PersonModel(BaseModel):
    """Person attributes for tour building."""

    person_id: int = Field(ge=1)
    hh_id: int = Field(ge=1)
    person_type: int = Field(ge=1)
    age: int | None = Field(ge=0, default=None)
    home_lat: float = Field(ge=-90, le=90)
    home_lon: float = Field(ge=-180, le=180)
    work_lat: float | None = Field(ge=-90, le=90, default=None)
    work_lon: float | None = Field(ge=-180, le=180, default=None)
    school_lat: float | None = Field(ge=-90, le=90, default=None)
    school_lon: float | None = Field(ge=-180, le=180, default=None)


class HouseholdModel(BaseModel):
    """Household attributes (minimal for tour building)."""

    hh_id: int = Field(ge=1)
    home_lat: float = Field(ge=-90, le=90)
    home_lon: float = Field(ge=-180, le=180)


# Minimal data schema for trip linking
class TripModel(BaseModel):
    """Trip data model for validation."""

    trip_id: int = Field(ge=1)
    day_id: int = Field(ge=1)
    person_id: int = Field(ge=1)
    hh_id: int = Field(ge=1)
    depart_date: str
    depart_hour: int = Field(ge=0, le=23)
    depart_minute: int = Field(ge=0, le=59)
    depart_seconds: int = Field(ge=0, le=59)
    arrive_date: str
    arrive_hour: int = Field(ge=0, le=23)
    arrive_minute: int = Field(ge=0, le=59)
    arrive_seconds: int = Field(ge=0, le=59)
    o_purpose_category: int
    d_purpose_category: int
    mode_type: int
    duration_minutes: float | None = Field(ge=0, default=None)
    distance_miles: float | None = Field(ge=0, default=None)

    depart_time: datetime
    arrive_time: datetime

    @model_validator(mode="after")
    def arrival_after_departure(self) -> "TripModel":
        """Ensure arrive_time is after depart_time."""
        if self.arrive_time < self.depart_time:
            msg = f"arrive_time ({self.arrive_time}) must be >= depart_time ({self.depart_time})"
            raise ValueError(msg)
        return self


# Subclassing allows you to extend TripModel cleanly
class LinkedTripModel(BaseModel):
    """Linked Trip data model for validation."""

    linked_trip_id: int | None = Field(ge=1, default=None)
    day_id: int = Field(ge=1)
    person_id: int = Field(ge=1)
    hh_id: int = Field(ge=1)
    depart_date: str
    depart_hour: int = Field(ge=0, le=23)
    depart_minute: int = Field(ge=0, le=59)
    depart_seconds: int = Field(ge=0, le=59)
    arrive_date: str
    arrive_hour: int = Field(ge=0, le=23)
    arrive_minute: int = Field(ge=0, le=59)
    arrive_seconds: int = Field(ge=0, le=59)
    o_purpose_category: int
    d_purpose_category: int
    mode_type: int
    duration_minutes: float | None = Field(ge=0, default=None)
    distance_miles: float | None = Field(ge=0, default=None)
    depart_time: datetime
    arrive_time: datetime


class TourModel(BaseModel):
    """Tour-level records with clear, descriptive field names."""

    tour_id: int = Field(ge=1)
    person_id: int = Field(ge=1)
    day_id: int = Field(ge=1)
    tour_sequence_num: int = Field(ge=1)
    tour_category: str  # 'home_based' or 'work_based'
    parent_tour_id: int | None = Field(ge=1, default=None)

    # Purpose and priority
    primary_purpose: int = Field(ge=1)
    primary_dest_purpose: int = Field(ge=1)
    purpose_priority: int = Field(ge=1)

    # Timing
    origin_depart_time: datetime
    dest_arrive_time: datetime
    dest_depart_time: datetime
    origin_arrive_time: datetime

    # Locations
    o_lat: float = Field(ge=-90, le=90)
    o_lon: float = Field(ge=-180, le=180)
    d_lat: float = Field(ge=-90, le=90)
    d_lon: float = Field(ge=-180, le=180)
    o_location_type: str  # 'home', 'work', 'school', 'other'
    d_location_type: str

    # Mode (hierarchical)
    tour_mode: int = Field(ge=1)
    outbound_mode: int = Field(ge=1)
    inbound_mode: int = Field(ge=1)

    # Stops
    num_outbound_stops: int = Field(ge=0)
    num_inbound_stops: int = Field(ge=0)

    # Flags
    is_primary_tour: bool
    tour_starts_at_origin: bool
    tour_ends_at_origin: bool


class PersonDayModel(BaseModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: int = Field(ge=1)
    day_id: int = Field(ge=1)
    primary_tour_id: int | None = Field(ge=1, default=None)


class TourTripModel(BaseModel):
    """Trip records with tour assignments using clear field names."""

    # Tour assignment
    tour_id: int = Field(ge=1)
    tour_sequence_num: int = Field(ge=1)
    half_tour: str  # 'outbound' or 'inbound'
    stop_sequence: int = Field(ge=1)  # Within half-tour
    
    # Flags
    is_tour_origin_trip: bool  # First trip of tour
    is_tour_dest_trip: bool  # Last trip to primary destination
    is_tour_return_trip: bool  # Last trip of tour
    
    # Trip details (from LinkedTripModel)
    linked_trip_id: int | None = Field(ge=1, default=None)
    day_id: int = Field(ge=1)
    person_id: int = Field(ge=1)
    hh_id: int = Field(ge=1)
    depart_date: str
    depart_hour: int = Field(ge=0, le=23)
    depart_minute: int = Field(ge=0, le=59)
    depart_seconds: int = Field(ge=0, le=59)
    arrive_date: str
    arrive_hour: int = Field(ge=0, le=23)
    arrive_minute: int = Field(ge=0, le=59)
    arrive_seconds: int = Field(ge=0, le=59)
    o_purpose_category: int
    d_purpose_category: int
    mode_type: int
    duration_minutes: float | None = Field(ge=0, default=None)
    distance_miles: float | None = Field(ge=0, default=None)
    depart_time: datetime
    arrive_time: datetime

