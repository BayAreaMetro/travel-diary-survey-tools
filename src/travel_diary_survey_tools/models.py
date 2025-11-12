"""Data models for trip linking and tour building."""

import pandera.polars as pa
import polars as pl
import pydantic as pd


# Data Models ------------------------------------------------------------------
class CoordModel(pd.BaseModel):
    """Coordinate data model for validation."""

    latitude: float = pd.Field(ge=-90.0, le=90.0)
    longitude: float = pd.Field(ge=-180.0, le=180.0)


class PersonModel(pa.DataFrameModel):
    """Person attributes for tour building."""

    person_id: pl.Int64 = pa.Field(ge=1)
    hh_id: pl.Int64 = pa.Field(ge=1)
    person_type: pl.Int64 = pa.Field(ge=1)
    age: pl.Int64 = pa.Field(ge=0, nullable=True)
    home_lat: pl.Float64 = pa.Field(ge=-90, le=90)
    home_lon: pl.Float64 = pa.Field(ge=-180, le=180)
    work_lat: pl.Float64 = pa.Field(ge=-90, le=90, nullable=True)
    work_lon: pl.Float64 = pa.Field(ge=-180, le=180, nullable=True)
    school_lat: pl.Float64 = pa.Field(ge=-90, le=90, nullable=True)
    school_lon: pl.Float64 = pa.Field(ge=-180, le=180, nullable=True)


class HouseholdModel(pa.DataFrameModel):
    """Household attributes (minimal for tour building)."""

    hh_id: pl.Int64 = pa.Field(ge=1)
    home_lat: pl.Float64 = pa.Field(ge=-90, le=90)
    home_lon: pl.Float64 = pa.Field(ge=-180, le=180)


# Minimal data schema for trip linking
class TripModel(pa.DataFrameModel):
    """Trip data model for validation."""

    trip_id: pl.Int64 = pa.Field(ge=1)
    day_id: pl.Int64 = pa.Field(ge=1)
    person_id: pl.Int64 = pa.Field(ge=1)
    hh_id: pl.Int64 = pa.Field(ge=1)
    depart_date: pl.Utf8
    depart_hour: pl.Int64 = pa.Field(ge=0, le=23)
    depart_minute: pl.Int64 = pa.Field(ge=0, le=59)
    depart_seconds: pl.Int64 = pa.Field(ge=0, le=59)
    arrive_date: pl.Utf8
    arrive_hour: pl.Int64 = pa.Field(ge=0, le=23)
    arrive_minute: pl.Int64 = pa.Field(ge=0, le=59)
    arrive_seconds: pl.Int64 = pa.Field(ge=0, le=59)
    o_purpose_category: pl.Int64
    d_purpose_category: pl.Int64
    mode_type: pl.Int64
    duration_minutes: pl.Float64 = pa.Field(ge=0, nullable=True, coerce=True)
    distance_miles: pl.Float64 = pa.Field(ge=0, nullable=True, coerce=True)

    depart_time: pl.Datetime = pa.Field(ge=0)
    arrive_time: pl.Datetime = pa.Field(ge=0)

    # Checks
    @pa.check("arrive_time")
    def arrival_after_departure(cls, data) -> pl.LazyFrame:  # noqa: N805, ANN001
        """Ensure arrive_time is after depart_time."""
        return data.lazyframe.select(
            pl.col("arrive_time").ge(pl.col("depart_time")).all(),
        )


# Subclassing allows you to extend TripModel cleanly
class LinkedTripModel(TripModel):
    """Linked Trip data model for validation."""

    trip_id: None
    linked_trip_id: pl.Int64 = pa.Field(ge=1, nullable=True)


class TourModel(pa.DataFrameModel):
    """Tour-level records with clear, descriptive field names."""

    tour_id: pl.Int64 = pa.Field(ge=1)
    person_id: pl.Int64 = pa.Field(ge=1)
    day_id: pl.Int64 = pa.Field(ge=1)
    tour_sequence_num: pl.Int64 = pa.Field(ge=1)
    tour_category: pl.Utf8  # 'home_based' or 'work_based'
    parent_tour_id: pl.Int64 = pa.Field(ge=1, nullable=True)

    # Purpose and priority
    primary_purpose: pl.Int64 = pa.Field(ge=1)
    primary_dest_purpose: pl.Int64 = pa.Field(ge=1)
    purpose_priority: pl.Int64 = pa.Field(ge=1)

    # Timing
    origin_depart_time: pl.Datetime
    dest_arrive_time: pl.Datetime
    dest_depart_time: pl.Datetime
    origin_arrive_time: pl.Datetime

    # Locations
    o_lat: pl.Float64 = pa.Field(ge=-90, le=90)
    o_lon: pl.Float64 = pa.Field(ge=-180, le=180)
    d_lat: pl.Float64 = pa.Field(ge=-90, le=90)
    d_lon: pl.Float64 = pa.Field(ge=-180, le=180)
    o_location_type: pl.Utf8  # 'home', 'work', 'school', 'other'
    d_location_type: pl.Utf8

    # Mode [hierarchical]  # noqa: ERA001
    tour_mode: pl.Int64 = pa.Field(ge=1)
    outbound_mode: pl.Int64 = pa.Field(ge=1)
    inbound_mode: pl.Int64 = pa.Field(ge=1)

    # Stops
    num_outbound_stops: pl.Int64 = pa.Field(ge=0)
    num_inbound_stops: pl.Int64 = pa.Field(ge=0)

    # Flags
    is_primary_tour: pl.Boolean
    tour_starts_at_origin: pl.Boolean
    tour_ends_at_origin: pl.Boolean


class PersonDayModel(pa.DataFrameModel):
    """Daily activity pattern summary with clear purpose-specific counts."""

    person_id: pl.Int64 = pa.Field(ge=1)
    day_id: pl.Int64 = pa.Field(ge=1)

    primary_tour_id: pl.Int64 = pa.Field(ge=1, nullable=True)


class TourTripModel(LinkedTripModel):
    """Trip records with tour assignments using clear field names."""

    tour_id: pl.Int64 = pa.Field(ge=1)
    tour_sequence_num: pl.Int64 = pa.Field(ge=1)
    half_tour: pl.Utf8  # 'outbound' or 'inbound'
    stop_sequence: pl.Int64 = pa.Field(ge=1)  # Within half-tour
    is_tour_origin_trip: pl.Boolean  # First trip of tour
    is_tour_dest_trip: pl.Boolean  # Last trip to primary destination
    is_tour_return_trip: pl.Boolean  # Last trip of tour
