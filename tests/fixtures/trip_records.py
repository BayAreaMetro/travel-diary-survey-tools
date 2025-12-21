"""Trip record builders for unlinked, linked, and processed trips.

This module provides builders for all trip types in the canonical format.
Uses field_utils for simplified purpose field resolution.
"""

from datetime import UTC, datetime, time, timedelta

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import (
    AccessEgressMode,
    Driver,
    Mode,
    ModeType,
    Purpose,
    PurposeCategory,
)

from .field_utils import add_optional_fields_batch, resolve_field_with_fallback


def _default_times(
    depart_time, arrive_time, default_depart_hour=8, travel_minutes=30
):
    """Set default departure and arrival times if not provided."""
    if depart_time is None:
        depart_time = datetime.combine(
            datetime.now(tz=UTC).date(), time(default_depart_hour, 0)
        )
    if arrive_time is None:
        arrive_time = depart_time + timedelta(minutes=travel_minutes)
    return depart_time, arrive_time


def create_unlinked_trip(
    trip_id: int = 10001,
    _linked_trip_id: int | None = None,
    person_id: int = 101,
    hh_id: int = 1,
    day_id: int = 1,
    person_num: int = 1,
    day_num: int = 1,
    o_taz: int = 100,
    d_taz: int = 200,
    o_maz: int | None = None,
    d_maz: int | None = None,
    o_lat: float | None = None,
    o_lon: float | None = None,
    d_lat: float | None = None,
    d_lon: float | None = None,
    depart_time: datetime | None = None,
    arrive_time: datetime | None = None,
    travel_time: int = 30,
    duration_minutes: float | None = None,
    distance_meters: float = 1000.0,
    trip_weight: float = 1.0,
    transit_access: int = 0,
    transit_egress: int = 0,
    travel_dow: TravelDow = TravelDow.MONDAY,
    mode: Mode = Mode.HOUSEHOLD_VEHICLE,
    mode_type: ModeType = ModeType.CAR,
    o_purpose: Purpose | None = None,
    o_purpose_category: PurposeCategory | None = None,
    d_purpose: Purpose | None = None,
    d_purpose_category: PurposeCategory | None = None,
    purpose: Purpose | None = None,
    purpose_category: PurposeCategory | None = None,
    driver: Driver = Driver.DRIVER,
    access_mode: AccessEgressMode | None = None,
    egress_mode: AccessEgressMode | None = None,
    num_travelers: int = 1,
    change_mode: bool = False,
    **overrides,
) -> dict:
    """Create an unlinked trip record (raw trip segment before linking).

    Unlinked trips represent individual trip segments as reported by the
    traveler, before they are linked into journeys and organized into tours.
    These are the inputs to the link_trips processing step.

    Args:
        trip_id: Trip ID
        person_id: Person ID
        hh_id: Household ID
        day_id: Day ID (links trip to a specific day)
        person_num: Person number within household
        day_num: Day number in survey period
        o_taz: Origin TAZ
        d_taz: Destination TAZ
        o_maz: Origin MAZ (optional, for Daysim)
        d_maz: Destination MAZ (optional, for Daysim)
        o_lat: Origin latitude (optional)
        o_lon: Origin longitude (optional)
        d_lat: Destination latitude (optional)
        d_lon: Destination longitude (optional)
        depart_time: Departure time (defaults to 8 AM)
        arrive_time: Arrival time (defaults to 8:30 AM)
        travel_time: Travel time in minutes
        duration_minutes: Trip duration in minutes (defaults to travel_time)
        distance_meters: Trip distance in meters
        trip_weight: Trip expansion weight
        transit_access: Transit access flag
        transit_egress: Transit egress flag
        travel_dow: Day of week enum
        mode: Specific mode enum
        mode_type: Mode type enum (car/transit/walk/bike)
        o_purpose: Origin purpose enum (deprecated, use o_purpose_category)
        o_purpose_category: Origin purpose category enum (for link_trips)
        d_purpose: Destination purpose enum (deprecated)
        d_purpose_category: Destination purpose category enum (for link_trips)
        purpose: Legacy purpose field enum (backward compatibility)
        purpose_category: Legacy purpose category enum (backward compat)
        driver: Driver status enum
        access_mode: Access mode enum for transit (optional)
        egress_mode: Egress mode enum for transit (optional)
        num_travelers: Number of travelers
        change_mode: Whether this is a change mode location (for linking)
        **overrides: Override any default values

    Returns:
        Complete unlinked trip record dict (no tour_id, trip_num,
        tour_direction)
    """
    # Default times if not provided
    depart_time, arrive_time = _default_times(
        depart_time,
        arrive_time,
        default_depart_hour=8,
        travel_minutes=travel_time,
    )

    record = {
        "trip_id": trip_id,
        "person_id": person_id,
        "hh_id": hh_id,
        "day_id": day_id,
        "person_num": person_num,
        "day_num": day_num,
        "o_taz": o_taz,
        "d_taz": d_taz,
        "depart_time": depart_time,
        "arrive_time": arrive_time,
        "travel_time": travel_time,
        "duration_minutes": (
            duration_minutes
            if duration_minutes is not None
            else float(travel_time)
        ),
        "distance_meters": distance_meters,
        "trip_weight": trip_weight,
        "transit_access": transit_access,
        "transit_egress": transit_egress,
        "travel_dow": travel_dow.value,
        "mode": mode.value,
        "mode_type": mode_type.value,
        "driver": driver.value,
        "num_travelers": num_travelers,
        "change_mode": change_mode,
    }

    # Add purpose fields using fallback resolution (simplified!)
    purpose_fields = {
        "o_purpose_category": resolve_field_with_fallback(
            ["o_purpose_category", "purpose_category"],
            o_purpose_category=(
                o_purpose_category.value if o_purpose_category else None
            ),
            purpose_category=(
                purpose_category.value if purpose_category else None
            ),
        ),
        "d_purpose_category": resolve_field_with_fallback(
            ["d_purpose_category", "purpose_category"],
            d_purpose_category=(
                d_purpose_category.value if d_purpose_category else None
            ),
            purpose_category=(
                purpose_category.value if purpose_category else None
            ),
        ),
        "o_purpose": resolve_field_with_fallback(
            ["o_purpose", "purpose"],
            o_purpose=o_purpose.value if o_purpose else None,
            purpose=purpose.value if purpose else None,
        ),
        "d_purpose": resolve_field_with_fallback(
            ["d_purpose", "purpose"],
            d_purpose=d_purpose.value if d_purpose else None,
            purpose=purpose.value if purpose else None,
        ),
    }

    # Legacy fields for backward compatibility
    if purpose is not None:
        purpose_fields["purpose"] = purpose.value
    if purpose_category is not None:
        purpose_fields["purpose_category"] = purpose_category.value

    add_optional_fields_batch(record, **purpose_fields)

    # Add optional MAZ fields
    add_optional_fields_batch(record, o_maz=o_maz, d_maz=d_maz)

    # Always include lat/lon fields (link_trips requires them even if None)
    record["o_lat"] = o_lat
    record["o_lon"] = o_lon
    record["d_lat"] = d_lat
    record["d_lon"] = d_lon

    # Add optional transit fields
    add_optional_fields_batch(
        record,
        access_mode=access_mode.value if access_mode else None,
        egress_mode=egress_mode.value if egress_mode else None,
    )

    return {**record, **overrides}


def create_linked_trip(
    linked_trip_id: int = 1,
    person_id: int = 101,
    hh_id: int = 1,
    person_num: int = 1,
    day_id: int | None = None,
    day_num: int = 1,
    travel_dow: TravelDow = TravelDow.MONDAY,
    linked_trip_num: int = 1,
    tour_id: int = 1,
    depart_time: datetime | None = None,
    arrive_time: datetime | None = None,
    origin_lat: float = 37.70,
    origin_lon: float = -122.40,
    origin_taz: int = 100,
    origin_maz: int = 1000,
    origin_purpose: PurposeCategory = PurposeCategory.HOME,
    dest_lat: float = 37.75,
    dest_lon: float = -122.45,
    dest_taz: int = 200,
    dest_maz: int = 2000,
    dest_purpose: PurposeCategory = PurposeCategory.WORK,
    mode: Mode = Mode.HOUSEHOLD_VEHICLE_1,
    mode_type: ModeType = ModeType.CAR,
    driver: Driver = Driver.DRIVER,
    num_travelers: int = 1,
    distance_meters: float = 8046.72,
    distance_miles: float | None = None,
    num_unlinked_trips: int = 1,
    tour_direction: int = 1,  # 1=OUTBOUND, 2=INBOUND
    access_mode: AccessEgressMode | None = None,
    egress_mode: AccessEgressMode | None = None,
    **overrides,
) -> dict:
    """Create a complete canonical linked trip record.

    Args:
        linked_trip_id: Linked trip ID
        person_id: Person ID
        hh_id: Household ID
        person_num: Person number
        day_id: Day ID (optional)
        day_num: Day number
        travel_dow: Day of week enum
        linked_trip_num: Linked trip number
        tour_id: Parent tour ID
        depart_time: Departure datetime
        arrive_time: Arrival datetime
        origin_lat: Origin latitude
        origin_lon: Origin longitude
        origin_taz: Origin TAZ
        origin_maz: Origin MAZ
        origin_purpose: Origin purpose enum
        dest_lat: Destination latitude
        dest_lon: Destination longitude
        dest_taz: Destination TAZ
        dest_maz: Destination MAZ
        dest_purpose: Destination purpose enum
        mode: Aggregated mode enum
        mode_type: Aggregated mode type enum
        driver: Driver status enum
        num_travelers: Number of travelers
        distance_meters: Trip distance in meters
        distance_miles: Trip distance in miles (optional)
        num_unlinked_trips: Number of component unlinked trips
        tour_direction: Tour direction (1=OUTBOUND, 2=INBOUND)
        access_mode: Transit access mode enum (for transit trips)
        egress_mode: Transit egress mode enum (for transit trips)
        **overrides: Override any default values

    Returns:
        Complete linked trip record dict
    """
    # Default times if not provided
    depart_time, arrive_time = _default_times(
        depart_time, arrive_time, default_depart_hour=8, travel_minutes=30
    )

    # Calculate distance_miles if not provided
    if distance_miles is None:
        distance_miles = distance_meters / 1609.34

    record = {
        "linked_trip_id": linked_trip_id,
        "person_id": person_id,
        "hh_id": hh_id,
        "person_num": person_num,
        "day_num": day_num,
        "travel_dow": travel_dow.value,
        "linked_trip_num": linked_trip_num,
        "tour_id": tour_id,
        "depart_time": depart_time,
        "arrive_time": arrive_time,
        "duration_minutes": int(
            (arrive_time - depart_time).total_seconds() / 60
        ),
        "o_lat": origin_lat,
        "o_lon": origin_lon,
        "o_taz": origin_taz,
        "o_maz": origin_maz,
        "o_purpose_category": origin_purpose.value,
        "d_lat": dest_lat,
        "d_lon": dest_lon,
        "d_taz": dest_taz,
        "d_maz": dest_maz,
        "d_purpose_category": dest_purpose.value,
        "mode": mode.value,
        "mode_type": mode_type.value,
        "driver": driver.value,
        "num_travelers": num_travelers,
        "distance_meters": distance_meters,
        "distance_miles": distance_miles,
        "num_unlinked_trips": num_unlinked_trips,
        "tour_direction": tour_direction,
        "linked_trip_weight": 1.0,
        # Always include access/egress mode fields (formatter expects them)
        "access_mode": access_mode.value if access_mode else None,
        "egress_mode": egress_mode.value if egress_mode else None,
    }

    # Add optional fields
    add_optional_fields_batch(record, day_id=day_id)

    return {**record, **overrides}


def create_trip(
    trip_id: int = 10001,
    tour_id: int = 1001,
    person_id: int = 101,
    hh_id: int = 1,
    trip_num: int = 1,
    tour_direction: TourDirection = TourDirection.OUTBOUND,
    o_taz: int = 100,
    d_taz: int = 200,
    o_maz: int | None = None,
    d_maz: int | None = None,
    o_location_type: LocationType = LocationType.HOME,
    d_location_type: LocationType = LocationType.WORK,
    depart_time: datetime | None = None,
    arrive_time: datetime | None = None,
    travel_time: int = 30,
    travel_dow: TravelDow = TravelDow.MONDAY,
    mode: Mode = Mode.MISSING,
    mode_type: ModeType = ModeType.CAR,
    purpose: Purpose = Purpose.PRIMARY_WORKPLACE,
    purpose_category: PurposeCategory = PurposeCategory.WORK,
    driver: Driver = Driver.DRIVER,
    access_mode: AccessEgressMode | None = None,
    egress_mode: AccessEgressMode | None = None,
    num_travelers: int = 1,
    **overrides,
) -> dict:
    """Create a complete canonical trip record.

    Args:
        trip_id: Trip ID
        tour_id: Parent tour ID
        person_id: Person ID
        hh_id: Household ID
        trip_num: Trip number within tour
        tour_direction: Outbound/inbound/subtour enum
        o_taz: Origin TAZ
        d_taz: Destination TAZ
        o_maz: Origin MAZ (optional, for Daysim)
        d_maz: Destination MAZ (optional, for Daysim)
        o_location_type: Origin location type enum
        d_location_type: Destination location type enum
        depart_time: Departure time (defaults to 8 AM)
        arrive_time: Arrival time (defaults to 8:30 AM)
        travel_time: Travel time in minutes
        travel_dow: Day of week enum
        mode: Specific mode enum
        mode_type: Mode type enum (car/transit/walk/bike)
        purpose: Trip purpose enum
        purpose_category: Purpose category enum
        driver: Driver status enum
        access_mode: Access mode enum for transit (optional)
        egress_mode: Egress mode enum for transit (optional)
        num_travelers: Number of travelers
        **overrides: Override any default values

    Returns:
        Complete trip record dict
    """
    # Default times if not provided
    depart_time, arrive_time = _default_times(
        depart_time,
        arrive_time,
        default_depart_hour=8,
        travel_minutes=travel_time,
    )

    record = {
        "trip_id": trip_id,
        "tour_id": tour_id,
        "person_id": person_id,
        "hh_id": hh_id,
        "trip_num": trip_num,
        "tour_direction": tour_direction.value,
        "o_taz": o_taz,
        "d_taz": d_taz,
        "o_location_type": o_location_type.value,
        "d_location_type": d_location_type.value,
        "depart_time": depart_time,
        "arrive_time": arrive_time,
        "travel_time": travel_time,
        "travel_dow": travel_dow.value,
        "mode": mode.value,
        "mode_type": mode_type.value,
        "purpose": purpose.value,
        "purpose_category": purpose_category.value,
        "driver": driver.value,
        "num_travelers": num_travelers,
    }

    # Add optional MAZ fields
    add_optional_fields_batch(record, o_maz=o_maz, d_maz=d_maz)

    # Add optional transit fields
    add_optional_fields_batch(
        record,
        access_mode=access_mode.value if access_mode else None,
        egress_mode=egress_mode.value if egress_mode else None,
    )

    return {**record, **overrides}
