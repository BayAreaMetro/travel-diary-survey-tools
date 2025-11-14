"""Custom validation checks for travel survey data.

This module contains DataFrame-level validation checks that run during the
custom validator phase (after row-level validation). Users can add checks here
and register them to tables using CUSTOM_VALIDATORS.

To add a new check:
1. Define a function that takes one or more DataFrames and returns list[str]
2. Add it to CUSTOM_VALIDATORS dict below, mapping table name to check functions
3. The check will automatically run when that table is validated
"""

from collections.abc import Callable

import polars as pl

# Registry of custom validators
# Format: {table_name: [check_function1, check_function2, ...]}
# Each check function should return list[str] of error messages
CUSTOM_VALIDATORS: dict[str, list[Callable]] = {
    "unlinked_trips": [],  # Registered checks defined below
    "linked_trips": [],    # Registered checks defined below
}


# Example check functions below:

def check_arrival_after_departure(unlinked_trips: pl.DataFrame) -> list[str]:
    """Ensure arrive_time is after depart_time for all trips.

    Args:
        unlinked_trips: DataFrame with trip records

    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []
    bad_trips = unlinked_trips.filter(
        pl.col("arrive_time") < pl.col("depart_time")
    )
    if len(bad_trips) > 0:
        trip_ids = bad_trips["trip_id"].to_list()[:5]
        errors.append(
            f"Found {len(bad_trips)} trips where arrive_time < depart_time. "
            f"Sample trip IDs: {trip_ids}"
        )
    return errors


def check_coordinate_bounds(
    latitude: float,
    longitude: float,
    min_lat: float = -90.0,
    max_lat: float = 90.0,
    min_lon: float = -180.0,
    max_lon: float = 180.0,
) -> None:
    """Ensure coordinates are within valid bounds.

    Args:
        latitude: Latitude value
        longitude: Longitude value
        min_lat: Minimum allowed latitude (default: -90.0)
        max_lat: Maximum allowed latitude (default: 90.0)
        min_lon: Minimum allowed longitude (default: -180.0)
        max_lon: Maximum allowed longitude (default: 180.0)

    Raises:
        ValueError: If coordinates are outside bounds
    """
    if not (min_lat <= latitude <= max_lat):
        msg = f"latitude ({latitude}) must be between {min_lat} and {max_lat}"
        raise ValueError(msg)

    if not (min_lon <= longitude <= max_lon):
        msg = (
            f"longitude ({longitude}) must be between "
            f"{min_lon} and {max_lon}"
        )
        raise ValueError(msg)


