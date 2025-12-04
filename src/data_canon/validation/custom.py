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

from pipeline.utils.helpers import expr_haversine

# Registry of custom validators
# Format: {table_name: [check_function1, check_function2, ...]}  # noqa: ERA001
# Each check function should return list[str] of error messages
CUSTOM_VALIDATORS: dict[str, list[Callable]] = {
    "unlinked_trips": [],  # Registered checks defined below
    "linked_trips": [],  # Registered checks defined below
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


def check_for_teleports(unlinked_trips: pl.DataFrame) -> list[str]:
    """Check for when trip destination is too far from next trip origin."""
    errors = []
    max_distance = 1000  # Define threshold distance in meters

    # Compare o_lat/o_lon of the next trip to d_lat/d_lon of current trip
    # Compute distance, and compare to threshold over person_id and day_id
    teleports = (
        unlinked_trips.with_columns(
            pl.col("d_lat").alias("current_d_lat"),
            pl.col("d_lon").alias("current_d_lon"),
            pl.col("o_lat")
            .shift(-1)
            .over(["person_id", "day_id"])
            .alias("next_o_lat"),
            pl.col("o_lon")
            .shift(-1)
            .over(["person_id", "day_id"])
            .alias("next_o_lon"),
        )
        .with_columns(
            expr_haversine(
                pl.col("current_d_lat"),
                pl.col("current_d_lon"),
                pl.col("next_o_lat"),
                pl.col("next_o_lon"),
            ).alias("distance_meters")
        )
        .filter(pl.col("distance_meters") > max_distance)
        .select(
            pl.col("trip_id"),
            pl.col("person_id"),
            pl.col("day_id"),
            pl.col("distance_meters"),
        )
    )

    if len(teleports) > 0:
        trip_ids = teleports["trip_id"].to_list()[:5]
        errors.append(
            f"Found {len(teleports)} trips where destination "
            f"is more than {max_distance}m away from next trip origin. "
            f"Sample trip IDs: {trip_ids}"
        )
    return errors
