"""Custom validation checks for travel survey data.

This module contains DataFrame-level validation checks that run during the
custom validator phase (after row-level validation). Users can add checks here
and register them to tables using CUSTOM_VALIDATORS.

These checks are for more complex validation logic that requires multiple tables
or spanning multiple rows. Most checks can be done using built-in validators,
or at the row-level.

To add a new check:
1. Define a function that takes one or more DataFrames and returns list[str]
2. Add it to CUSTOM_VALIDATORS dict below, mapping table name to check functions
3. The check will automatically run when that table is validated
"""

from collections.abc import Callable

import polars as pl

from data_canon.codebook.tours import TourDataQuality
from utils.helpers import expr_haversine

# Registry of custom validators
# Format: {table_name: [check_function1, check_function2, ...]}
# Each check function should return list[str] of error messages
CUSTOM_VALIDATORS: dict[str, list[Callable]] = {
    "households": [],
    "persons": [],
    "days": [],
    "unlinked_trips": [],
    "linked_trips": [],
    "tours": [],
}


# Example check functions below:
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
            pl.col("o_lat").shift(-1).over(["person_id", "day_id"]).alias("next_o_lat"),
            pl.col("o_lon").shift(-1).over(["person_id", "day_id"]).alias("next_o_lon"),
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
            pl.col("unlinked_trip_id"),
            pl.col("person_id"),
            pl.col("day_id"),
            pl.col("distance_meters"),
        )
    )

    if len(teleports) > 0:
        trip_ids = teleports["unlinked_trip_id"].to_list()[:5]
        errors.append(
            f"Found {len(teleports)} trips where destination "
            f"is more than {max_distance}m away from next trip origin. "
            f"Sample trip IDs: {trip_ids}"
        )
    return errors


def check_single_trip_tour_flag_consistency(
    tours: pl.DataFrame, linked_trips: pl.DataFrame
) -> list[str]:
    """Verify single_trip_tour flag matches actual trip count.

    This validates the business logic that sets the single_trip_tour flag.
    Tours with trip_count=1 should have single_trip_tour=True, and tours
    with trip_count>=2 should have single_trip_tour=False.

    Args:
        tours: Tour records with single_trip_tour flag
        linked_trips: Trip records to count per tour

    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []

    # Count trips per tour
    trip_counts = linked_trips.group_by("tour_id").agg(pl.len().alias("actual_trip_count"))

    # Join with tours and check consistency
    inconsistent = tours.join(trip_counts, on="tour_id", how="left").filter(
        # Flag says single-trip but has multiple trips
        (pl.col("single_trip_tour") & (pl.col("actual_trip_count") != 1))
        # Flag says multi-trip but has only one trip
        | (~pl.col("single_trip_tour") & (pl.col("actual_trip_count") == 1))
    )

    if len(inconsistent) > 0:
        tour_ids = inconsistent["tour_id"].to_list()[:5]
        errors.append(
            f"Found {len(inconsistent)} tours where single_trip_tour flag "
            f"doesn't match actual trip count. Sample tour IDs: {tour_ids}"
        )

    return errors


def check_valid_tours_are_complete(tours: pl.DataFrame) -> list[str]:
    """Verify tours flagged VALID are genuinely complete.

    A tour marked ``tour_data_quality == VALID`` must not be a single-trip tour
    and must have a non-null ``tour_purpose``. This guards the downstream
    formatters, which keep only VALID tours: a tour mislabeled VALID but missing
    its purpose would survive that filter and leak into the model output (in
    CT-RAMP, silently coerced to the OTHDISCR catch-all purpose).

    ``TourModel.validate_complete_tours`` enforces the converse (non-single-trip
    tours have a purpose); this check keys on ``tour_data_quality`` instead, so
    the two are complementary rather than redundant.

    Args:
        tours: Tour records with tour_data_quality, single_trip_tour, tour_purpose

    Returns:
        List of error messages (empty if validation passes)
    """
    errors = []

    # tour_data_quality is produced during extraction; skip if absent
    # (e.g. schema-only empty frames).
    if "tour_data_quality" not in tours.columns:
        return errors

    invalid = tours.filter(
        (pl.col("tour_data_quality") == TourDataQuality.VALID.value)
        & (pl.col("single_trip_tour") | pl.col("tour_purpose").is_null())
    )

    if len(invalid) > 0:
        tour_ids = invalid["tour_id"].to_list()[:5]
        errors.append(
            f"Found {len(invalid)} tours flagged VALID that are single-trip "
            f"or have a null tour_purpose. Sample tour IDs: {tour_ids}"
        )

    return errors


# Register the tour validators
CUSTOM_VALIDATORS["tours"].append(check_single_trip_tour_flag_consistency)
CUSTOM_VALIDATORS["tours"].append(check_valid_tours_are_complete)
