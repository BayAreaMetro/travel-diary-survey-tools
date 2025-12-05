"""Tour aggregation helper functions for tour extraction.

This module contains functions for:
- Aggregating trips to tour-level records
- Computing tour attributes (purpose, mode, timing)
- Assigning half-tour classification
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import (
    HalfTour,
    TourBoundary,
    TourType,
)
from utils.helpers import expr_haversine

from .priority_utils import (
    add_activity_duration_column,
    add_mode_priority_column,
    add_purpose_priority_column,
)
from .tour_configs import TourConfig

logger = logging.getLogger(__name__)


def _calculate_tour_purp_and_dest(
    linked_trips: pl.DataFrame,
    config: TourConfig,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Calculate tour purpose and primary destination from trip data.

    Determines tour purpose from the highest priority non-last trip, with
    activity duration as a tie-breaker. Returns enhanced trip data with
    purpose priorities and primary destination coordinates.

    Args:
        linked_trips: Trip data with tour_num and subtour_num
        config: TourConfig with purpose hierarchy

    Returns:
        Tuple of (enhanced_linked_trips, tour_purp_and_coords):
        - enhanced_linked_trips: All trips with _agg_key, priorities, and flags
        - tour_purp_and_coords: Aggregated tour purpose and destination coords
    """
    # Create hierarchical tour_id as aggregation key
    linked_trips = linked_trips.with_columns(
        [
            (
                pl.col("day_id").cast(pl.Utf8)
                + pl.col("tour_num").cast(pl.Utf8).str.pad_start(2, "0")
                + pl.col("subtour_num").cast(pl.Utf8).str.pad_start(2, "0")
            )
            .cast(pl.Int64)
            .alias("_agg_key"),
        ]
    )

    # Add priorities and activity duration for selection logic
    linked_trips = add_purpose_priority_column(
        linked_trips, config, alias="_purpose_priority"
    )
    linked_trips = add_mode_priority_column(
        linked_trips, config.mode_hierarchy, alias="_mode_priority"
    )
    linked_trips = add_activity_duration_column(
        linked_trips,
        config.default_activity_duration_minutes,
        alias="_activity_duration",
    )

    # Mark last trip (excluded from purpose selection)
    linked_trips = linked_trips.with_columns(
        [
            (
                pl.col("linked_trip_id").rank("ordinal").over("_agg_key")
                == pl.col("linked_trip_id").count().over("_agg_key")
            ).alias("_is_last_trip"),
        ]
    )

    # Determine tour purpose and primary destination from non-last trips
    non_last = linked_trips.filter(~pl.col("_is_last_trip")).sort(
        ["_agg_key", "_purpose_priority", "_activity_duration"],
        descending=[False, False, True],
    )

    tour_purp_and_coords = non_last.group_by(
        "_agg_key", maintain_order=True
    ).agg(
        [
            pl.col("d_purpose_category").first().alias("tour_purpose"),
            pl.col("d_lat").first().alias("_primary_d_lat"),
            pl.col("d_lon").first().alias("_primary_d_lon"),
            pl.col("d_location_type").first().alias("_primary_d_type"),
        ]
    )

    linked_trips = linked_trips.join(
        tour_purp_and_coords, on="_agg_key", how="left"
    )

    return linked_trips, tour_purp_and_coords


def _calculate_destination_times(
    linked_trips: pl.DataFrame,
    config: TourConfig,
) -> pl.DataFrame:
    """Calculate arrival and departure times at primary destination.

    Uses distance thresholds based on location type to identify when trips
    arrive at or depart from the primary destination.

    Args:
        linked_trips: Enhanced trip data with primary destination coordinates
        config: TourConfig with distance thresholds

    Returns:
        DataFrame with dest_arrive_time, dest_depart_time, and
        dest_linked_trip_id per _agg_key
    """
    # Calculate distances to primary destination and apply thresholds
    linked_trips = linked_trips.with_columns(
        [
            expr_haversine(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("_primary_d_lat"),
                pl.col("_primary_d_lon"),
            ).alias("_dist_d_to_primary"),
            expr_haversine(
                pl.col("o_lat"),
                pl.col("o_lon"),
                pl.col("_primary_d_lat"),
                pl.col("_primary_d_lon"),
            ).alias("_dist_o_to_primary"),
            pl.when(pl.col("_primary_d_type") == LocationType.HOME)
            .then(pl.lit(config.distance_thresholds[LocationType.HOME]))
            .when(pl.col("_primary_d_type") == LocationType.WORK)
            .then(pl.lit(config.distance_thresholds[LocationType.WORK]))
            .when(pl.col("_primary_d_type") == LocationType.SCHOOL)
            .then(pl.lit(config.distance_thresholds[LocationType.SCHOOL]))
            .otherwise(pl.lit(config.distance_thresholds[LocationType.HOME]))
            .alias("_threshold"),
        ]
    ).with_columns(
        [
            (pl.col("_dist_d_to_primary") <= pl.col("_threshold")).alias(
                "_arrives_at_primary"
            ),
            (pl.col("_dist_o_to_primary") <= pl.col("_threshold")).alias(
                "_departs_from_primary"
            ),
        ]
    )

    # Aggregate arrive times (exclude last trip) and depart times (all trips)
    dest_times = (
        linked_trips.filter(
            ~pl.col("_is_last_trip") & pl.col("_arrives_at_primary")
        )
        .group_by("_agg_key")
        .agg(
            [
                pl.col("arrive_time").max().alias("dest_arrive_time"),
                pl.col("linked_trip_id").max().alias("dest_linked_trip_id"),
            ]
        )
        .join(
            linked_trips.filter(pl.col("_departs_from_primary"))
            .group_by("_agg_key")
            .agg(pl.col("depart_time").max().alias("dest_depart_time")),
            on="_agg_key",
            how="full",
            coalesce=True,
        )
    )

    return dest_times


def _aggregate_and_classify_tours(
    linked_trips: pl.DataFrame,
    tour_purpose_and_coords: pl.DataFrame,
    dest_times: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate trip data to tour level and classify tour categories.

    Groups trips by tour and calculates tour-level attributes including mode,
    timing, locations, and counts. Classifies tours as work-based subtours or
    by boundary type (complete, partial start/end/both).

    Args:
        linked_trips: Enhanced trip data with priorities and flags
        tour_purpose_and_coords: Tour purpose and destination coordinates
        dest_times: Destination arrival/departure times

    Returns:
        Tour-level DataFrame with all attributes and classifications
    """
    tours = linked_trips.group_by("_agg_key").agg(
        [
            # Identifiers
            pl.col("_agg_key").first().alias("tour_id"),
            pl.col("person_id").first(),
            pl.col("hh_id").first(),
            pl.col("day_id").first(),
            pl.col("tour_num").first(),
            pl.col("subtour_num").first(),
            pl.col("parent_tour_id").first(),
            pl.col("linked_trip_id").first().alias("origin_linked_trip_id"),
            # Tour mode (highest priority)
            pl.col("mode_type")
            .sort_by("_mode_priority")
            .last()
            .alias("tour_mode"),
            # Origin timing and locations
            pl.col("depart_time").min().alias("origin_depart_time"),
            pl.col("arrive_time").max().alias("origin_arrive_time"),
            pl.col("o_lat").first(),
            pl.col("o_lon").first(),
            pl.col("d_lat").last(),
            pl.col("d_lon").last(),
            pl.col("o_location_type").first(),
            pl.col("d_location_type").last(),
            # Counts
            pl.col("linked_trip_id").count().alias("trip_count"),
            (pl.col("linked_trip_id").count() - 1).alias("stop_count"),
            # Flags for classification
            pl.col("subtour_num").first().alias("_subtour_num"),
            pl.col("o_is_home").first().alias("_o_is_home"),
            pl.col("d_is_home").last().alias("_d_is_home"),
        ]
    )

    # Join purpose and destination timing
    tours = tours.join(
        tour_purpose_and_coords.select(["_agg_key", "tour_purpose"]),
        on="_agg_key",
        how="left",
    ).join(dest_times, on="_agg_key", how="left")

    # Classify tour category and clean up
    tours = (
        tours.with_columns(
            [
                pl.when(pl.col("_subtour_num") > 0)
                .then(pl.lit(TourType.WORK_BASED))
                .when(pl.col("_o_is_home") & pl.col("_d_is_home"))
                .then(pl.lit(TourBoundary.COMPLETE))
                .when(pl.col("_o_is_home") & ~pl.col("_d_is_home"))
                .then(pl.lit(TourBoundary.PARTIAL_END))
                .when(~pl.col("_o_is_home") & pl.col("_d_is_home"))
                .then(pl.lit(TourBoundary.PARTIAL_START))
                .otherwise(pl.lit(TourBoundary.PARTIAL_BOTH))
                .alias("tour_category"),
            ]
        )
        .drop(["_agg_key", "_subtour_num", "_o_is_home", "_d_is_home"])
        .sort(["person_id", "day_id", "origin_depart_time"])
    )

    return tours


def aggregate_tours(
    linked_trips: pl.DataFrame,
    config: TourConfig,
) -> pl.DataFrame:
    """Aggregate trip data to tour-level records with attributes.

    Calculates tour attributes from trip data:
    - Tour purpose: Highest priority destination, with duration tie-breaker
      (When priorities equal, selects trip with longest activity duration)
    - Tour mode: Highest priority trip mode
    - Timing: First departure and last arrival
    - Counts: Number of trips and stops

    IMPORTANT: Work-based subtours are aggregated separately with their own
    tour_id (which includes subtour_num in the last 2 digits). The final output
    includes both home-based tours and work-based subtours as separate records.

    Args:
        linked_trips: Linked trips with tour_num and subtour_num assignments
        config: TourConfig object with priority settings

    Returns:
        Tour-level DataFrame with aggregated attributes (includes
        dest_arrive_time and dest_depart_time for half-tour classification)
    """
    logger.info("Aggregating tour data...")

    # Calculate tour purpose and primary destination
    linked_trips, tour_purp_and_coords = _calculate_tour_purp_and_dest(
        linked_trips, config
    )

    # Calculate destination arrival/departure times
    dest_times = _calculate_destination_times(linked_trips, config)

    # Aggregate to tour level and classify
    tours = _aggregate_and_classify_tours(
        linked_trips, tour_purp_and_coords, dest_times
    )

    logger.info("Aggregated %d tours", len(tours))
    return tours


def assign_half_tour(
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> pl.DataFrame:
    """Assign half-tour classification based on primary destination.

    Classifies each trip as:
    - OUTBOUND: Trips before first arrival at primary destination
    - INBOUND: Trips after final departure from primary destination
    - SUBTOUR: Work-based subtour trips

    Args:
        linked_trips: Linked trips with tour_id assignments
        tours: Tour table with dest_arrive_time and dest_depart_time

    Returns:
        Linked trips with half_tour_type (HalfTour enum) column added
    """
    logger.info("Assigning half-tour classification...")

    # Join destination times from tours table
    # tour_id already matches between linked_trips and tours
    linked_trips = linked_trips.join(
        tours.select(
            [
                "hh_id",
                "person_id",
                "day_id",
                "tour_num",
                "dest_arrive_time",
                "dest_depart_time",
            ]
        ),
        on=["day_id", "tour_num"],
        how="left",
    )

    # Classify half-tour type based on trip timing relative to
    # primary destination arrival/departure
    linked_trips = linked_trips.with_columns(
        [
            # Subtours are identified by subtour_num > 0
            pl.when(pl.col("subtour_num") > 0)
            .then(pl.lit(HalfTour.SUBTOUR))
            # Outbound: trip arrives before or at first arrival at primary dest
            .when(pl.col("arrive_time") <= pl.col("dest_arrive_time"))
            .then(pl.lit(HalfTour.OUTBOUND))
            # Inbound: trip departs after final departure from primary dest
            .when(pl.col("depart_time") >= pl.col("dest_depart_time"))
            .then(pl.lit(HalfTour.INBOUND))
            # Default to outbound if times are null (shouldn't happen)
            .otherwise(pl.lit(HalfTour.OUTBOUND))
            .alias("half_tour_type"),
        ]
    )

    # Clean up temporary columns
    linked_trips = linked_trips.drop(
        [
            "dest_arrive_time",
            "dest_depart_time",
        ]
    )

    logger.info("Half-tour classification complete")
    return linked_trips
