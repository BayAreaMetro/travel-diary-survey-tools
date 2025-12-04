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
from pipeline.utils import expr_haversine

from .priority_utils import (
    add_activity_duration_column,
    add_mode_priority_column,
    add_purpose_priority_column,
)
from .tour_configs import TourConfig

logger = logging.getLogger(__name__)


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

    # Create tour_id from tour_num and subtour_num
    # Hierarchical ID: day_id + tour_num + subtour_num
    # For parent tours subtour_num = 0, for subtours subtour_num > 0

    linked_trips_with_priority = linked_trips.with_columns(
        [
            (
                pl.col("day_id").cast(pl.Utf8)
                + pl.col("tour_num").cast(pl.Utf8).str.pad_start(2, "0")
                + pl.col("subtour_num").cast(pl.Utf8).str.pad_start(2, "0")
            )
            .cast(pl.Int64)
            .alias("tour_id"),
        ]
    )

    # Aggregation key is just tour_id (which includes subtour information)
    linked_trips_with_priority = linked_trips_with_priority.with_columns(
        [
            pl.col("tour_id").alias("_agg_key"),
        ]
    )

    # Add priority columns
    linked_trips_with_priority = add_purpose_priority_column(
        linked_trips_with_priority, config, alias="_purpose_priority"
    )
    linked_trips_with_priority = add_mode_priority_column(
        linked_trips_with_priority,
        config.mode_hierarchy,
        alias="_mode_priority",
    )

    # Add activity duration for tie-breaking
    linked_trips_with_priority = add_activity_duration_column(
        linked_trips_with_priority,
        config.default_activity_duration_minutes,
        alias="_activity_duration",
    )

    # Mark last trip in aggregation group to exclude from purpose selection
    linked_trips_with_priority = linked_trips_with_priority.with_columns(
        [
            (
                pl.col("linked_trip_id").rank("ordinal").over("_agg_key")
                == pl.col("linked_trip_id").count().over("_agg_key")
            ).alias("_is_last_trip_in_group"),
        ]
    )

    # For purpose selection, we need trips that are NOT the last trip
    non_last_trips = linked_trips_with_priority.filter(
        ~pl.col("_is_last_trip_in_group")
    )

    # Get the tour purpose from non-last trips
    tour_purposes = (
        non_last_trips.sort(
            ["_agg_key", "_purpose_priority", "_activity_duration"],
            descending=[False, False, True],
        )
        .group_by("_agg_key", maintain_order=True)
        .agg(pl.col("d_purpose_category").first().alias("tour_purpose"))
    )

    # Join tour purpose back to get destination coordinates
    # (first arrive and last depart at the primary destination location)
    trips_with_tour_purpose = linked_trips_with_priority.join(
        tour_purposes, on="_agg_key", how="left"
    )

    # Get coordinates and location type of primary destination
    # (highest priority trip that determined the tour purpose)
    primary_dest_coords = (
        non_last_trips.sort(
            ["_agg_key", "_purpose_priority", "_activity_duration"],
            descending=[False, False, True],
        )
        .group_by("_agg_key", maintain_order=True)
        .agg(
            [
                pl.col("d_lat").first().alias("_primary_d_lat"),
                pl.col("d_lon").first().alias("_primary_d_lon"),
                pl.col("d_location_type").first().alias("_primary_d_type"),
            ]
        )
    )

    # Join primary destination coordinates to all trips
    trips_with_primary_coords = trips_with_tour_purpose.join(
        primary_dest_coords, on="_agg_key", how="left"
    )

    # Calculate distances from trip origin and destination to
    # primary destination. Use haversine distance with small threshold
    # to account for GPS inaccuracy
    trips_with_primary_coords = trips_with_primary_coords.with_columns(
        [
            # Distance from trip destination to primary destination
            # (for arrivals)
            expr_haversine(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("_primary_d_lat"),
                pl.col("_primary_d_lon"),
            ).alias("_dist_d_to_primary"),
            # Distance from trip origin to primary destination (for departures)
            expr_haversine(
                pl.col("o_lat"),
                pl.col("o_lon"),
                pl.col("_primary_d_lat"),
                pl.col("_primary_d_lon"),
            ).alias("_dist_o_to_primary"),
        ]
    )

    # Get destination arrival and departure times
    # dest_arrive_time: Latest arrival at primary destination
    # dest_depart_time: Latest departure FROM primary destination

    # First calculate threshold for each trip's primary destination type
    trips_with_thresholds = trips_with_primary_coords.with_columns(
        [
            pl.when(pl.col("_primary_d_type") == LocationType.HOME)
            .then(pl.lit(config.distance_thresholds[LocationType.HOME]))
            .when(pl.col("_primary_d_type") == LocationType.WORK)
            .then(pl.lit(config.distance_thresholds[LocationType.WORK]))
            .when(pl.col("_primary_d_type") == LocationType.SCHOOL)
            .then(pl.lit(config.distance_thresholds[LocationType.SCHOOL]))
            .otherwise(pl.lit(config.distance_thresholds[LocationType.HOME]))
            .alias("_threshold"),
        ]
    )

    # Then mark arrivals and departures at primary destination
    trips_with_flags = trips_with_thresholds.with_columns(
        [
            (pl.col("_dist_d_to_primary") <= pl.col("_threshold")).alias(
                "_arrives_at_primary"
            ),
            (pl.col("_dist_o_to_primary") <= pl.col("_threshold")).alias(
                "_departs_from_primary"
            ),
        ]
    )

    # For arrivals: exclude last trip (last trip's destination is not the
    # tour destination). For departures: include ALL trips (last trip may
    # depart FROM the primary destination)
    arrival_times = (
        trips_with_flags.filter(
            ~pl.col("_is_last_trip_in_group") & pl.col("_arrives_at_primary")
        )
        .group_by("_agg_key")
        .agg(
            [
                pl.col("arrive_time").max().alias("dest_arrive_time"),
                pl.col("linked_trip_id").max().alias("dest_linked_trip_id"),
            ]
        )
    )

    departure_times = (
        trips_with_flags.filter(
            pl.col("_departs_from_primary")
        )  # Include all trips, even last trip
        .group_by("_agg_key")
        .agg(
            [
                pl.col("depart_time").max().alias("dest_depart_time"),
            ]
        )
    )

    destination_times = arrival_times.join(
        departure_times, on="_agg_key", how="full", coalesce=True
    )

    # Step 1: Aggregate basic tour attributes from trips
    tours = linked_trips_with_priority.group_by("_agg_key").agg(
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
            # Tour mode (highest priority = highest number in hierarchy)
            pl.col("mode_type")
            .sort_by("_mode_priority")
            .last()
            .alias("tour_mode"),
            # Timing at tour origin
            pl.col("depart_time").min().alias("origin_depart_time"),
            pl.col("arrive_time").max().alias("origin_arrive_time"),
            # Trip and stop counts
            pl.col("linked_trip_id").count().alias("trip_count"),
            (pl.col("linked_trip_id").count() - 1).alias("stop_count"),
            # Origin/destination locations
            pl.col("o_lat").first(),
            pl.col("o_lon").first(),
            pl.col("d_lat").last(),
            pl.col("d_lon").last(),
            pl.col("o_location_type").first(),
            pl.col("d_location_type").last(),
            # Flags for tour category classification
            pl.col("subtour_num").first().alias("_first_subtour_num"),
            pl.col("o_is_home").first().alias("_o_is_home"),
            pl.col("d_is_home").last().alias("_d_is_home"),
        ]
    )

    # Step 2: Join tour purpose and destination timing
    tours = tours.join(tour_purposes, on="_agg_key", how="left")
    tours = tours.join(destination_times, on="_agg_key", how="left")

    # Step 3: Classify tour category based on subtours and boundaries
    tours = tours.with_columns(
        [
            pl.when(pl.col("_first_subtour_num") > 0)
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

    # Step 4: Clean up temporary columns and sort
    tours = tours.drop(["_agg_key", "_o_is_home", "_d_is_home"]).sort(
        ["person_id", "day_id", "origin_depart_time"]
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
