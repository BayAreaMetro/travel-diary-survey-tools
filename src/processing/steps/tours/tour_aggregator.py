"""Tour aggregation module for computing tour-level attributes from trips.

This module handles the aggregation of trip-level data to tour-level records,
including purpose and mode prioritization, timing calculations, and trip counts.
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourType
from data_canon.codebook.trips import PurposeCategory

from .configs import TourConfig
from .mode_prioritizer import ModePrioritizer
from .purpose_prioritizer import PurposePrioritizer

logger = logging.getLogger(__name__)


class TourAggregator:
    """Aggregates trip data to tour-level records with computed attributes."""

    def __init__(self, config: TourConfig) -> None:
        """Initialize TourAggregator with configuration.

        Args:
            config: Tour configuration with priority settings
        """
        self.config = config
        self.purpose_prioritizer = PurposePrioritizer(config)
        self.mode_prioritizer = ModePrioritizer(config)

    def assign_tour_attributes(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Assign tour-level attributes from trip data.

        For each tour (home-based or work-based), compute:
        - Tour purpose (highest priority purpose on tour)
        - Tour mode (highest priority mode on tour)
        - Origin departure time (first trip depart time)
        - Destination arrival time (last trip arrive time)
        - Half-tour (outbound vs inbound)
        - Number of stops (intermediate destinations)

        Args:
            linked_trips: Linked trips with tour/subtour assignments

        Returns:
            Linked trips with tour attributes assigned
        """
        logger.info("Assigning tour attributes...")

        # Add priority columns
        linked_trips = self.purpose_prioritizer.add_priority_column(
            linked_trips
        )
        linked_trips = self.mode_prioritizer.add_priority_column(
            linked_trips
        )

        # Identify tour purpose (min priority = highest priority)
        # Mode hierarchy: higher value = more important, so take last
        tour_purposes = linked_trips.group_by("tour_id").agg(
            [
                pl.col("d_purpose_category")
                    .sort_by("purpose_priority")
                    .first()
                    .alias("tour_purpose"),
                pl.col("mode_type")
                    .sort_by("mode_priority")
                    .last()
                    .alias("tour_mode"),
                pl.col("depart_time").min().alias("origin_depart_time"),
                pl.col("arrive_time").max().alias("dest_arrive_time"),
                pl.col("linked_trip_id").count().alias("tour_trip_count"),
            ]
        )

        # Join tour attributes back to linked trips
        linked_trips = linked_trips.join(
            tour_purposes,
            on="tour_id",
            how="left",
        )

        # For work-based subtours, compute subtour attributes separately
        # Only if subtour_id column exists (after subtour identification)
        if "subtour_id" in linked_trips.columns:
            subtour_attrs = (
                linked_trips.filter(pl.col("subtour_id").is_not_null())
                .group_by("subtour_id")
                .agg(
                    [
                        pl.col("d_purpose_category")
                            .sort_by("purpose_priority")
                            .first()
                            .alias("subtour_purpose"),
                        pl.col("mode_type")
                            .sort_by("mode_priority")
                            .last()
                            .alias("subtour_mode"),
                        pl.col("depart_time")
                            .min()
                            .alias("subtour_origin_depart_time"),
                        pl.col("arrive_time")
                            .max()
                            .alias("subtour_dest_arrive_time"),
                        pl.col("linked_trip_id").count().alias("subtour_trip_count"),
                    ]
                )
            )

            # Join subtour attributes
            linked_trips = linked_trips.join(
                subtour_attrs,
                on="subtour_id",
                how="left",
            )

        # Determine half-tour (outbound/inbound) based on trip sequence
        linked_trips = linked_trips.with_columns(
            [
                pl.col("depart_time")
                .rank("ordinal")
                .over("tour_id")
                .alias("trip_seq_in_tour"),
            ]
        )

        linked_trips = linked_trips.with_columns(
            [
                pl.when(
                    pl.col("trip_seq_in_tour")
                    <= (pl.col("tour_trip_count") / 2).ceil()
                )
                .then(pl.lit("outbound"))
                .otherwise(pl.lit("inbound"))
                .alias("half_tour"),
            ]
        )

        # Clean up temporary columns
        linked_trips = linked_trips.drop(["purpose_priority", "mode_priority"])

        logger.info("Tour attribute assignment complete")
        return linked_trips

    def aggregate_tours(self, linked_trips: pl.DataFrame) -> pl.DataFrame:
        """Aggregate trip data to tour-level records with attributes.

        Calculates tour attributes from trip data:
        - Tour purpose: Highest priority trip purpose
        - Tour mode: Highest priority trip mode
        - Timing: First departure and last arrival
        - Counts: Number of trips and stops

        Args:
            linked_trips: Linked trips with tour_id assignments

        Returns:
            Tour-level DataFrame with aggregated attributes
        """
        logger.info("Aggregating tour data...")

        # Get or create priority columns
        if "purpose_priority" not in linked_trips.columns:
            linked_trips_with_priority = (
                self.purpose_prioritizer.add_priority_column(
                    linked_trips, alias="_purpose_priority"
                )
            )
            linked_trips_with_priority = (
                self.mode_prioritizer.add_priority_column(
                    linked_trips_with_priority, alias="_mode_priority"
                )
            )
        else:
            linked_trips_with_priority = linked_trips.with_columns([
                pl.col("purpose_priority").alias("_purpose_priority"),
                pl.col("mode_priority").alias("_mode_priority"),
            ])

        tours = (
            linked_trips_with_priority.group_by("tour_id")
            .agg(
                [
                    # Identifiers
                    pl.col("person_id").first(),
                    pl.col("hh_id").first(),
                    pl.col("day_id").first(),
                    pl.col("tour_num_in_day").first(),
                    # Tour category
                    pl.col("tour_category").first(),
                    # Tour purpose (highest priority = lowest number)
                    pl.col("d_purpose_category")
                        .sort_by("_purpose_priority")
                        .first()
                        .alias("tour_purpose"),
                    # Tour mode (highest priority = highest number in hierarchy)
                    pl.col("mode_type")
                        .sort_by("_mode_priority")
                        .last()
                        .alias("tour_mode"),
                    # Timing
                    pl.col("depart_time").min().alias("origin_depart_time"),
                    pl.col("arrive_time").max().alias("dest_arrive_time"),
                    # Trip counts
                    pl.col("linked_trip_id").count().alias("trip_count"),
                    # Stop counts (intermediate destinations)
                    (pl.col("linked_trip_id").count() - 1).alias("stop_count"),
                    # Location flags (if available)
                    pl.col("o_is_home").first().alias("starts_at_home")
                    if "o_is_home" in linked_trips.columns
                    else pl.lit(None).alias("starts_at_home"),
                    pl.col("d_is_home").last().alias("ends_at_home")
                    if "d_is_home" in linked_trips.columns
                    else pl.lit(None).alias("ends_at_home"),
                ]
            )
            .sort(["person_id", "day_id", "origin_depart_time"])
        )

        logger.info("Aggregated %d tours", len(tours))
        return tours

    def aggregate_person_days(
        self, trips: pl.DataFrame, tours: pl.DataFrame
    ) -> pl.DataFrame:
        """Aggregate trip and tour data to person-day records.

        Creates one record per person-day with activity pattern summaries.

        Args:
            trips: Trips with tour attributes
            tours: Tour-level aggregated data

        Returns:
            Person-day DataFrame with pattern summaries
        """
        logger.info("Aggregating person-day data...")

        # Count tours by category for each person-day
        tour_counts = tours.group_by(["person_id", "day_id"]).agg(
            [
                pl.when(pl.col("tour_category") == TourType.HOME_BASED)
                    .then(1)
                    .otherwise(0)
                    .sum()
                    .alias("home_based_tour_count"),
                pl.when(pl.col("tour_category") == TourType.WORK_BASED)
                    .then(1)
                    .otherwise(0)
                    .sum()
                    .alias("work_based_tour_count"),
            ]
        )

        # Count trips by purpose for each person-day
        purpose_counts = trips.group_by(["person_id", "day_id"]).agg(
            [
                # Count trips for each purpose type dynamically
                *[
                    (pl.col("d_purpose_category") == purpose)
                    .sum()
                    .alias(
                        f"{purpose.value.lower().replace(' ', '_')}_trip_count"
                    )
                    for purpose in PurposeCategory
                ],
                # Total trip count
                pl.len().alias("total_trip_count"),
            ]
        )

        # Combine person-day aggregations
        person_days = (
            trips.select(["person_id", "hh_id", "day_id"])
            .unique()
            .join(tour_counts, on=["person_id", "day_id"], how="left")
            .join(purpose_counts, on=["person_id", "day_id"], how="left")
            .fill_null(0)
            .sort(["person_id", "day_id"])
        )

        logger.info("Aggregated %d person-days", len(person_days))
        return person_days

