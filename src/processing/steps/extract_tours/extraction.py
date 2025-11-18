"""Tour building module for travel diary survey processing.

This module implements a hierarchical tour extraction algorithm that processes
linked trip data to identify and classify tours and subtours based on spatial
and temporal patterns.

Algorithm Overview:
-------------------
The tour building process follows a four-stage pipeline:
1. Location Classification
    - Calculates haversine distances from trip endpoints to known locations
      (home, work, school) using person-specific coordinates
    - Classifies each trip origin/destination as HOME, WORK, SCHOOL, or OTHER
      based on configurable distance thresholds
    - Only matches work/school locations if person has those locations defined
2. Home-Based Tour Identification
    - Sorts trips by person, day, and departure time
    - Identifies tour boundaries by detecting:
      * Departures from home (o_is_home=True, d_is_home=False)
      * Returns to home (!o_is_home=True, d_is_home=True)
      * Day boundaries (first trip of person-day)
    - Assigns sequential tour IDs within each person-day
    - Format: tour_id = (day_id * 100) + tour_sequence_number
3. Work-Based Subtour Detection
    - Within home-based tours, identifies work-based subtours by detecting:
      * Departures from work (o_is_work=True, d_is_work=False)
      * Returns to work (!o_is_work=True, d_is_work=True)
    - Assigns hierarchical subtour IDs
    - Format: subtour_id = (tour_id * 10) + subtour_sequence_number
    - Updates tour_category to WORK_BASED for subtour trips
4. Tour Attribute Aggregation
    - Computes tour-level attributes from constituent trips:
      * tour_purpose: Highest priority dest purpose (person-type specific)
      * tour_mode: Highest priority travel mode (from mode hierarchy)
      * origin_depart_time: First trip departure time
      * dest_arrive_time: Last trip arrival time
      * trip_count: Number of trips in tour
      * stop_count: Number of intermediate stops (trip_count - 1)
    - Half-tour assignment: outbound (first half) vs inbound (second half)

Configuration:
-------------
Tour building behavior is controlled by TourConfig which defines:
- distance_thresholds: Maximum distances (meters) for location matching
- purpose_priority_by_person_category: Purpose hierarchies by person type
- mode_hierarchy: Ordered list of modes (ascending priority)
- person_type_mapping: Maps person_type codes to PersonCategory enum

Output:
-------
Returns two DataFrames:
1. linked_trips_with_tour_ids: Input trips with tour_id, subtour_id, and
    tour attributes joined for analysis
2. tours: Aggregated tour records with computed attributes (one row per tour)
The algorithm handles edge cases including:
- Incomplete tours (no return home at end of day)
- Multi-day tours (spanning survey boundaries)
- Missing work/school locations (null coordinates)
- Non-sequential trip chains (spatial gaps)
"""

import logging
from typing import Any

import polars as pl

from data_canon.codebook.generic import LocationType
from data_canon.codebook.tours import PersonCategory, TourBoundary, TourType
from data_canon.codebook.trips import PurposeCategory
from processing.decoration import step
from processing.utils import expr_haversine

from .priority_utils import (
    add_activity_duration_column,
    add_mode_priority_column,
    add_purpose_priority_column,
)
from .tour_configs import TourConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@step()
def extract_tours(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    linked_trips: pl.DataFrame,
    **kwargs: dict[str, Any],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Extract tours from linked trip data using TourExtractor class.

    Args:
        persons: DataFrame with person attributes
        households: DataFrame with household attributes
        linked_trips: DataFrame with linked trip data
        **kwargs: Additional configuration parameters for TourConfig

    Returns:
        Tuple of (linked_trips_with_tour_ids, tours):
        - linked_trips_with_tour_ids: Input trips with tour_id and
          subtour_id added (join to tours for attributes)
        - tours: Aggregated tour records with purpose, mode, timing
    """
    configs = TourConfig(**kwargs)
    extractor = TourExtractor(
        persons,
        households,
        linked_trips,
        configs,
    )
    return extractor.run()

class TourExtractor:
    """Build tours from trip data with cached person locations."""

    def __init__(
        self,
        persons: pl.DataFrame,
        households: pl.DataFrame,
        linked_trips: pl.DataFrame,
        config: TourConfig | None = None,
    ) -> None:
        """Initialize TourExtractor with person data and configuration.

        Args:
            persons: DataFrame with person attributes
            households: DataFrame with household attributes
            linked_trips: DataFrame with linked trip data
            config: Optional configuration dictionary
        """
        logger.info("Initializing TourExtractor...")

        self.linked_trips = linked_trips
        self.persons = persons
        self.households = households
        self.config = config or TourConfig()

        # Prepare person location cache with categories
        self.person_locations = self._prepare_person_locations()

        logger.info("TourExtractor ready for %d persons", len(self.persons))

    # =========================================================================
    # LOCATION CLASSIFICATION METHODS
    # =========================================================================

    def _prepare_person_locations(self) -> pl.DataFrame:
        """Prepare cached person location data with person categories.

        Returns:
            DataFrame with person locations and categories
        """
        logger.info("Preparing person location data...")

        # Join home location from households
        persons_with_home = self.persons.join(
            self.households.select(["hh_id", "home_lat", "home_lon"]),
            on="hh_id",
            how="left",
        )

        # Select needed columns
        person_locations = persons_with_home.select([
            "person_id", "person_type",
            "home_lat", "home_lon",
            "work_lat", "work_lon",
            "school_lat", "school_lon",
        ])

        # Add person category mapping
        # Convert enum keys to integer values for Polars compatibility
        person_type_map = {
            k.value: v for k, v in self.config.person_type_mapping.items()
        }
        return person_locations.with_columns([
            pl.col("person_type")
            .replace_strict(
                person_type_map,
                default=PersonCategory.OTHER
            )
            .alias("person_category")
        ])

    def _classify_trip_locations(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Classify trip origins and destinations by location type.

        Uses hybrid strategy: matches location if EITHER:
        - Purpose code indicates location (e.g., purpose=HOME)
        - Distance within threshold (haversine distance <= config threshold)

        Args:
            linked_trips: Trip data with o_lat, o_lon, d_lat, d_lon

        Returns:
            Trips with added columns:
            - o_is_home, o_is_work, o_is_school (bool flags)
            - d_is_home, d_is_work, d_is_school (bool flags)
            - o_location_type, d_location_type (LocationType enum)
        """
        logger.info("Classifying trip locations...")

        # Join person locations
        linked_trips = linked_trips.join(
            self.person_locations, on="person_id", how="left"
        )

        # Calculate distances to all known locations
        linked_trips = self._add_distance_columns(linked_trips)

        # Create boolean flags for location matches
        linked_trips = self._add_location_flags(linked_trips)

        # Determine primary location type for each trip end
        linked_trips = self._add_location_types(linked_trips)

        # Clean up temporary columns
        temp_cols = [
            "home_lat", "home_lon", "work_lat", "work_lon",
            "school_lat", "school_lon", "person_type",
        ]
        drop_cols = [
            c for c in linked_trips.columns
            if "dist_to" in c or c in temp_cols
        ]

        logger.info("Location classification complete")
        return linked_trips.drop(drop_cols)

    def _add_distance_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate haversine distances to known locations."""
        distance_cols = [
            expr_haversine(
                pl.col(f"{end}_lat"),
                pl.col(f"{end}_lon"),
                pl.col(f"{loc}_lat"),
                pl.col(f"{loc}_lon"),
            ).alias(f"{end}_dist_to_{loc}_meters")
            for loc in ["home", "work", "school"]
            for end in ["o", "d"]
        ]
        return df.with_columns(distance_cols)

    def _add_location_flags(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create boolean flags for location matches.

        Uses hybrid strategy: matches if EITHER purpose code OR distance
        indicates the location type.
        """
        flag_cols = []

        # Location configs: (location_type, null_check, purpose_categories)
        location_configs = {
            "home": (
                LocationType.HOME,
                None,
                [PurposeCategory.HOME],
            ),
            "work": (
                LocationType.WORK,
                "work_lat",
                [PurposeCategory.WORK, PurposeCategory.WORK_RELATED],
            ),
            "school": (
                LocationType.SCHOOL,
                "school_lat",
                [PurposeCategory.SCHOOL, PurposeCategory.SCHOOL_RELATED],
            ),
        }

        for loc, (loc_type, null_check, purpose_cats) in (
            location_configs.items()
        ):
            for end in ["o", "d"]:
                # Distance-based check
                distance_check = (
                    pl.col(f"{end}_dist_to_{loc}_meters")
                    <= self.config.distance_thresholds[loc_type]
                )
                if null_check:
                    distance_check = (
                        distance_check & pl.col(null_check).is_not_null()
                    )

                # Purpose-based check
                purpose_col = f"{end}_purpose_category"
                if purpose_col in df.columns:
                    # Convert enum objects to integer values for Polars
                    purpose_values = [p.value for p in purpose_cats]
                    purpose_check = pl.col(purpose_col).is_in(purpose_values)
                else:
                    purpose_check = pl.lit(value=False)

                # Match if EITHER purpose OR distance indicates location
                combined_check = purpose_check | distance_check

                flag_cols.append(combined_check.alias(f"{end}_is_{loc}"))

        return df.with_columns(flag_cols)

    def _add_location_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Determine primary location type based on priority."""

        def build_location_expr(prefix: str) -> pl.Expr:
            """Build expression for location type with priority order."""
            expr = pl.lit(LocationType.OTHER)
            # Reverse priority order: HOME > WORK > SCHOOL > OTHER
            for loc_type in [
                LocationType.SCHOOL,
                LocationType.WORK,
                LocationType.HOME,
            ]:
                col_name = f"{prefix}_is_{loc_type.name.lower()}"
                expr = pl.when(pl.col(col_name)).then(
                    pl.lit(loc_type)
                ).otherwise(expr)
            return expr

        return df.with_columns([
            build_location_expr("o").alias("o_location_type"),
            build_location_expr("d").alias("d_location_type"),
        ])

    # =========================================================================
    # TOUR BOUNDARY DETECTION METHODS
    # =========================================================================

    def _identify_home_based_tours(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Identify home-based tours from classified trip data.

        Creates tour boundaries for sequences of trips, classifying each tour
        by whether it starts/ends at home using TourBoundary enum:
        - COMPLETE: Starts at home, ends at home
        - PARTIAL_END: Starts at home, doesn't end at home
        - PARTIAL_START: Doesn't start at home, ends at home
        - PARTIAL_BOTH: Neither starts nor ends at home

        Tours are identified by detecting:
        1. Departures from home (o_is_home=True, d_is_home=False)
        2. Returns to home (o_is_home=False, d_is_home=True)
        3. Multi-day gaps (if config.check_multiday_gaps=True)

        Tours can be filtered downstream using the tour_boundary column:
        - Filter to TourBoundary.COMPLETE for legacy compatibility
        - Include partial tours for more comprehensive analysis

        Args:
            linked_trips: Classified linked trips with location type flags

        Returns:
            Linked trips with tour_id, tour_category, and tour_boundary
        """
        logger.info("Identifying home-based tours...")

        # Sort trips by person, day, and time
        linked_trips = linked_trips.sort(
            ["person_id", "day_id", "depart_time"]
        )

        # Mark trip characteristics for tour boundary detection
        is_leaving_home = pl.col("o_is_home") & ~pl.col("d_is_home")
        is_returning_home = ~pl.col("o_is_home") & pl.col("d_is_home")
        is_first_trip = (
            pl.col("depart_time")
            == pl.col("depart_time").min().over(["person_id", "day_id"])
        )
        is_last_trip = (
            pl.col("depart_time")
            == pl.col("depart_time").max().over(["person_id", "day_id"])
        )

        # Check for multi-day gaps if configured
        if self.config.check_multiday_gaps:
            day_gap = (pl.col("day_id") - pl.col("day_id").shift(1)).over(
                ["person_id"]
            )
            has_gap = day_gap > 1
        else:
            has_gap = pl.lit(value=False)

        # Tour starts when: leaving home OR (first trip AND not at home) OR gap
        tour_starts_leaving = is_leaving_home
        tour_starts_away = is_first_trip & ~pl.col("o_is_home")
        tour_starts_gap = has_gap & ~pl.col("o_is_home")
        tour_starts = (
            tour_starts_leaving | tour_starts_away | tour_starts_gap
        ).cast(pl.Int32)

        # Tour ends when: returning home OR last trip
        tour_ends = (is_returning_home | is_last_trip).cast(pl.Int32)

        # Assign tour numbers by cumulative sum of tour starts
        linked_trips = linked_trips.with_columns([
            is_leaving_home.alias("leaving_home"),
            is_returning_home.alias("returning_home"),
            tour_starts.alias("_tour_starts"),
            tour_ends.alias("_tour_ends"),
        ]).with_columns([
            pl.col("_tour_starts")
            .cum_sum()
            .over(["person_id", "day_id"])
            .alias("tour_num_in_day"),
        ]).with_columns([
            ((pl.col("day_id") * 100) + pl.col("tour_num_in_day")).alias(
                "tour_id"
            ),
        ])

        # Classify tour boundaries by checking first/last trips of each tour
        tour_first_trip = linked_trips.group_by("tour_id").agg([
            pl.col("o_is_home").first().alias("_tour_starts_at_home"),
            pl.col("d_is_home").last().alias("_tour_ends_at_home"),
        ])

        linked_trips = linked_trips.join(tour_first_trip, on="tour_id")

        # Assign tour boundary classification
        linked_trips = linked_trips.with_columns([
            pl.when(
                pl.col("_tour_starts_at_home") & pl.col("_tour_ends_at_home")
            )
            .then(pl.lit(TourBoundary.COMPLETE))
            .when(
                pl.col("_tour_starts_at_home") & ~pl.col("_tour_ends_at_home")
            )
            .then(pl.lit(TourBoundary.PARTIAL_END))
            .when(
                ~pl.col("_tour_starts_at_home") & pl.col("_tour_ends_at_home")
            )
            .then(pl.lit(TourBoundary.PARTIAL_START))
            .otherwise(pl.lit(TourBoundary.PARTIAL_BOTH))
            .alias("tour_boundary"),
            pl.lit(TourType.HOME_BASED).alias("tour_category"),
        ])

        # Clean up temporary columns
        linked_trips = linked_trips.drop([
            "_tour_starts",
            "_tour_ends",
            "_tour_starts_at_home",
            "_tour_ends_at_home",
        ])

        logger.info("Home-based tour identification complete")
        return linked_trips

    def _identify_work_based_subtours(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Identify work-based subtours within home-based tours.

        ALGORITHM NOTE - DIFFERS FROM LEGACY:
        ======================================
        Current: Detects ALL departures/returns to any work location
        Legacy:  Only detects subtours from "usual workplace" (coord match)

        Result: Current may over-detect subtours from work-related errands
                at non-usual locations (e.g., client site, delivery stop)

        TODO: Implement config.detect_usual_workplace to match legacy

        A work-based subtour is a sequence of trips during a work tour that:
        - Starts when leaving work (o_is_work & !d_is_work)
        - Ends when returning to work (!o_is_work & d_is_work)
        - Is numbered sequentially within the parent tour

        Args:
            linked_trips: Linked trips with home-based tour assignments

        Returns:
            Linked trips with subtour_id and updated tour_category
        """
        logger.info("Identifying work-based subtours...")

        # Step 1: Add basic work departure/return flags
        # A subtour starts when leaving work to go to a non-home location
        # A subtour ends when returning to work from a non-home location
        linked_trips = linked_trips.with_columns([
            (
                pl.col("o_is_work")
                & ~pl.col("d_is_work")
                & ~pl.col("d_is_home")
            ).alias("leaving_work"),
            (
                ~pl.col("o_is_work")
                & ~pl.col("o_is_home")
                & pl.col("d_is_work")
            ).alias("returning_work"),
        ])

        # Step 2: Add window-based subtour detection columns
        linked_trips = linked_trips.with_columns([
            pl.col("returning_work").shift(1, fill_value=False)
                .over("tour_id")
                .alias("prev_returned_work"),
            (
                pl.col("leaving_work")
                & (pl.col("leaving_work").cum_sum().over("tour_id") == 1)
            ).alias("is_first_work_departure"),
        ])

        # Step 3: Calculate subtour starts
        linked_trips = linked_trips.with_columns([
            pl.when(pl.col("leaving_work") | pl.col("returning_work"))
                .then(
                    (
                        pl.col("prev_returned_work")
                        | pl.col("is_first_work_departure")
                    ).cast(pl.Int32)
                )
                .otherwise(0)
                .alias("subtour_starts"),
        ])

        # Step 4: Calculate subtour numbers (cumsum of starts)
        linked_trips = linked_trips.with_columns([
            pl.col("subtour_starts").cum_sum().over("tour_id")
                .alias("subtour_num_in_tour"),
        ])

        # Step 5: Mark trips that are in a subtour
        # A trip is in a subtour if:
        # - It starts a subtour (leaving_work), OR
        # - It's between a leaving_work and returning_work, OR
        # - It ends a subtour (returning_work)
        # But NOT if it's going to/from home
        linked_trips = linked_trips.with_columns([
            pl.when(
                (pl.col("subtour_num_in_tour") > 0)
                & (pl.col("leaving_work") | pl.col("returning_work"))
            )
                .then(pl.lit(value=True))
                .otherwise(pl.lit(value=False))
                .alias("_in_subtour"),
        ])

        # Step 6: Add final subtour_id and update tour_category
        linked_trips = linked_trips.with_columns([
            pl.when(pl.col("_in_subtour"))
                .then(
                    (pl.col("tour_id") * 10)
                    + pl.col("subtour_num_in_tour")
                )
                .otherwise(None)
                .alias("subtour_id"),
            pl.when(pl.col("_in_subtour"))
                .then(pl.lit(TourType.WORK_BASED))
                .otherwise(pl.col("tour_category"))
                .alias("tour_category"),
        ])

        # Clean up temporary column
        linked_trips = linked_trips.drop("_in_subtour")

        logger.info("Work-based subtour identification complete")
        return linked_trips

    # =========================================================================
    # TOUR AGGREGATION METHODS
    # =========================================================================

    def _aggregate_tours(self, linked_trips: pl.DataFrame) -> pl.DataFrame:
        """Aggregate trip data to tour-level records with attributes.

        Calculates tour attributes from trip data:
        - Tour purpose: Highest priority destination, with duration tie-breaker
          (When priorities equal, selects trip with longest activity duration)
        - Tour mode: Highest priority trip mode
        - Timing: First departure and last arrival
        - Counts: Number of trips and stops

        IMPORTANT: Work-based subtours are aggregated separately using
        subtour_id instead of tour_id. The final output includes both
        home-based tours and work-based subtours as separate records.

        Args:
            linked_trips: Linked trips with tour_id and subtour_id assignments

        Returns:
            Tour-level DataFrame with aggregated attributes
        """
        logger.info("Aggregating tour data...")

        # Create aggregation key: use subtour_id for work-based subtours,
        # otherwise use tour_id
        linked_trips_with_priority = linked_trips.with_columns([
            pl.when(pl.col("subtour_id").is_not_null())
                .then(pl.col("subtour_id"))
                .otherwise(pl.col("tour_id"))
                .alias("_agg_key"),
        ])

        # Add priority columns
        linked_trips_with_priority = add_purpose_priority_column(
            linked_trips_with_priority, self.config, alias="_purpose_priority"
        )
        linked_trips_with_priority = add_mode_priority_column(
            linked_trips_with_priority,
            self.config.mode_hierarchy,
            alias="_mode_priority",
        )

        # Add activity duration for tie-breaking
        linked_trips_with_priority = add_activity_duration_column(
            linked_trips_with_priority,
            self.config.default_activity_duration_minutes,
            alias="_activity_duration",
        )

        # Mark last trip in aggregation group to exclude from purpose selection
        linked_trips_with_priority = linked_trips_with_priority.with_columns([
            (
                pl.col("linked_trip_id").rank("ordinal").over("_agg_key")
                == pl.col("linked_trip_id").count().over("_agg_key")
            ).alias("_is_last_trip_in_group"),
        ])

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

        tours = (
            linked_trips_with_priority.group_by("_agg_key")
            .agg(
                [
                    # Identifiers - tour_id is always the parent tour
                    pl.col("_agg_key").first().alias("tour_id"),
                    pl.col("person_id").first(),
                    pl.col("hh_id").first(),
                    pl.col("day_id").first(),
                    # Tour category
                    pl.col("tour_category").first(),
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
            .join(tour_purposes, on="_agg_key", how="left")
            .drop("_agg_key")
            .with_columns([
                # Add tour_num_in_day after aggregation
                pl.col("tour_id").rank("ordinal").over(
                    ["person_id", "day_id"]
                ).alias("tour_num_in_day"),
                # Add parent_tour_id for work-based subtours
                # For subtours (tour_id >= 1000), extract parent tour
                # by integer division by 10
                # For regular tours, parent_tour_id is null
                pl.when(pl.col("tour_category") == TourType.WORK_BASED)
                    .then(pl.col("tour_id") // 10)
                    .otherwise(None)
                    .alias("parent_tour_id"),
            ])
            .sort(["person_id", "day_id", "origin_depart_time"])
        )

        logger.info("Aggregated %d tours", len(tours))
        return tours

    # =========================================================================
    # MAIN PIPELINE METHOD
    # =========================================================================

    def run(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Build tours from linked trip data.

        Pipeline processes linked trip data through tour building steps:
        1. Classify trip locations (home, work, school, other)
        2. Identify home-based tours (assigns tour_id)
        3. Identify work-based subtours (assigns subtour_id)
        4. Aggregate to tour-level records with attributes

        Args:
            linked_trips: Linked trip data (see LinkedTripModel).
                          Trips must be pre-linked using linker.py.

        Returns:
            Tuple of (linked_trips_with_tour_ids, tours):
            - linked_trips_with_tour_ids: Input trips with tour_id and
              subtour_id added (join to tours for attributes)
            - tours: Aggregated tour records with purpose, mode, timing
        """
        logger.info("Building tours from linked trip data...")

        # Step 1: Classify trip locations
        linked_trips_classified = self._classify_trip_locations(
            self.linked_trips
        )

        # Step 2: Identify home-based tours
        linked_trips_with_hb_tours = self._identify_home_based_tours(
            linked_trips_classified
        )

        # Step 3: Identify work-based subtours
        linked_trips_with_subtours = self._identify_work_based_subtours(
            linked_trips_with_hb_tours
        )

        # Step 4: Aggregate tours
        tours = self._aggregate_tours(linked_trips_with_subtours)

        # Return clean linked_trips (drop temporary columns)
        temp_patterns = [
            "_is_home", "_is_work", "_is_school", "location_type",
            "leaving_", "returning_", "subtour_num_", "tour_num_",
        ]
        output_cols = [
            c for c in linked_trips_with_subtours.columns
            if not any(p in c for p in temp_patterns)
        ]
        linked_trips_with_tour_ids = linked_trips_with_subtours.select(
            output_cols
        )

        logger.info(
            "Tour building complete: %d linked trips, %d tours",
            len(linked_trips_with_tour_ids),
            len(tours),
        )
        return linked_trips_with_tour_ids, tours

