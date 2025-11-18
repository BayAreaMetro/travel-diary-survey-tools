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

import polars as pl

from data_canon.codebook.persons import PersonType
from data_canon.codebook.tours import TourType

from .configs import TourConfig
from .location_classifier import LocationClassifier
from .tour_aggregator import TourAggregator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TourBuilder:
    """Build tours from trip data with cached person locations."""

    def __init__(
        self,
        persons: pl.DataFrame,
        households: pl.DataFrame | None = None,
        config: TourConfig | None = None,
    ) -> None:
        """Initialize TourBuilder with person data and configuration.

        Args:
            persons: DataFrame with person attributes
            households: Optional DataFrame with household attributes
                       (home_lat, home_lon). If provided, will be
                       joined to persons by hh_id
            config: Optional configuration dictionary
        """
        logger.info("Initializing TourBuilder...")

        if households is not None:
            # Join home_lat and home_lon from households if provided
            persons = persons.join(
                households.select(
                    ["hh_id", "home_lat", "home_lon"]
                ),
                on="hh_id",
                how="left",
            )

        self.persons = persons
        self.config = config or TourConfig()

        # Initialize helper modules
        person_locations = self._prepare_location_cache()
        self.location_classifier = LocationClassifier(
            self.config, person_locations
        )
        self.tour_aggregator = TourAggregator(self.config)

        logger.info("TourBuilder ready for %d persons", len(self.persons))

    def _prepare_location_cache(self) -> pl.DataFrame:
        """Prepare cached person location data.

        Returns:
            DataFrame with person locations and categories
        """
        logger.info("Caching person location data...")
        person_locations = self.persons.select([
            "person_id", "person_type",
            "home_lat", "home_lon",
            "work_lat", "work_lon",
            "school_lat", "school_lon",
        ])

        person_type_map = self.config.person_type_mapping
        return person_locations.with_columns([
            pl.col("person_type")
            .replace_strict(person_type_map, default=PersonType.OTHER)
            .alias("person_category")
        ])

    def _identify_home_based_tours(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Identify home-based tours from classified trip data.

        A home-based tour is a sequence of trips that:
        - Starts when leaving home (o_is_home & !d_is_home)
        - Ends when returning home (!o_is_home & d_is_home)
        - Or spans multiple days (incomplete tours)

        Args:
            linked_trips: Classified linked trips with location type flags

        Returns:
            Linked trips with tour_id and tour_category assigned
        """
        logger.info("Identifying home-based tours...")

        # Sort and identify tour boundaries
        linked_trips = linked_trips.sort(
            ["person_id", "day_id", "depart_time"]
        )

        # Create tour boundary flags and assign tour IDs
        is_returning = ~pl.col("o_is_home") & pl.col("d_is_home")
        prev_returned = is_returning.shift(1, fill_value=False).over(
            ["person_id", "day_id"]
        )
        is_first = (
            pl.col("depart_time")
            == pl.col("depart_time").min().over(["person_id", "day_id"])
        )

        linked_trips = (
            linked_trips.with_columns([
                (pl.col("o_is_home") & ~pl.col("d_is_home")).alias(
                    "leaving_home"
                ),
                is_returning.alias("returning_home"),
                (prev_returned | is_first).cast(pl.Int32).alias(
                    "tour_starts"
                ),
            ])
            .with_columns([
                pl.col("tour_starts")
                .cum_sum()
                .over(["person_id", "day_id"])
                .alias("tour_num_in_day"),
            ])
            .with_columns([
                ((pl.col("day_id") * 100) + pl.col("tour_num_in_day")).alias(
                    "tour_id"
                ),
                pl.lit(TourType.HOME_BASED).alias("tour_category"),
            ])
            .drop(["tour_starts"])
        )

        logger.info("Home-based tour identification complete")
        return linked_trips

    def _identify_work_based_subtours(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Identify work-based subtours within home-based tours.

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

        # Identify work-based subtours
        leaving_work = pl.col("o_is_work") & ~pl.col("d_is_work")
        returning_work = ~pl.col("o_is_work") & pl.col("d_is_work")
        prev_returned_work = returning_work.shift(
            1, fill_value=False
        ).over("tour_id")
        is_first_work_departure = leaving_work & (
            leaving_work.cum_sum().over("tour_id") == 1
        )

        subtour_starts = (
            pl.when(leaving_work | returning_work)
            .then(
                (prev_returned_work | is_first_work_departure).cast(
                    pl.Int32
                )
            )
            .otherwise(0)
        )

        subtour_num = (
            pl.when(subtour_starts.cum_sum().over("tour_id") > 0)
            .then(subtour_starts.cum_sum().over("tour_id"))
            .otherwise(0)
        )

        linked_trips = linked_trips.with_columns([
            leaving_work.alias("leaving_work"),
            returning_work.alias("returning_work"),
            subtour_num.alias("subtour_num_in_tour"),
            pl.when(subtour_num > 0)
                .then((pl.col("tour_id") * 10) + subtour_num)
                .otherwise(None)
                .alias("subtour_id"),
            pl.when(subtour_num > 0)
                .then(pl.lit(TourType.WORK_BASED))
                .otherwise(pl.col("tour_category"))
                .alias("tour_category"),
        ])

        logger.info("Work-based subtour identification complete")
        return linked_trips

    def extract_tours(
        self, linked_trips: pl.DataFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
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
        linked_trips_classified = (
            self.location_classifier.classify_trip_locations(linked_trips)
        )

        # Step 2: Identify home-based tours
        linked_trips_with_hb_tours = self._identify_home_based_tours(
            linked_trips_classified
        )

        # Step 3: Identify work-based subtours
        linked_trips_with_subtours = self._identify_work_based_subtours(
            linked_trips_with_hb_tours
        )

        # Step 4: Aggregate tours (calculates attributes)
        tours = self.tour_aggregator.aggregate_tours(linked_trips_with_subtours)

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
