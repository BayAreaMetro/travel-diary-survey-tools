"""Tour building module for travel diary survey processing.

This module provides functionality to build tours from trip data, identifying
home-based and work-based tour patterns and generating tour-level summaries.

The tour building algorithm:
1. Classifies trip locations (home, work, school, other) using configurable
   distance thresholds
2. Identifies home-based tours (sequences starting/ending at home)
3. Identifies work-based subtours (sequences starting/ending at work within
   home-based tours)
4. Assigns tour attributes (purpose, mode, timing) using priority hierarchies
5. Aggregates to tour-level and person-day records

Usage (Functional API):
    >>> import polars as pl
    >>> from travel_diary_survey_tools.tours import build_tours
    >>>
    >>> # Load linked trip and person data
    >>> # Note: Trips must be pre-linked using linker.py
    >>> linked_trips = pl.read_csv("linked_trips.csv")
    >>> persons = pl.read_csv("persons.csv")
    >>>
    >>> # Build tours (adds tour_id/subtour_id only)
    >>> linked_trips_with_ids, tours = build_tours(linked_trips, persons)
    >>>
    >>> # Join to get tour attributes on trips if needed
    >>> enriched = linked_trips_with_ids.join(
    ...     tours.select(["tour_id", "tour_purpose", "tour_mode"]),
    ...     on="tour_id", how="left"
    ... )
    >>>
    >>> # Save outputs
    >>> linked_trips_with_ids.write_parquet("linked_trips_with_ids.parquet")
    >>> tours.write_parquet("tours.parquet")

Usage (Class-based API with custom config):
    >>> from travel_diary_survey_tools.tours import TourBuilder, DEFAULT_CONFIG
    >>>
    >>> # Customize configuration
    >>> config = DEFAULT_CONFIG.copy()
    >>> config["distance_threshold"][LocationType.HOME] = 0.5  # miles
    >>>
    >>> # Build tours
    >>> builder = TourBuilder(persons, config)
    >>> linked_trips_with_ids, tours = builder.build_tours(linked_trips)

Usage (Standalone):
    Can be run standalone by executing the module directly:
    $ python src/travel_diary_survey_tools/tours.py

    Edit the __main__ block to specify input file paths and configuration.

Relational Structure:
    The module maintains foreign key relationships similar to link_trips():

    >>> # Complete pipeline from raw trips to tours
    >>> from travel_diary_survey_tools import link_trips, build_tours
    >>>
    >>> # Step 1: Link raw trips (adds linked_trip_id)
    >>> trip_tours_raw, linked_trips = link_trips(raw_trips, change_mode)
    >>>
    >>> # Step 2: Build tours (adds tour_id, subtour_id only)
    >>> linked_trips_with_ids, tours = build_tours(linked_trips, persons)
    >>>
    >>> # Now linked_trips_with_ids contains:
    >>> # - All original linked trip fields
    >>> # - linked_trip_id (from link_trips)
    >>> # - tour_id, subtour_id (from build_tours)
    >>>
    >>> # Join tour attributes as needed:
    >>> enriched = linked_trips_with_ids.join(
    ...     tours.select(["tour_id", "tour_purpose", "tour_mode"]),
    ...     on="tour_id",
    ...     how="left"
    ... )

Configuration:
    The DEFAULT_CONFIG dict contains all configurable parameters:
    - distance_threshold: Miles within which to match trip ends to locations
    - purpose_priority: Priority order for tour purpose assignment
    - mode_priority: Priority order for tour mode assignment
    - person_category_map: Mapping from PersonType to PersonCategory

Input Requirements:
    Trips DataFrame must contain:
    - person_id, household_id, day_id, trip_id
    - o_lat, o_lon, d_lat, d_lon
    - depart_time, arrive_time
    - d_purpose_category, mode_type

    Persons DataFrame must contain:
    - person_id, household_id, person_type
    - home_lat, home_lon
    - work_lat, work_lon (optional, can be null)
    - school_lat, school_lon (optional, can be null)

Output:
    Returns tuple of (trip_tours, tours, person_days):
    - trip_tours: Original trips with tour assignments and location flags
    - tours: Aggregated tour records with purpose, mode, timing
    - person_days: Person-day summaries with tour and trip counts
"""

import logging

import polars as pl

from data_canon.codebook import ModeType
from travel_diary_survey_tools.utils import expr_haversine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Default Configuration --------------------------------------------------------

DEFAULT_CONFIG = {
    "distance_thresholds": {
        LocationType.HOME: 100.0,
        LocationType.WORK: 100.0,
        LocationType.SCHOOL: 100.0,
    },
    "mode_hierarchy": {
        # Maps mode codes to hierarchy levels (higher = more important)
        ModeType.WALK: 1,
        ModeType.BIKE: 2,
        ModeType.AUTO: 3,
        ModeType.TRANSIT: 4,
        ModeType.DRIVE_TRANSIT: 5,
    },
    "purpose_priority_by_person_category": {
        # Priority order for determining primary tour purpose
        # Lower number = higher priority
        PersonCategory.WORKER: {
            TripPurpose.WORK: 1,  # Highest priority for workers
            TripPurpose.SCHOOL: 2,
            TripPurpose.ESCORT: 3,
            # All other purposes get default priority of 4
        },
        PersonCategory.STUDENT: {
            TripPurpose.SCHOOL: 1,  # Highest priority for students
            TripPurpose.WORK: 2,
            TripPurpose.ESCORT: 3,
        },
        PersonCategory.OTHER: {
            TripPurpose.WORK: 1,
            TripPurpose.SCHOOL: 2,
            TripPurpose.ESCORT: 3,
        },
    },
    "default_purpose_priority": 4,
    "person_type_mapping": {
        # Maps person_type codes to categories for priority lookup
        PersonType.FULL_TIME_WORKER: PersonCategory.WORKER,
        PersonType.PART_TIME_WORKER: PersonCategory.WORKER,
        PersonType.RETIRED: PersonCategory.OTHER,
        PersonType.NON_WORKER: PersonCategory.OTHER,
        PersonType.UNIVERSITY_STUDENT: PersonCategory.STUDENT,
        PersonType.HIGH_SCHOOL_STUDENT: PersonCategory.STUDENT,
        PersonType.CHILD_5_15: PersonCategory.STUDENT,
        PersonType.CHILD_UNDER_5: PersonCategory.OTHER,
    },
}


# Tour Builder Class -----------------------------------------------------------


class TourBuilder:
    """Build tours from trip data with cached person locations."""

    def __init__(
        self,
        persons: pl.DataFrame,
        households: pl.DataFrame | None = None,
        config: dict | None = None,
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
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._prepare_location_cache()
        logger.info("TourBuilder ready for %d persons", len(self.persons))

    def _prepare_location_cache(self) -> None:
        """Prepare cached person location data."""
        logger.info("Caching person location data...")
        self.person_locations = self.persons.select(
            [
                "person_id",
                "person_type",
                "home_lat",
                "home_lon",
                "work_lat",
                "work_lon",
                "school_lat",
                "school_lon",
            ]
        )

        person_type_map = self.config["person_type_mapping"]
        self.person_locations = self.person_locations.with_columns(
            [
                pl.col("person_type")
                .replace_strict(person_type_map, default=PersonCategory.OTHER)
                .alias("person_category")
            ]
        )

    def _is_within_threshold(
        self,
        distance_col: str,
        location_type: LocationType,
    ) -> pl.Expr:
        """Create expression to check if distance is within threshold.

        Args:
            distance_col: Name of distance column to check
            location_type: Type of location (from LocationType enum)

        Returns:
            Boolean expression for within-threshold check
        """
        threshold = self.config["distance_thresholds"][location_type]
        return pl.col(distance_col) <= threshold

    def _classify_trip_locations(
        self, linked_trips: pl.DataFrame
    ) -> pl.DataFrame:
        """Classify trip origins and destinations by location type."""
        logger.info("Classifying trip locations...")

        linked_trips = linked_trips.join(
            self.person_locations, on="person_id", how="left"
        )

        # Calculate distances to known locations
        for location in ["home", "work", "school"]:
            for end in ["o", "d"]:
                lat_col = f"{end}_lat"
                lon_col = f"{end}_lon"
                dist_col = f"{end}_dist_to_{location}_meters"

                linked_trips = linked_trips.with_columns(
                    expr_haversine(
                        pl.col(lat_col),
                        pl.col(lon_col),
                        pl.col(f"{location}_lat"),
                        pl.col(f"{location}_lon"),
                    ).alias(dist_col)
                )

        # Create boolean flags for location matches
        # Note: For work/school, only match if person has that location
        linked_trips = linked_trips.with_columns(
            [
                self._is_within_threshold(
                    "o_dist_to_home_meters", LocationType.HOME
                ).alias("o_is_home"),
                (
                    self._is_within_threshold(
                        "o_dist_to_work_meters", LocationType.WORK
                    )
                    & pl.col("work_lat").is_not_null()
                ).alias("o_is_work"),
                (
                    self._is_within_threshold(
                        "o_dist_to_school_meters",
                        LocationType.SCHOOL,
                    )
                    & pl.col("school_lat").is_not_null()
                ).alias("o_is_school"),
                self._is_within_threshold(
                    "d_dist_to_home_meters", LocationType.HOME
                ).alias("d_is_home"),
                (
                    self._is_within_threshold(
                        "d_dist_to_work_meters", LocationType.WORK
                    )
                    & pl.col("work_lat").is_not_null()
                ).alias("d_is_work"),
                (
                    self._is_within_threshold(
                        "d_dist_to_school_meters",
                        LocationType.SCHOOL,
                    )
                    & pl.col("school_lat").is_not_null()
                ).alias("d_is_school"),
            ]
        )

        # Determine location type using priority order
        location_priority = [
            ("o_is_home", LocationType.HOME),
            ("o_is_work", LocationType.WORK),
            ("o_is_school", LocationType.SCHOOL),
        ]

        # Determine location type using priority order, defaulting to OTHER
        o_expr = pl.lit(LocationType.OTHER)
        for flag_col, loc_type in reversed(location_priority):
            o_expr = (
                pl.when(pl.col(flag_col))
                .then(pl.lit(loc_type))
                .otherwise(o_expr)
            )

        # Determine destination location type using priority order
        d_expr = pl.lit(LocationType.OTHER)
        for flag_col, loc_type in reversed(location_priority):
            d_flag = flag_col.replace("o_", "d_")
            d_expr = (
                pl.when(pl.col(d_flag))
                .then(pl.lit(loc_type))
                .otherwise(d_expr)
            )

        linked_trips = linked_trips.with_columns(
            [
                o_expr.alias("o_location_type"),
                d_expr.alias("d_location_type"),
            ]
        )

        # Drop temporary columns
        drop_cols = [
            c
            for c in linked_trips.columns
            if "dist_to" in c
            or c
            in [
                "home_lat",
                "home_lon",
                "work_lat",
                "work_lon",
                "school_lat",
                "school_lon",
                "person_type",
            ]
        ]
        linked_trips = linked_trips.drop(drop_cols)

        logger.info("Location classification complete")
        return linked_trips

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

        # Sort trips by person, day, and time
        linked_trips = linked_trips.sort(["person_id", "day_id", "depart_time"])

        # Identify tour start points:
        # - First trip of person-day
        # - Leaving home (o_is_home=True, d_is_home=False)
        # - After a gap (previous dest wasn't current origin)
        linked_trips = linked_trips.with_columns(
            [
                # Flag: leaving home on this trip
                (pl.col("o_is_home") & ~pl.col("d_is_home")).alias(
                    "leaving_home"
                ),
                # Flag: returning home on this trip
                (~pl.col("o_is_home") & pl.col("d_is_home")).alias(
                    "returning_home"
                ),
                # Flag: staying at home (both origin and dest are home)
                (pl.col("o_is_home") & pl.col("d_is_home")).alias(
                    "at_home"
                ),
            ]
        )

        # Create tour boundary markers
        # A new tour starts when:
        # 1. It's the first trip for this person-day, OR
        # 2. Previous trip returned home
        linked_trips = linked_trips.with_columns(
            [
                pl.col("returning_home")
                .shift(1, fill_value=False)
                .over(["person_id", "day_id"])
                .alias("prev_returned_home"),
            ]
        )

        linked_trips = linked_trips.with_columns(
            [
                # New tour starts after returning home or at day start
                (
                    pl.col("prev_returned_home")
                    | (
                        pl.col("depart_time")
                        == pl.col("depart_time")
                        .min()
                        .over(["person_id", "day_id"])
                    )
                )
                .cast(pl.Int32)
                .alias("tour_starts"),
            ]
        )

        # Assign tour IDs using cumulative sum of tour starts
        linked_trips = linked_trips.with_columns(
            [
                pl.col("tour_starts")
                .cum_sum()
                .over(["person_id", "day_id"])
                .alias("tour_num_in_day"),
            ]
        )

        # Create globally unique tour ID as Int64
        # Format: day_id concatenated with tour_num (with leading zero)
        # Example: day_id=5, tour_num=3 -> tour_id=503
        linked_trips = linked_trips.with_columns(
            [
                ((pl.col("day_id") * 100) + pl.col("tour_num_in_day")).alias(
                    "tour_id"
                ),
                # All tours at this level are home-based
                pl.lit(TourCategory.HOME_BASED).alias("tour_category"),
            ]
        )

        # Clean up temporary columns
        linked_trips = linked_trips.drop(
            ["tour_starts", "prev_returned_home", "at_home"]
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

        # Initialize subtour tracking columns
        linked_trips = linked_trips.with_columns(
            [
                pl.lit(None).cast(pl.Int64).alias("subtour_id"),
                pl.lit(0).alias("subtour_num_in_tour"),
            ]
        )

        # Identify subtour boundaries within each home-based tour
        linked_trips = linked_trips.with_columns(
            [
                # Leaving work
                (pl.col("o_is_work") & ~pl.col("d_is_work")).alias(
                    "leaving_work"
                ),
                # Returning to work
                (~pl.col("o_is_work") & pl.col("d_is_work")).alias(
                    "returning_work"
                ),
            ]
        )

        # Track previous trip's return to work within each home tour
        linked_trips = linked_trips.with_columns(
            [
                pl.col("returning_work")
                .shift(1, fill_value=False)
                .over("tour_id")
                .alias("prev_returned_work"),
            ]
        )

        # Mark subtour starts: after returning to work or first work departure
        linked_trips = linked_trips.with_columns(
            [
                (
                    pl.when(pl.col("leaving_work") | pl.col("returning_work"))
                    .then(
                        (
                            pl.col("prev_returned_work")
                            | (
                                pl.col("leaving_work")
                                & (
                                    pl.col("leaving_work")
                                    .cum_sum()
                                    .over("tour_id")
                                    == 1
                                )
                            )
                        ).cast(pl.Int32)
                    )
                    .otherwise(0)
                ).alias("subtour_starts"),
            ]
        )

        # Assign subtour numbers within each tour
        linked_trips = linked_trips.with_columns(
            [
                pl.when(pl.col("subtour_starts").cum_sum().over("tour_id") > 0)
                .then(pl.col("subtour_starts").cum_sum().over("tour_id"))
                .otherwise(0)
                .alias("subtour_num_in_tour"),
            ]
        )

        # Create subtour IDs for work-based subtours
        # Format: tour_id times 10 plus subtour_num (e.g., 503*10+2=5032)
        linked_trips = linked_trips.with_columns(
            [
                pl.when(pl.col("subtour_num_in_tour") > 0)
                .then((pl.col("tour_id") * 10) + pl.col("subtour_num_in_tour"))
                .otherwise(None)
                .alias("subtour_id"),
                # Update tour category for subtours
                pl.when(pl.col("subtour_num_in_tour") > 0)
                .then(pl.lit(TourCategory.WORK_BASED))
                .otherwise(pl.col("tour_category"))
                .alias("tour_category"),
            ]
        )

        # Clean up temporary columns
        linked_trips = linked_trips.drop(
            ["subtour_starts", "prev_returned_work"]
        )

        logger.info("Work-based subtour identification complete")
        return linked_trips

    def _assign_tour_attributes(
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

        # Map trip purposes to priority values based on person category
        purpose_priority_by_category = self.config[
            "purpose_priority_by_person_category"
        ]
        default_priority = self.config["default_purpose_priority"]

        # Map mode types to priority values using hierarchy
        mode_hierarchy = self.config["mode_hierarchy"]

        # Add priority columns for aggregation
        # Purpose priority depends on person category
        def get_purpose_priority(purpose: int, category: str) -> int:
            category_map = purpose_priority_by_category.get(
                category, purpose_priority_by_category[PersonCategory.OTHER]
            )
            return category_map.get(purpose, default_priority)

        linked_trips = linked_trips.with_columns(
            [
                pl.struct(["d_purpose_category", "person_category"])
                .map_elements(
                    lambda x: get_purpose_priority(
                        x["d_purpose_category"], x["person_category"]
                    ),
                    return_dtype=pl.Int32,
                )
                .alias("purpose_priority"),
                pl.col("mode_type")
                .map_elements(
                    lambda x: mode_hierarchy.get(x, 0),
                    return_dtype=pl.Int32,
                )
                .alias("mode_priority"),
            ]
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

    def _aggregate_tours(self, linked_trips: pl.DataFrame) -> pl.DataFrame:
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

        # Map trip purposes and modes to priority values
        purpose_priority_by_category = self.config[
            "purpose_priority_by_person_category"
        ]
        default_priority = self.config["default_purpose_priority"]
        mode_hierarchy = self.config["mode_hierarchy"]

        # Add priority columns for aggregation if not already present
        # (they may exist from _assign_tour_attributes)
        if "purpose_priority" not in linked_trips.columns:

            def get_purpose_priority(purpose: int, category: str) -> int:
                category_map = purpose_priority_by_category.get(
                    category,
                    purpose_priority_by_category[PersonCategory.OTHER],
                )
                return category_map.get(purpose, default_priority)

            linked_trips_with_priority = linked_trips.with_columns(
                [
                    pl.struct(["d_purpose_category", "person_category"])
                    .map_elements(
                        lambda x: get_purpose_priority(
                            x["d_purpose_category"], x["person_category"]
                        ),
                        return_dtype=pl.Int32,
                    )
                    .alias("_purpose_priority"),
                    pl.col("mode_type")
                    .map_elements(
                        lambda x: mode_hierarchy.get(x, 0),
                        return_dtype=pl.Int32,
                    )
                    .alias("_mode_priority"),
                ]
            )
        else:
            # Reuse existing priority columns
            linked_trips_with_priority = linked_trips.with_columns(
                [
                    pl.col("purpose_priority").alias("_purpose_priority"),
                    pl.col("mode_priority").alias("_mode_priority"),
                ]
            )

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

    def _aggregate_person_days(
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
                pl.when(pl.col("tour_category") == TourCategory.HOME_BASED)
                .then(1)
                .otherwise(0)
                .sum()
                .alias("home_based_tour_count"),
                pl.when(pl.col("tour_category") == TourCategory.WORK_BASED)
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
                    for purpose in TripPurpose
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
        linked_trips_classified = self._classify_trip_locations(linked_trips)

        # Step 2: Identify home-based tours
        linked_trips_with_hb_tours = self._identify_home_based_tours(
            linked_trips_classified
        )

        # Step 3: Identify work-based subtours
        linked_trips_with_subtours = self._identify_work_based_subtours(
            linked_trips_with_hb_tours
        )

        # Step 4: Aggregate tours (calculates attributes)
        tours = self._aggregate_tours(linked_trips_with_subtours)

        # Return clean linked_trips with just IDs (no temp columns)
        output_cols = [
            c
            for c in linked_trips_with_subtours.columns
            if not any(
                x in c
                for x in [
                    "_is_home",
                    "_is_work",
                    "_is_school",
                    "location_type",
                    "leaving_",
                    "returning_",
                    "subtour_num_",
                    "tour_num_",
                ]
            )
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


if __name__ == "__main__":  # pragma: no cover
    from pathlib import Path

    logging.basicConfig(level=logging.INFO)

    # Example standalone usage
    DATA_DIR = Path("data")
    LINKED_TRIPS_FILE = DATA_DIR / "linked_trips.csv"
    PERSONS_FILE = DATA_DIR / "persons.csv"
    OUTPUT_DIR = DATA_DIR / "output"

    # Load data (trips must be pre-linked using linker.py)
    logger.info("Loading linked trips from %s", LINKED_TRIPS_FILE)
    linked_trips = pl.read_csv(LINKED_TRIPS_FILE)

    logger.info("Loading persons from %s", PERSONS_FILE)
    persons = pl.read_csv(PERSONS_FILE)

    # Build tours with default configuration
    tour_builder = TourBuilder(persons)
    linked_trips_with_tours, tours = tour_builder.build_tours(linked_trips)

    # Write outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    linked_trips_file = OUTPUT_DIR / "linked_trips_with_tours.parquet"
    tours_file = OUTPUT_DIR / "tours.parquet"

    logger.info(
        "Writing %d linked trips to %s",
        len(linked_trips_with_tours),
        linked_trips_file,
    )
    linked_trips_with_tours.write_parquet(linked_trips_file)

    logger.info("Writing %d tours to %s", len(tours), tours_file)
    tours.write_parquet(tours_file)

    logger.info("Tour building complete!")
