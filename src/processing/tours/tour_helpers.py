"""Tour detection helper functions for tour extraction.

This module contains functions for:
- Identifying home-based tour boundaries
- Expanding anchor location periods (work, school)
- Detecting anchor-based subtours (work-based, school-based)
"""

import logging

import polars as pl

from data_canon.codebook.generic import LocationType
from pipeline.utils import expr_haversine

logger = logging.getLogger(__name__)


def identify_home_based_tours(
    linked_trips: pl.DataFrame,
    check_multiday_gaps: bool = False,  # noqa: FBT001, FBT002
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
    3. Multi-day gaps (if check_multiday_gaps=True)

    Tours can be filtered downstream using the tour_category column:
    - Filter to TourBoundary.COMPLETE for legacy compatibility
    - Include partial tours for more comprehensive analysis

    Args:
        linked_trips: Classified linked trips with location type flags
        check_multiday_gaps: Whether to check for multi-day gaps

    Returns:
        Linked trips with tour_id, tour_category
    """
    logger.info("Identifying home-based tours...")

    # Sort trips by person, day, and time
    linked_trips = linked_trips.sort(["person_id", "day_id", "depart_time"])

    # Mark trip characteristics for tour boundary detection
    is_leaving_home = pl.col("o_is_home") & ~pl.col("d_is_home")
    is_returning_home = ~pl.col("o_is_home") & pl.col("d_is_home")
    is_first_trip = pl.col("depart_time") == pl.col("depart_time").min().over(
        ["person_id", "day_id"]
    )
    is_last_trip = pl.col("depart_time") == pl.col("depart_time").max().over(
        ["person_id", "day_id"]
    )

    # Check for multi-day gaps if configured
    if check_multiday_gaps:
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
    linked_trips = (
        linked_trips.with_columns(
            [
                is_leaving_home.alias("leaving_home"),
                is_returning_home.alias("returning_home"),
                tour_starts.alias("_tour_starts"),
                tour_ends.alias("_tour_ends"),
            ]
        )
        .with_columns(
            [
                pl.col("_tour_starts")
                .cum_sum()
                .over(["person_id", "day_id"])
                .alias("tour_num_in_day"),
            ]
        )
        .with_columns(
            [
                ((pl.col("day_id") * 100) + pl.col("tour_num_in_day")).alias(
                    "tour_id"
                ),
            ]
        )
    )

    # Clean up temporary columns
    linked_trips = linked_trips.drop(
        [
            "_tour_starts",
            "_tour_ends",
        ]
    )

    logger.info("Home-based tour identification complete")
    return linked_trips


def expand_anchor_periods(
    linked_trips: pl.DataFrame,
    person_locations: pl.DataFrame,
    distance_thresholds: dict,
) -> pl.DataFrame:
    """Expand anchor location periods for tours to usual locations.

    LEGACY REFERENCE: 03a-tour_extract_week.py lines 565-576
    GENERALIZED: Works for work, school, or any future anchor location

    For tours where trips visit a person's "usual" anchor location
    (work_lat/work_lon, school_lat/school_lon), this expands the period
    spent at that anchor by finding the first arrival and last departure.

    This ensures that subtours are only detected WITHIN the anchor period,
    not during travel to/from the anchor. For example:
        Home -> Work -> Lunch -> Work -> Errand -> Home
    Without expansion: Errand might be detected as subtour
    With expansion: Only Lunch is a subtour (between work visits)

    The expansion uses pure Polars window functions:
    1. Mark trips at each usual anchor location (work, school, etc.)
    2. For each tour, find first/last trip indices at each anchor
    3. Store as anchor_period_start/end for subtour detection

    Args:
        linked_trips: Classified trips with tour_id and location flags
        person_locations: Person location cache with work/school coords
        distance_thresholds: Dict mapping LocationType to distance in meters

    Returns:
        Trips with anchor_period_start_trip_num and anchor_period_end_trip_num
        markers, plus anchor_location_type indicating which anchor (if any)
    """
    logger.info("Expanding anchor location periods...")

    # Join person anchor locations (work and school)
    linked_trips = linked_trips.join(
        person_locations.select(
            ["person_id", "work_lat", "work_lon", "school_lat", "school_lon"]
        ),
        on="person_id",
        how="left",
    )

    # Add trip sequence number within tour for tracking positions
    linked_trips = linked_trips.with_columns(
        [
            pl.col("linked_trip_id")
            .rank("ordinal")
            .over("tour_id")
            .alias("_trip_num_in_tour"),
        ]
    )

    # Calculate distances to usual anchor locations
    # Work anchor
    linked_trips = linked_trips.with_columns(
        [
            expr_haversine(
                pl.col("o_lat"),
                pl.col("o_lon"),
                pl.col("work_lat"),
                pl.col("work_lon"),
            ).alias("_o_dist_to_usual_work"),
            expr_haversine(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("work_lat"),
                pl.col("work_lon"),
            ).alias("_d_dist_to_usual_work"),
        ]
    )

    # School anchor
    linked_trips = linked_trips.with_columns(
        [
            expr_haversine(
                pl.col("o_lat"),
                pl.col("o_lon"),
                pl.col("school_lat"),
                pl.col("school_lon"),
            ).alias("_o_dist_to_usual_school"),
            expr_haversine(
                pl.col("d_lat"),
                pl.col("d_lon"),
                pl.col("school_lat"),
                pl.col("school_lon"),
            ).alias("_d_dist_to_usual_school"),
        ]
    )

    # Mark trips at usual anchor locations
    work_threshold = distance_thresholds[LocationType.WORK]
    school_threshold = distance_thresholds[LocationType.SCHOOL]

    linked_trips = linked_trips.with_columns(
        [
            # Work anchor flags
            (
                (pl.col("_o_dist_to_usual_work") <= work_threshold)
                & pl.col("work_lat").is_not_null()
            ).alias("_o_at_usual_work"),
            (
                (pl.col("_d_dist_to_usual_work") <= work_threshold)
                & pl.col("work_lat").is_not_null()
            ).alias("_d_at_usual_work"),
            # School anchor flags
            (
                (pl.col("_o_dist_to_usual_school") <= school_threshold)
                & pl.col("school_lat").is_not_null()
            ).alias("_o_at_usual_school"),
            (
                (pl.col("_d_dist_to_usual_school") <= school_threshold)
                & pl.col("school_lat").is_not_null()
            ).alias("_d_at_usual_school"),
        ]
    )

    # Determine if trip involves usual anchor (either end at anchor)
    linked_trips = linked_trips.with_columns(
        [
            (pl.col("_o_at_usual_work") | pl.col("_d_at_usual_work")).alias(
                "_at_usual_work"
            ),
            (pl.col("_o_at_usual_school") | pl.col("_d_at_usual_school")).alias(
                "_at_usual_school"
            ),
        ]
    )

    # For each tour, find first and last trip at each anchor type
    # Work anchor expansion
    linked_trips = linked_trips.with_columns(
        [
            pl.when(pl.col("_at_usual_work"))
            .then(pl.col("_trip_num_in_tour"))
            .otherwise(None)
            .min()
            .over("tour_id")
            .alias("_work_period_start"),
            pl.when(pl.col("_at_usual_work"))
            .then(pl.col("_trip_num_in_tour"))
            .otherwise(None)
            .max()
            .over("tour_id")
            .alias("_work_period_end"),
        ]
    )

    # School anchor expansion
    linked_trips = linked_trips.with_columns(
        [
            pl.when(pl.col("_at_usual_school"))
            .then(pl.col("_trip_num_in_tour"))
            .otherwise(None)
            .min()
            .over("tour_id")
            .alias("_school_period_start"),
            pl.when(pl.col("_at_usual_school"))
            .then(pl.col("_trip_num_in_tour"))
            .otherwise(None)
            .max()
            .over("tour_id")
            .alias("_school_period_end"),
        ]
    )

    # Determine primary anchor type for tours with anchors
    # Priority: Work > School (matches person type priority)
    # Store which anchor type and the period boundaries
    linked_trips = linked_trips.with_columns(
        [
            pl.when(pl.col("_work_period_start").is_not_null())
            .then(pl.lit(LocationType.WORK))
            .when(pl.col("_school_period_start").is_not_null())
            .then(pl.lit(LocationType.SCHOOL))
            .otherwise(None)
            .alias("anchor_location_type"),
            pl.when(pl.col("_work_period_start").is_not_null())
            .then(pl.col("_work_period_start"))
            .when(pl.col("_school_period_start").is_not_null())
            .then(pl.col("_school_period_start"))
            .otherwise(None)
            .alias("anchor_period_start_trip_num"),
            pl.when(pl.col("_work_period_end").is_not_null())
            .then(pl.col("_work_period_end"))
            .when(pl.col("_school_period_end").is_not_null())
            .then(pl.col("_school_period_end"))
            .otherwise(None)
            .alias("anchor_period_end_trip_num"),
        ]
    )

    # Clean up temporary columns
    drop_cols = [
        "work_lat",
        "work_lon",
        "school_lat",
        "school_lon",
        "_o_dist_to_usual_work",
        "_d_dist_to_usual_work",
        "_o_dist_to_usual_school",
        "_d_dist_to_usual_school",
        "_o_at_usual_work",
        "_d_at_usual_work",
        "_o_at_usual_school",
        "_d_at_usual_school",
        "_at_usual_work",
        "_at_usual_school",
        "_work_period_start",
        "_work_period_end",
        "_school_period_start",
        "_school_period_end",
    ]
    linked_trips = linked_trips.drop(drop_cols)

    logger.info("Anchor location period expansion complete")
    return linked_trips


def detect_anchor_based_subtours(  # noqa: C901
    linked_trips: pl.DataFrame,
) -> pl.DataFrame:
    """Detect anchor-based subtours using hybrid loop approach.

    LEGACY REFERENCE: 03a-tour_extract_week.py lines 578-600
    MATCHES LEGACY: Only detects subtours within expanded anchor periods

    This hybrid approach loops over tours with anchor periods and detects
    subtours by finding leave/return patterns. Uses the anchor_period markers
    from expand_anchor_periods() to know where to look for subtours.

    A subtour is detected when:
    1. Trip leaves anchor location (o_at_anchor, !d_at_anchor)
    2. Trip returns to anchor location (!o_at_anchor, d_at_anchor)
    3. Both trips are WITHIN the expanded anchor period

    This prevents false subtour detection on trips to/from home.

    Args:
        linked_trips: Trips with anchor period markers

    Returns:
        Trips with subtour_id assigned to subtour trips
    """
    logger.info("Detecting anchor-based subtours...")

    # Initialize subtour_id column
    linked_trips = linked_trips.with_columns(
        [
            pl.lit(None, dtype=pl.Int64).alias("subtour_id"),
        ]
    )

    # Convert to list of dicts for loop-based processing
    trips_list = linked_trips.to_dicts()

    # Group trips by tour_id
    tours_dict = {}
    for trip in trips_list:
        tour_id = trip["tour_id"]
        if tour_id not in tours_dict:
            tours_dict[tour_id] = []
        tours_dict[tour_id].append(trip)

    # Process each tour
    subtour_counter = 0
    for tour_id, tour_trips in tours_dict.items():
        # Skip if no anchor period
        if not tour_trips[0].get("anchor_period_start_trip_num"):
            continue

        anchor_start = tour_trips[0]["anchor_period_start_trip_num"]
        anchor_end = tour_trips[0]["anchor_period_end_trip_num"]
        anchor_type = tour_trips[0]["anchor_location_type"]

        # Check if there are trips within the anchor period beyond just
        # arrival/departure (anchor_end > anchor_start + 1 means there
        # are intermediate trips)
        if anchor_end <= anchor_start + 1:
            continue

        # Detect subtours within the anchor period
        # Loop through trips in the anchor period
        subtour_num = 0
        in_subtour = False

        for _i, trip in enumerate(tour_trips):
            trip_num = trip["_trip_num_in_tour"]

            # Only check trips within anchor period (exclusive of
            # boundaries). anchor_start is first trip AT anchor,
            # anchor_end is last trip AT anchor. We want trips BETWEEN
            # these, so: anchor_start < trip_num < anchor_end
            if trip_num <= anchor_start or trip_num >= anchor_end:
                continue

            # Check if leaving anchor
            # Note: anchor_type is integer value from to_dicts()
            is_leaving_anchor = (
                trip["o_is_work"]
                if anchor_type == LocationType.WORK.value
                else trip["o_is_school"]
                if anchor_type == LocationType.SCHOOL.value
                else False
            )

            # Check if returning to anchor
            is_returning_anchor = (
                trip["d_is_work"]
                if anchor_type == LocationType.WORK.value
                else trip["d_is_school"]
                if anchor_type == LocationType.SCHOOL.value
                else False
            )

            # Subtour starts when leaving anchor
            if is_leaving_anchor and not is_returning_anchor and not in_subtour:
                in_subtour = True
                subtour_num += 1
                # Assign subtour_id to this trip
                trip["subtour_id"] = (tour_id * 10) + subtour_num

            # Subtour continues
            elif in_subtour and not is_returning_anchor:
                trip["subtour_id"] = (tour_id * 10) + subtour_num

            # Subtour ends when returning to anchor
            elif in_subtour and is_returning_anchor:
                trip["subtour_id"] = (tour_id * 10) + subtour_num
                in_subtour = False
                subtour_counter += 1

    # Convert back to polars DataFrame
    linked_trips = pl.DataFrame(trips_list)

    logger.info("Detected %s anchor-based subtours", subtour_counter)
    return linked_trips
