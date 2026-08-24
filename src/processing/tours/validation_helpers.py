"""Tour validation helper functions.

This module contains functions for:
- Grading each tour's structure into a ``tour_data_quality`` code
- Detecting spatial gaps (a missing leg) between consecutive trips
- Asserting that tour boundary detection assigned every trip to a tour
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from utils.helpers import expr_haversine

logger = logging.getLogger(__name__)

# Coordinate columns required to detect spatial gaps between consecutive trips.
_GAP_COORD_COLS = ("o_lat", "o_lon", "d_lat", "d_lon", "depart_time")


def _spatial_gap_flags(
    linked_trips: pl.DataFrame,
    threshold_meters: float,
) -> pl.DataFrame:
    """Flag tours that contain an internal spatial gap (a missing leg).

    Within each tour, trips are ordered by departure time and the haversine
    distance from one trip's destination to the next trip's origin is measured.
    A gap larger than ``threshold_meters`` means the diary skipped a connecting
    trip: the person was at one place and the next recorded trip begins
    elsewhere. Left alone, ``identify_home_based_tours`` welds such trips into a
    single tour (boundary detection keys only on home touches, never on spatial
    continuity), so a home-to-home tour can silently teleport mid-tour and still
    read as COMPLETE/VALID. Flagging lets the formatters drop it as a unit.

    Returns one row per tour_id with a ``_has_spatial_gap`` boolean. If the
    coordinate columns are absent (e.g. schema-only frames or unit tests without
    coordinates), every tour is reported as gap-free.

    Args:
        linked_trips: Linked trips with tour_id and o/d coordinates
        threshold_meters: Gap distance above which a junction is discontinuous

    Returns:
        DataFrame with columns [tour_id, _has_spatial_gap]
    """
    if any(c not in linked_trips.columns for c in _GAP_COORD_COLS):
        return (
            linked_trips.select("tour_id")
            .unique()
            .with_columns(pl.lit(value=False).alias("_has_spatial_gap"))
        )

    ordered = linked_trips.sort(["tour_id", "depart_time"]).with_columns(
        [
            pl.col("d_lat").shift(1).over("tour_id").alias("_prev_d_lat"),
            pl.col("d_lon").shift(1).over("tour_id").alias("_prev_d_lon"),
        ]
    )
    ordered = ordered.with_columns(
        expr_haversine(
            pl.col("_prev_d_lat"),
            pl.col("_prev_d_lon"),
            pl.col("o_lat"),
            pl.col("o_lon"),
        ).alias("_junction_gap_m")
    )
    return ordered.group_by("tour_id").agg(
        # any() ignores nulls, so a tour's first trip (no previous dest) and
        # single-trip tours (no internal junction) never trigger the flag.
        (pl.col("_junction_gap_m") > threshold_meters).any().alias("_has_spatial_gap")
    )


def _assert_tours_assigned(linked_trips: pl.DataFrame) -> None:
    """Raise if any trip was never assigned to a tour.

    Every first trip of a person-day starts a tour -- it either leaves the
    anchor, loops at it, or begins away from it -- so ``tour_num`` is always
    >= 1. A trip without one means boundary detection itself broke, which
    invalidates the whole tour table rather than the one row. Failing here beats
    stamping a "cause unknown" flag and letting a broken table flow downstream.

    Raises:
        ValueError: If any trip has a null or non-positive ``tour_num``.
    """
    if "tour_num" not in linked_trips.columns:
        return
    unassigned = linked_trips.filter(pl.col("tour_num").is_null() | (pl.col("tour_num") < 1))
    if unassigned.height:
        msg = (
            f"{unassigned.height} trips were never assigned to a tour, across "
            f"{unassigned['person_id'].n_unique()} persons. Tour boundary "
            f"detection failed; the tour table cannot be trusted."
        )
        raise ValueError(msg)


def validate_and_correct_tours(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    spatial_gap_threshold_meters: float = 1000.0,
) -> pl.DataFrame:
    """Stamp ``tour_data_quality``: what is structurally wrong with each tour.

    Assigns :class:`TourDataQuality` by first match, most actionable defect
    first, so a spatially-gapped partial tour reports the gap rather than the
    partialness:

    1. ``LOOP_TRIP`` / ``SINGLE_TRIP`` -- one trip, so not a tour at all
    2. ``CHANGE_MODE`` -- change mode as primary purpose (trip linking failure)
    3. ``SPATIAL_GAP`` -- an internal junction jumps > threshold (a missing leg)
    4. ``PARTIAL_BOTH`` / ``PARTIAL_START`` / ``PARTIAL_END`` -- the tour does
       not reach its own anchor at one or both ends

    The partial codes are read straight off ``tour_category`` rather than
    recomputed from the anchor flags, which is what makes the value-for-value
    mirror between the two enums true by construction rather than by
    coincidence. ``tour_category`` is left exactly as aggregation found it: it
    reports where the trips actually ran, and quality reports the verdict.

    Args:
        tours: Aggregated tour DataFrame with trip_count, tour_category and
            tour_purpose.
        linked_trips: Linked trips with tour_id, used for the spatial-gap check.
        spatial_gap_threshold_meters: Gap distance (meters) above which a tour's
            internal junction is treated as a missing leg (SPATIAL_GAP).

    Returns:
        Tours DataFrame with an added tour_data_quality column.
    """
    logger.info("Validating tour data quality...")

    _assert_tours_assigned(linked_trips)

    # Flag tours whose trips teleport across a data gap (missing connecting leg)
    gap_check = _spatial_gap_flags(linked_trips, spatial_gap_threshold_meters)
    tours = tours.join(gap_check, on="tour_id", how="left")

    # A one-trip tour is graded on where that single trip ran: anchor-to-anchor
    # is a loop, anything else never came back. Neither is a tour.
    single_trip = pl.col("trip_count") == 1
    tours = tours.with_columns(
        [
            pl.when(single_trip & (pl.col("tour_category") == TourCategory.COMPLETE))
            .then(pl.lit(TourDataQuality.LOOP_TRIP))
            .when(single_trip)
            .then(pl.lit(TourDataQuality.SINGLE_TRIP))
            .when(pl.col("tour_purpose") == PurposeCategory.CHANGE_MODE)
            .then(pl.lit(TourDataQuality.CHANGE_MODE))
            .when(pl.col("_has_spatial_gap").fill_null(value=False))
            .then(pl.lit(TourDataQuality.SPATIAL_GAP))
            .when(pl.col("tour_category") == TourCategory.PARTIAL_BOTH)
            .then(pl.lit(TourDataQuality.PARTIAL_BOTH))
            .when(pl.col("tour_category") == TourCategory.PARTIAL_START)
            .then(pl.lit(TourDataQuality.PARTIAL_START))
            .when(pl.col("tour_category") == TourCategory.PARTIAL_END)
            .then(pl.lit(TourDataQuality.PARTIAL_END))
            .otherwise(pl.lit(TourDataQuality.VALID))
            .alias("tour_data_quality")
        ]
    )

    # Log validation summary
    quality_summary = (
        tours.group_by("tour_data_quality").agg(pl.len().alias("count")).sort("tour_data_quality")
    )

    # Report all quality levels, including those with 0 count
    quality_counts = {
        row["tour_data_quality"]: row["count"] for row in quality_summary.iter_rows(named=True)
    }

    logger.info("Tour data quality summary:")
    for quality_enum in TourDataQuality:
        count = quality_counts.get(quality_enum.value, 0)
        logger.info("  %s: %d", quality_enum.label, count)

    # Warn if invalid tours found
    invalid_count = tours.filter(pl.col("tour_data_quality") != TourDataQuality.VALID).height
    if invalid_count > 0:
        logger.warning(
            "Found %d tours with data quality issues.\n"
            "These tours should be filtered in formatters.",
            invalid_count,
        )

    return tours.drop("_has_spatial_gap")
