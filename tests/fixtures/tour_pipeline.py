"""Run tour extraction the way the pipeline does: locations first.

``extract_tours`` reads a finished ``habitual_locations`` table, which survey
cleaning delivers and ``detect_habitual_locations`` completes. Tests that start
from persons, households and trips go through the same three stages here.
"""

from typing import Any

import polars as pl

from processing.habitual_locations import (
    HabitualLocationConfig,
    MatchConfig,
    detect_habitual_locations,
    reported_habitual_locations,
)
from processing.tours import extract_tours
from processing.tours.tour_configs import TourConfig

# The habitual-location buffer, the value the BATS configs give both steps.
BUFFER_METERS = 300.0


def tour_config(**overrides: Any) -> TourConfig:
    """Tour rules as the BATS configs state them, with *overrides* on top."""
    return TourConfig(**{"spatial_gap_threshold_meters": 1000.0, **overrides})


def days_not_asked(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Days for the trips, from a survey that did not ask where days began or ended.

    Each day's ``travel_date`` is the date its first trip departs, so members of
    one household travelling on the same date share it, and so share a
    household-day, numbered in date order.
    """
    return (
        linked_trips.group_by(pl.col("day_id").cast(pl.Int64))
        .agg(
            pl.col("hh_id").first(),
            pl.col("depart_time").min().dt.date().alias("travel_date"),
        )
        .sort("day_id")
        .with_columns(
            (
                pl.col("hh_id") * 100
                + pl.col("travel_date").rank("dense").over("hh_id").cast(pl.Int64)
            ).alias("hh_day_id"),
            pl.lit(None, dtype=pl.Int64).alias("begin_day"),
            pl.lit(None, dtype=pl.Int64).alias("end_day"),
        )
    )


def locate_and_extract_tours(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    *,
    joint_trips: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    extra: pl.DataFrame | None = None,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]:
    """Deliver the reported locations, detect the observed ones, then extract tours.

    Args:
        persons: Persons with work/school addresses and person-category fields.
        households: Households with the home address.
        unlinked_trips: Unlinked trips.
        linked_trips: Linked trips.
        joint_trips: Optional joint trips.
        days: Days with ``begin_day``/``end_day``; by default, not asked.
        extra: Further reported locations, as ``reported_habitual_locations``
            takes them (a second home).
        **kwargs: ``TourConfig`` settings over the BATS ones (see ``tour_config``).

    Returns:
        The ``extract_tours`` outputs plus both location tables.
    """
    days = days if days is not None else days_not_asked(linked_trips)
    located = detect_habitual_locations(
        habitual_locations=reported_habitual_locations(households, persons, extra=extra),
        linked_trips=linked_trips,
        days=days,
        locations=HabitualLocationConfig(buffer_meters=BUFFER_METERS, include_observed_homes=True),
    )
    tours = extract_tours(
        persons=persons,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        habitual_locations=located["habitual_locations"],
        days=days,
        joint_trips=joint_trips,
        extraction=tour_config(**kwargs),
        habitual_match=MatchConfig(buffer_meters=BUFFER_METERS),
    )
    return {**tours, **located}
