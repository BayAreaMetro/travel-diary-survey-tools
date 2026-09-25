"""Run tour extraction the way the pipeline does: locations first.

``extract_tours`` reads a finished ``habitual_locations`` table, which survey
cleaning delivers and ``detect_habitual_locations`` completes. Tests that start
from persons, households and trips go through the same three stages here.
"""

from typing import Any

import polars as pl

from processing.habitual_locations import detect_habitual_locations, reported_habitual_locations
from processing.tours import extract_tours


def days_not_asked(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Days for the trips, from a survey that did not ask where days began or ended."""
    return (
        linked_trips.select(pl.col("day_id").cast(pl.Int64))
        .unique()
        .sort("day_id")
        .with_columns(
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
        **kwargs: ``extract_tours`` configuration.

    Returns:
        The ``extract_tours`` outputs plus both location tables.
    """
    days = days if days is not None else days_not_asked(linked_trips)
    located = detect_habitual_locations(
        habitual_locations=reported_habitual_locations(households, persons, extra=extra),
        linked_trips=linked_trips,
        days=days,
    )
    tours = extract_tours(
        persons=persons,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        habitual_locations=located["habitual_locations"],
        days=days,
        joint_trips=joint_trips,
        **kwargs,
    )
    return {**tours, **located}
