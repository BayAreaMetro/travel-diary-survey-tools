"""The per-day table: when a person was at each of their habitual locations.

It records *facts*, never conclusions: how long somebody was somewhere and how
the day related to it, but never which location "was" the day's anchor. That
judgement belongs to whichever consumer is making it.
"""

import polars as pl

from .habitual_location_configs import MatchConfig
from .matching import best_match, match_points, with_stated_day_ends

# Table columns in schema order (see HabitualLocationDayModel).
DAY_COLUMNS = [
    "habitual_location_id",
    "person_id",
    "day_id",
    "location_type",
    "dwell_minutes",
    "n_visits",
    "is_day_start",
    "is_day_end",
]


def day_schema() -> dict[str, pl.DataType]:
    """Column types for an empty habitual_location_days table."""
    return {
        "habitual_location_id": pl.Int64,
        "person_id": pl.Int64,
        "day_id": pl.Int64,
        "location_type": pl.Int64,
        "dwell_minutes": pl.Float64,
        "n_visits": pl.Int64,
        "is_day_start": pl.Boolean,
        "is_day_end": pl.Boolean,
    }


def build_location_days(
    episodes: pl.DataFrame,
    locations: pl.DataFrame,
    config: MatchConfig,
    days: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate stays into one row per habitual location per day.

    Each stay is attributed to the nearest location it is at (see
    :func:`~.matching.match_points`); stays at none are dropped, since this
    table is about habitual locations rather than all travel.

    Args:
        episodes: Stays, as produced by ``build_presence_episodes``.
        locations: Habitual locations with ``habitual_location_id``.
        config: Provides the buffer.
        days: Days with ``begin_day``/``end_day``.

    Returns:
        Table conforming to ``HabitualLocationDayModel``.
    """
    stays = episodes.with_row_index("_pt").with_columns(pl.col("_pt").cast(pl.Int64))
    best = best_match(match_points(with_stated_day_ends(stays, days), locations, config))
    attributed = stays.join(
        best.select("_pt", "habitual_location_id", "location_type"), on="_pt", how="inner"
    )

    location_days = (
        attributed.group_by(["habitual_location_id", "person_id", "day_id", "location_type"])
        .agg(
            pl.col("dwell_minutes").sum().alias("_dwell_sum"),
            pl.col("dwell_minutes").count().alias("_dwell_measured"),
            pl.len().alias("n_visits"),
            pl.col("is_day_start").any().alias("is_day_start"),
            pl.col("is_day_end").any().alias("is_day_end"),
        )
        .with_columns(
            # No measurable stay is not the same as a zero-length stay.
            pl.when(pl.col("_dwell_measured") == 0)
            .then(None)
            .otherwise(pl.col("_dwell_sum"))
            .cast(pl.Float64)
            .alias("dwell_minutes")
        )
        .with_columns(pl.col("n_visits").cast(pl.Int64))
    )

    return location_days.select(DAY_COLUMNS).sort(["person_id", "day_id", "habitual_location_id"])
