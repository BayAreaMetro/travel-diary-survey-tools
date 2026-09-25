"""Trips as stays: where a person was, for how long, and when."""

import polars as pl

# Sentinel values of d_activity_duration that are not real dwell times
# (see LinkedTripModel.d_activity_duration): -1 = destination is home,
# -2 = last trip of the person-day (no subsequent departure).
_DWELL_SENTINELS = (-1, -2)


def build_presence_episodes(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Turn linked trips into one row per stay somewhere.

    Every trip destination is a stay. Each day's *first origin* is also a stay —
    the person was there before they set off, and no destination records it,
    which is what makes ``is_day_start`` underivable from destinations alone.
    Later origins are skipped: they repeat the previous trip's destination.

    Args:
        linked_trips: Linked trips with ``person_id``, ``day_id``, ``o_lat``,
            ``o_lon``, ``d_lat``, ``d_lon``, ``depart_time``, ``arrive_time``,
            purpose columns and ``d_activity_duration``.

    Returns:
        One row per stay with ``person_id``, ``day_id``, ``lat``, ``lon``,
        ``purpose``, ``purpose_category``, ``dwell_minutes`` (null where the
        duration is a sentinel or the stay is a day's first origin),
        ``seen_at`` (when the person was there), ``is_day_start`` and
        ``is_day_end``.
    """
    ordered = linked_trips.sort(["person_id", "day_id", "depart_time"]).with_columns(
        pl.int_range(0, pl.len()).over(["person_id", "day_id"]).alias("_trip_idx"),
        pl.len().over(["person_id", "day_id"]).alias("_n_trips"),
    )

    destinations = ordered.select(
        "person_id",
        "day_id",
        pl.col("d_lat").alias("lat"),
        pl.col("d_lon").alias("lon"),
        pl.col("d_purpose").alias("purpose"),
        pl.col("d_purpose_category").alias("purpose_category"),
        pl.when(pl.col("d_activity_duration").is_in(_DWELL_SENTINELS))
        .then(None)
        .otherwise(pl.col("d_activity_duration"))
        .cast(pl.Float64)
        .alias("dwell_minutes"),
        pl.col("arrive_time").alias("seen_at"),
        pl.lit(value=False).alias("is_day_start"),
        (pl.col("_trip_idx") == pl.col("_n_trips") - 1).alias("is_day_end"),
    )

    first_origins = ordered.filter(pl.col("_trip_idx") == 0).select(
        "person_id",
        "day_id",
        pl.col("o_lat").alias("lat"),
        pl.col("o_lon").alias("lon"),
        pl.col("o_purpose").alias("purpose"),
        pl.col("o_purpose_category").alias("purpose_category"),
        # The stay began before the diary day, so its length is unknown.
        pl.lit(None, dtype=pl.Float64).alias("dwell_minutes"),
        pl.col("depart_time").alias("seen_at"),
        pl.lit(value=True).alias("is_day_start"),
        pl.lit(value=False).alias("is_day_end"),
    )

    return pl.concat([destinations, first_origins], how="vertical")
