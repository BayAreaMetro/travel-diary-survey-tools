"""Utility functions for trip linking."""

import logging

import polars as pl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def datetime_from_parts(
    date: pl.Series,
    hour: pl.Series,
    minute: pl.Series,
    second: pl.Series,
) -> pl.Series:
    """Construct datetime from date and time parts."""
    return pl.concat_str(
        [
            date,
            pl.lit("T"),
            hour.cast(pl.Utf8).str.pad_start(2, "0"),
            pl.lit(":"),
            minute.cast(pl.Utf8).str.pad_start(2, "0"),
            pl.lit(":"),
            second.cast(pl.Utf8).str.pad_start(2, "0"),
        ]
    ).str.to_datetime()


def add_time_columns(
    trips: pl.DataFrame,
    datetime_format: str = "%Y-%m-%d %H:%M:%S",
) -> pl.DataFrame:
    """Add datetime columns for departure and arrival times if missing.

    If datetime columns exist as strings, parse them to datetime type.
    Otherwise, construct them from component columns.
    """
    logger.info("Adding datetime columns...")

    for prefix in ["depart", "arrive"]:
        col_name = f"{prefix}_time"
        comp_cols = [
            f"{prefix}_{s}" for s in ["date", "hour", "minute", "seconds"]
        ]

        if col_name not in trips.columns:
            logger.info("Constructing %s...", col_name)
            trips = trips.with_columns(
                datetime_from_parts(*[pl.col(c) for c in comp_cols])
                .alias(col_name)
            )
        elif trips[col_name].dtype == pl.Utf8:
            logger.info("Parsing %s from string...", col_name)
            trips = trips.with_columns(
                pl.col(col_name).str
                .to_datetime(format=datetime_format, strict=False)
            )

            if trips[col_name].null_count() > 0:
                logger.info(
                    "Reconstructing null %s from components...", col_name
                )
                trips = trips.with_columns(
                    pl.when(pl.col(col_name).is_null())
                    .then(datetime_from_parts(*[pl.col(c) for c in comp_cols]))
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )

    return trips


def expr_haversine(
    lat1: pl.Expr,
    lon1: pl.Expr,
    lat2: pl.Expr,
    lon2: pl.Expr,
    units: str = "meters",
) -> pl.Expr:
    """Return a Polars expression for Haversine distance."""
    r = 6371000.0  # Earth radius (meters)
    dlat = lat2.radians() - lat1.radians()
    dlon = lon2.radians() - lon1.radians()
    a = (dlat / 2).sin().pow(
        2
    ) + lat1.radians().cos() * lat2.radians().cos() * (dlon / 2).sin().pow(2)

    distance = 2 * r * a.sqrt().arcsin()

    if units in ["kilometers", "km"]:
        distance = distance / 1000.0
    elif units in ["miles", "mi"]:
        distance = distance / 1609.344

    return distance
