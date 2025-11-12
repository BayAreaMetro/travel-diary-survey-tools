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

    suffixes = ["date", "hour", "minute", "seconds"]
    d_cols = [f"depart_{s}" for s in suffixes]
    a_cols = [f"arrive_{s}" for s in suffixes]

    # Helper function to handle datetime columns
    def _to_datetime_column(
        df: pl.DataFrame, col_name: str, component_cols: list[str]
    ) -> pl.DataFrame:
        if col_name not in df.columns:
            logger.info("Constructing %s...", col_name)
            return df.with_columns(
                datetime_from_parts(
                    *[pl.col(c) for c in component_cols]
                ).alias(col_name)
            )
        if df[col_name].dtype == pl.Utf8:
            logger.info("Parsing %s from string...", col_name)
            df = df.with_columns(
                pl.col(col_name).str.to_datetime(
                    format=datetime_format,
                    strict=False)
                )

            # If parsing failed for some rows, reconstruct from components
            if df[col_name].null_count() > 0:
                logger.info(
                    "Reconstructing null %s from components...",
                    col_name
                    )
                df = df.with_columns(
                    pl.when(pl.col(col_name).is_null())
                    .then(
                        datetime_from_parts(
                            *[pl.col(c) for c in component_cols]
                        )
                    )
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )
        return df

    trips = _to_datetime_column(trips, "depart_time", d_cols)
    trips = _to_datetime_column(trips, "arrive_time", a_cols)

    return trips  # noqa: RET504


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
