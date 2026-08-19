"""Polars expression builders shared by the ETC conversion.

Small, reusable pieces that turn one vendor column into one canonical column.
They are separated from :mod:`conform_etc` so the table builders there read as
a list of field assignments rather than a wall of expression plumbing.
"""

import logging

import polars as pl
from conformity.mappings import (
    ACTIVITY_PURPOSE,
    AGE_BREAKS,
    BICYCLE_USED,
    BIKE_SHARE_CODES,
    HISPANIC_CODE,
    LODGING_PLACE_CODES,
    MICROMOBILITY_DEVICE,
    MODE,
    MODE_TYPE,
    PLACE_REFINEMENTS,
    PNTA_CODE,
    RACE_ETHNICITY_TO_RACE,
    SCOOTER_SHARE_CODE,
    VENDOR_AGE_CATEGORY,
)

from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.persons import AgeCategory, Ethnicity, Race
from data_canon.codebook.trips import Mode, ModeType, Purpose

logger = logging.getLogger(__name__)


def code_expr(col: str) -> pl.Expr:
    """Read a vendor code column as an integer.

    A question nobody in the extract answered comes back from the CSV reader
    as an all-null string column, so every comparison against a code has to
    say what type it means.
    """
    return pl.col(col).cast(pl.Int64, strict=False)


def recode_expr(
    df: pl.DataFrame,
    col: str,
    mapping: dict[int, int],
    default: int | None,
    *,
    keep_null: bool = True,
) -> pl.Expr:
    """Map a vendor code column onto canonical codes, warning on surprises.

    Args:
        df: Frame holding the column, used to report codes the mapping misses.
        col: Vendor column name.
        mapping: Vendor code to canonical code.
        default: Canonical code for anything the mapping does not cover.
        keep_null: Leave unanswered cells null rather than sending them to
            ``default``.  Turn this off for fields whose canonical enum has an
            explicit MISSING member that a null should become.

    Returns:
        Expression yielding the canonical code.
    """
    present = set(df[col].drop_nulls().unique().to_list())
    unmapped = sorted(present - set(mapping))
    if unmapped:
        logger.warning("%s: vendor codes %s are not mapped, sent to %s", col, unmapped, default)

    expr = code_expr(col).replace_strict(mapping, default=default, return_dtype=pl.Int64)
    if keep_null:
        expr = pl.when(pl.col(col).is_not_null()).then(expr).otherwise(None)
    return expr


def text_expr(df: pl.DataFrame, col: str) -> pl.Expr:
    """Render a date/time column as text however the CSV reader typed it."""
    dtype = df.schema[col]
    if dtype == pl.Time:
        return pl.col(col).dt.to_string("%H:%M:%S")
    if dtype in (pl.Date, pl.Datetime):
        return pl.col(col).dt.to_string("%Y-%m-%d")
    return pl.col(col).cast(pl.String)


def timestamp_expr(df: pl.DataFrame, date_col: str, time_col: str) -> pl.Expr:
    """Combine a vendor date column and clock-time column into a timestamp."""
    return pl.concat_str(
        [text_expr(df, date_col), text_expr(df, time_col)], separator=" "
    ).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)


def yes_no_expr(condition: pl.Expr, asked: pl.Expr) -> pl.Expr:
    """Return a canonical yes/no code, staying null where nothing was asked."""
    return (
        pl.when(asked.is_null())
        .then(None)
        .when(condition)
        .then(pl.lit(BooleanYesNo.YES.value))
        .otherwise(pl.lit(BooleanYesNo.NO.value))
        .cast(pl.Int64)
    )


def age_category_expr(df: pl.DataFrame) -> pl.Expr:
    """Bin reported age, falling back to the vendor's own age band.

    Canonical ``AgeCategory`` has no missing member, so a person who answered
    neither question stays null and is reported by :func:`report_gaps`.
    """
    banded = recode_expr(df, "Age Category", VENDOR_AGE_CATEGORY, None)

    age = code_expr("Age")
    from_age = pl.when(age.is_null()).then(None)
    for upper, category in AGE_BREAKS:
        from_age = from_age.when(age < upper).then(pl.lit(category))
    from_age = from_age.otherwise(pl.lit(AgeCategory.AGE_85_AND_UP.value))

    return pl.coalesce(from_age, banded).cast(pl.Int64)


def race_ethnicity_exprs() -> tuple[pl.Expr, pl.Expr]:
    """Split the vendor's combined race/ethnicity multi-select into two fields.

    Returns:
        Tuple of (race expression, ethnicity expression).
    """
    codes = (
        pl.col("Ethnicity")
        .cast(pl.String)
        .str.split(";")
        .list.eval(pl.element().str.strip_chars().cast(pl.Int64, strict=False))
    )
    races = codes.list.eval(
        pl.element()
        .replace_strict(RACE_ETHNICITY_TO_RACE, default=None, return_dtype=pl.Int64)
        .drop_nulls()
        .unique()
    )
    n_races = races.list.len()

    race = (
        pl.when(n_races > 1)
        .then(pl.lit(Race.MULTI.value))
        .when(n_races == 1)
        .then(races.list.first())
        .when(codes.list.contains(PNTA_CODE))
        .then(pl.lit(Race.PNTA.value))
        .otherwise(pl.lit(Race.MISSING.value))
    )
    ethnicity = (
        pl.when(codes.list.contains(HISPANIC_CODE))
        .then(pl.lit(Ethnicity.MEXICAN.value))
        .when(codes.list.contains(PNTA_CODE))
        .then(pl.lit(Ethnicity.PNTA.value))
        .when(n_races > 0)
        .then(pl.lit(Ethnicity.NOT_HISPANIC.value))
        .otherwise(pl.lit(Ethnicity.MISSING.value))
    )
    return race.cast(pl.Int64), ethnicity.cast(pl.Int64)


def purpose_expr(df: pl.DataFrame, activity_col: str, place_col: str) -> pl.Expr:
    """Map an activity code to a canonical purpose, sharpened by place type."""
    expr = pl.when(code_expr(place_col).is_in(LODGING_PLACE_CODES)).then(
        pl.lit(Purpose.TEMP_LODGING.value)
    )
    for activities, places, purpose in PLACE_REFINEMENTS:
        expr = expr.when(
            code_expr(activity_col).is_in(activities) & code_expr(place_col).is_in(places)
        ).then(pl.lit(purpose))
    fallback = recode_expr(
        df, activity_col, ACTIVITY_PURPOSE, Purpose.MISSING.value, keep_null=False
    )
    return expr.otherwise(fallback).cast(pl.Int64)


def mode_detail_expr(df: pl.DataFrame, mode_col: str) -> pl.Expr:
    """Resolve the detailed mode, preferring the bike/micromobility follow-ups."""
    return pl.coalesce(
        recode_expr(df, "Bicycle Used", BICYCLE_USED, None),
        recode_expr(df, "Micromobility Device", MICROMOBILITY_DEVICE, None),
        recode_expr(df, mode_col, MODE, Mode.MISSING.value, keep_null=False),
    ).cast(pl.Int64)


def mode_type_expr(df: pl.DataFrame, mode_col: str) -> pl.Expr:
    """Resolve the mode type, letting the shared-vehicle follow-ups override."""
    fallback = recode_expr(df, mode_col, MODE_TYPE, ModeType.MISSING.value, keep_null=False)
    return (
        pl.when(code_expr("Bicycle Used").is_in(BIKE_SHARE_CODES))
        .then(pl.lit(ModeType.BIKESHARE.value))
        .when(code_expr("Micromobility Device") == SCOOTER_SHARE_CODE)
        .then(pl.lit(ModeType.SCOOTERSHARE.value))
        .otherwise(fallback)
        .cast(pl.Int64)
    )
