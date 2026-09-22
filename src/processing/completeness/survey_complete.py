"""The reporting cascade: what the survey collected, walked across the tables.

Survey reporting only. Nothing here consults a model criterion or reads a
profile, and one run of the pipeline has exactly one answer, so this is
independent of how many usability passes follow it.
"""

import polars as pl

from .household_day import flag_household_day_complete

# Records that sit on a day: their ``survey_complete`` is their own AND their day's
# (a trip or tour is no more complete than the day it belongs to).
_DAY_RECORDS = ("unlinked_trips", "linked_trips", "joint_trips", "tours", "joint_tours")


def rollup_completeness(tables: dict[str, pl.DataFrame | None]) -> None:
    """Roll ``survey_complete`` UP from days, then broadcast each day's value down, in place.

    Completeness is measured at the person-day (``day.complete`` is set upstream
    by the project cleaner from surveyed trips / a declared no-travel day). This
    derives the rest:

    * ``person.complete`` = it has at least one complete day (an ANY rollup).
    * every day-record (trip, tour, joint entity) = its own reporting AND its
      day complete -- broadcast down, since a trip is no more complete than the
      day it sits in.

    ``household.complete`` is a household-day rollup and is set separately by
    :func:`rollup_household_complete` (it needs ``hh_day_survey_complete`` first). Tables
    lacking ``survey_complete`` or the join key are left unchanged. Idempotent.
    """
    days = tables.get("days")
    if days is None or "survey_complete" not in days.columns:
        return
    day_complete = pl.col("survey_complete").fill_null(value=False)

    persons = tables.get("persons")
    if persons is not None and "person_id" in days.columns:
        per_flag = days.group_by("person_id").agg(day_complete.any().alias("_c"))
        tables["persons"] = (
            persons.drop("survey_complete", strict=False)
            .join(per_flag, on="person_id", how="left")
            .with_columns(pl.col("_c").fill_null(value=False).alias("survey_complete"))
            .drop("_c")
        )

    day_flag = days.select("day_id", day_complete.alias("_day_complete"))
    for name in _DAY_RECORDS:
        df = tables.get(name)
        if df is None or "survey_complete" not in df.columns or "day_id" not in df.columns:
            continue
        tables[name] = (
            df.join(day_flag, on="day_id", how="left")
            .with_columns(
                (
                    pl.col("survey_complete").fill_null(value=False)
                    & pl.col("_day_complete").fill_null(value=True)
                ).alias("survey_complete")
            )
            .drop("_day_complete")
        )


def rollup_household_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Set ``household.complete`` = has at least one complete household-day, in place.

    A household is complete when at least one date was coherently observed (every
    member complete). Requires ``hh_day_survey_complete`` on days (from
    :func:`flag_household_day_complete`). Left unchanged if days or the flag is
    absent.
    """
    households = tables.get("households")
    days = tables.get("days")
    if (
        households is None
        or days is None
        or "hh_day_survey_complete" not in days.columns
        or "hh_id" not in days.columns
    ):
        return
    has_complete_day = (
        days.filter(pl.col("hh_day_survey_complete").fill_null(value=False))
        .select("hh_id")
        .unique()
        .with_columns(pl.lit(value=True).alias("_h"))
    )
    tables["households"] = (
        households.drop("survey_complete", strict=False)
        .join(has_complete_day, on="hh_id", how="left")
        .with_columns(pl.col("_h").fill_null(value=False).alias("survey_complete"))
        .drop("_h")
    )


def cascade_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Derive every ``survey_complete`` flag, in place.

    Survey reporting only: what the vendor collected, cascaded through the
    hierarchy. Nothing here consults a model criterion, and one run of the
    pipeline has exactly one answer, so this is independent of how many
    usability passes follow.

    Args:
        tables: Mutable dict of table_name -> DataFrame (or None).
    """
    # -- Completeness rolls UP from days (person = >=1 complete day) and each
    #    day-record inherits its day's complete.
    rollup_completeness(tables)

    # -- Household-day coherence (lateral: ALL member-days complete that date) -
    flag_household_day_complete(tables)

    # -- household.complete = >=1 complete household-day ----------------------
    rollup_household_complete(tables)
