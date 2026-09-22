"""The household-date reduction, for both kinds of flag.

A **household-day** is the set of person-days sharing one ``hh_id`` and
``travel_date``. Both reductions here are the same shape -- ALL surveyable
members' days pass, written back onto every day on that date -- and they sit
side by side so the usable one reads as the mirror of the complete one.
"""

import polars as pl

from .profiles import UsabilityProfile


def _join_surveyable(days: pl.DataFrame, persons: pl.DataFrame | None) -> pl.DataFrame:
    """Return *days* with a ``_surveyable`` bool column joined from persons.

    A person is *surveyable* when the survey could collect their travel at all;
    unrelated household members (e.g. roommates) are not, file no trips, and
    must not veto the household-day reductions -- the vendor gives them no day
    rows whatsoever. When the persons table (or its ``surveyable`` column) is
    absent, every member-day counts, preserving the plain ALL reduction.
    """
    if persons is None or "surveyable" not in persons.columns or "person_id" not in days.columns:
        return days.with_columns(pl.lit(value=True).alias("_surveyable"))
    flag = persons.select(
        "person_id",
        pl.col("surveyable").cast(pl.Boolean).fill_null(value=True).alias("_surveyable"),
    )
    return days.join(flag, on="person_id", how="left").with_columns(
        pl.col("_surveyable").fill_null(value=True)
    )


def flag_household_day_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``hh_day_survey_complete`` on the days table, in place (reverse cascade).

    A **household-day** -- the set of person-days sharing one ``hh_id`` and
    ``travel_date`` -- is complete only when every *surveyable* member's day is
    complete (an ALL reduction over surveyable member-days). The result is
    written back onto each day on that date, so ``hh_day_survey_complete`` marks
    whether the day belongs to a coherently observed household-date.

    Unsurveyable members (see :func:`_join_surveyable`) neither veto the date
    nor borrow its verdict: their own day rows -- if a data source carries any
    -- keep their own ``survey_complete``, which is normally False since they file no
    trips. A source that nonetheless marks one complete is believed rather than
    overruled: the contradiction is upstream, and inventing a verdict here would
    hide it.

    This runs after :func:`rollup_completeness`, so ``survey_complete`` already
    reflects ancestry; the reduction then flows the other way, up from members to
    the shared date. Idempotent. Days without ``hh_id`` / ``travel_date`` (e.g.
    schema-only fixtures) fall back to each day's own ``survey_complete``.
    """
    days = tables.get("days")
    if days is None or "survey_complete" not in days.columns:
        return

    own = pl.col("survey_complete").fill_null(value=False)
    if "hh_id" not in days.columns or "travel_date" not in days.columns:
        tables["days"] = days.with_columns(own.alias("hh_day_survey_complete"))
        return

    days = _join_surveyable(days, tables.get("persons"))
    # all() over an empty set is True: a date observed only through unsurveyable
    # members has no surveyable observation to fail -- and no surveyable day to
    # gain usability from it either.
    household_day = days.group_by("hh_id", "travel_date").agg(
        own.filter(pl.col("_surveyable")).all().alias("_hh_day_survey_complete")
    )
    tables["days"] = (
        days.join(household_day, on=["hh_id", "travel_date"], how="left")
        .with_columns(
            pl.when(pl.col("_surveyable"))
            .then(pl.col("_hh_day_survey_complete").fill_null(value=False))
            .otherwise(own)
            .alias("hh_day_survey_complete")
        )
        .drop("_hh_day_survey_complete", "_surveyable")
    )


def flag_household_day_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the household-day usable flag: ALL surveyable member-days usable, in place.

    The usable-side mirror of :func:`flag_household_day_complete`: a household-day
    is *usable* only when every surveyable member's day is model-usable, not
    merely complete. ``household`` then needs at least one such date.
    Unsurveyable members neither veto the date nor inherit its verdict.
    Requires the per-record flag on days; days without ``hh_id`` /
    ``travel_date`` fall back to each day's own verdict.
    """
    days = tables.get("days")
    if days is None or cols.flag not in days.columns:
        return

    own = pl.col(cols.flag).fill_null(value=False)
    if "hh_id" not in days.columns or "travel_date" not in days.columns:
        tables["days"] = days.with_columns(own.alias(cols.household_day))
        return

    days = _join_surveyable(days, tables.get("persons"))
    household_day = days.group_by("hh_id", "travel_date").agg(
        own.filter(pl.col("_surveyable")).all().alias("_hh_day_usable")
    )
    tables["days"] = (
        days.join(household_day, on=["hh_id", "travel_date"], how="left")
        .with_columns(
            pl.when(pl.col("_surveyable"))
            .then(pl.col("_hh_day_usable").fill_null(value=False))
            .otherwise(own)
            .alias(cols.household_day)
        )
        .drop("_hh_day_usable", "_surveyable")
    )
