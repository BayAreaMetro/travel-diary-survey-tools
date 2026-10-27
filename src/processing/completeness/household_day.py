"""The household-date reduction, for both kinds of flag.

A **household-day** is one household's travel date (``household_days``, built by
the project cleaner): the person-days sharing one ``hh_day_id``. Both reductions
here are the same shape -- ALL surveyable members' days pass -- and they sit side
by side so the usable one reads as the mirror of the complete one. Each writes
onto ``household_days``; :func:`with_household_day` reads the verdict back for
the person-day level.
"""

import polars as pl

from .profiles import UsabilityProfile


def _join_surveyable(person_days: pl.DataFrame, persons: pl.DataFrame | None) -> pl.DataFrame:
    """Return *days* with a ``_surveyable`` bool column joined from persons.

    A person is *surveyable* when the survey could collect their travel at all;
    unrelated household members (e.g. roommates) are not, file no trips, and
    must not veto the household-day reductions -- the vendor gives them no day
    rows whatsoever. When the persons table (or its ``surveyable`` column) is
    absent, every member-day counts, preserving the plain ALL reduction.
    """
    if (
        persons is None
        or "surveyable" not in persons.columns
        or "person_id" not in person_days.columns
    ):
        return person_days.with_columns(pl.lit(value=True).alias("_surveyable"))
    flag = persons.select(
        "person_id",
        pl.col("surveyable").cast(pl.Boolean).fill_null(value=True).alias("_surveyable"),
    )
    return person_days.join(flag, on="person_id", how="left").with_columns(
        pl.col("_surveyable").fill_null(value=True)
    )


def _require_household_days(tables: dict[str, pl.DataFrame | None]) -> pl.DataFrame:
    """Return ``household_days``, raising if person-days arrive without it.

    The table is canonical and every cleaner builds it, so its absence is a
    caller error. Judging dates some other way would give a different answer
    silently.

    Raises:
        ValueError: If ``household_days`` is missing, or days carry no ``hh_day_id``.
    """
    household_days = tables.get("household_days")
    person_days = tables.get("person_days")
    if household_days is None or person_days is None or "hh_day_id" not in person_days.columns:
        msg = (
            "The household-day reductions need the household_days table and hh_day_id "
            "on days. Build them in the project cleaner with "
            "utils.create_ids.create_household_days."
        )
        raise ValueError(msg)
    return household_days


def _reduce_to_household_days(
    tables: dict[str, pl.DataFrame | None], own: pl.Expr, column: str
) -> None:
    """Write ALL(surveyable members' *own*) onto ``household_days.<column>``, in place.

    A household-day with no surveyable member's day on it still exists; nothing
    the survey meant to observe was observed on it, so it is False rather than
    vacuously True.
    """
    household_days = _require_household_days(tables)
    person_days = _join_surveyable(tables["person_days"], tables.get("persons"))
    reduced = (
        person_days.filter(pl.col("_surveyable"))
        .group_by("hh_day_id")
        .agg(own.fill_null(value=False).all().alias("_verdict"))
    )
    tables["household_days"] = (
        household_days.drop(column, strict=False)
        .join(reduced, on="hh_day_id", how="left")
        .with_columns(pl.col("_verdict").fill_null(value=False).alias(column))
        .drop("_verdict")
    )


def flag_household_day_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Stamp ``household_days.survey_complete``, in place (the reverse cascade).

    A household-day is complete only when every *surveyable* member's day is
    complete: an ALL reduction over those person-days, which are the vendor's
    verdicts. It decides nothing of its own. Unsurveyable members (see
    :func:`_join_surveyable`) take no part.

    This runs after :func:`rollup_completeness`, so ``survey_complete`` already
    reflects ancestry; the reduction then flows the other way, up from members to
    the shared date. Idempotent.
    """
    person_days = tables.get("person_days")
    if person_days is None or "survey_complete" not in person_days.columns:
        return
    _reduce_to_household_days(tables, pl.col("survey_complete"), "survey_complete")


def flag_household_day_usable(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp ``household_days.<flag>``: ALL surveyable members' days usable, in place.

    The usable-side mirror of :func:`flag_household_day_complete`: a household-day
    is *usable* only when every surveyable member's day is usable under the
    profile, not merely complete. Requires the per-record flag on days.
    """
    person_days = tables.get("person_days")
    if person_days is None or cols.flag not in person_days.columns:
        return
    _reduce_to_household_days(tables, pl.col(cols.flag), cols.flag)


def with_household_day(
    person_days: pl.DataFrame,
    tables: dict[str, pl.DataFrame | None],
    column: str,
    alias: str,
) -> pl.DataFrame:
    """Return *days* with *alias*: its household-day's *column*.

    An unsurveyable member's day takes no part in the household-day, so it is
    not judged by it either: it keeps its own *column*.
    """
    household_days = _require_household_days(tables)
    verdict = household_days.select("hh_day_id", pl.col(column).alias("_hh_day_verdict"))
    person_days = _join_surveyable(person_days, tables.get("persons")).join(
        verdict, on="hh_day_id", how="left"
    )
    return person_days.with_columns(
        pl.when(pl.col("_surveyable"))
        .then(pl.col("_hh_day_verdict").fill_null(value=False))
        .otherwise(pl.col(column).fill_null(value=False))
        .alias(alias)
    ).drop("_hh_day_verdict", "_surveyable")
