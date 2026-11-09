"""The reporting cascade: what the survey collected, walked across the tables.

Survey reporting only. Nothing here consults a model criterion or reads a
profile, and one run of the pipeline has exactly one answer, so this is
independent of how many usability passes follow it.
"""

import polars as pl

from .household_day import flag_household_day_complete
from .reduce import and_reduced

# Records that sit on a day: their ``survey_complete`` is their own AND their day's
# (a trip or tour is no more complete than the day it belongs to). Joint trips and
# joint tours are not here: they belong to a household date, not to one member's
# day, and their own value is already the AND of their members'.
_DAY_RECORDS = ("unlinked_trips", "linked_trips", "tours")


def rollup_completeness(tables: dict[str, pl.DataFrame | None]) -> None:
    """Roll ``survey_complete`` UP from days, then broadcast each day's value down, in place.

    Completeness is measured at the person-day (``person_days.survey_complete`` is set
    upstream by the project cleaner, from the vendor's day verdict). This derives
    the rest:

    * ``persons.survey_complete`` = it has at least one complete day (an ANY rollup).
    * every day-record (trip, tour) = its own reporting AND its day complete --
      broadcast down, since a trip is no more complete than the day it sits in.
      A record with no verdict of its own (null, e.g. a trip the vendor added
      without a trip survey) takes its day's. Joint trips and tours are not
      day-records; see ``_DAY_RECORDS``.

    ``households.survey_complete`` is a household-day rollup and is set separately by
    :func:`rollup_household_complete` (it needs ``household_days`` judged first). Tables
    lacking ``survey_complete`` or the join key are left unchanged. Idempotent.
    """
    person_days = tables.get("person_days")
    if person_days is None or "survey_complete" not in person_days.columns:
        return
    day_complete = pl.col("survey_complete").fill_null(value=False)

    persons = tables.get("persons")
    if persons is not None and "person_id" in person_days.columns:
        tables["persons"] = and_reduced(
            persons.drop("survey_complete", strict=False),
            person_days,
            "person_id",
            day_complete.any(),
            fill=False,
            into="survey_complete",
        )

    for name in _DAY_RECORDS:
        df = tables.get(name)
        if df is None or "survey_complete" not in df.columns or "person_day_id" not in df.columns:
            continue
        # A null own verdict defers to the day's.
        tables[name] = and_reduced(
            df.with_columns(pl.col("survey_complete").fill_null(value=True)),
            person_days,
            "person_day_id",
            day_complete.first(),
            fill=True,
            into="survey_complete",
        )


def rollup_household_complete(tables: dict[str, pl.DataFrame | None]) -> None:
    """Set ``households.survey_complete`` = has at least one complete household-day, in place.

    A household is complete when at least one date was coherently observed (every
    member complete). Requires ``household_days.survey_complete`` (from
    :func:`flag_household_day_complete`). Left unchanged if either is absent.
    """
    households = tables.get("households")
    household_days = tables.get("household_days")
    if households is None or household_days is None or "survey_complete" not in household_days:
        return
    tables["households"] = and_reduced(
        households.drop("survey_complete", strict=False),
        household_days,
        "hh_id",
        pl.col("survey_complete").fill_null(value=False).any(),
        fill=False,
        into="survey_complete",
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

    # -- households.survey_complete = >=1 complete household-day -------------
    rollup_household_complete(tables)
